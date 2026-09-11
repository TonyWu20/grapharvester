use std::time::Instant;

use crate::dag::Dag;
use crate::error::WorkflowError;
use crate::process::{ProcessHandle, ProcessRunner};
use crate::state::{StateStore, StateStoreExt, TaskStatus, TaskSuccessors};
use crate::task::{ExecutionMode, Task, TaskClosure};

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::HookExecutor;

/// A handle to a running task with metadata.
pub(crate) struct InFlightTask {
    pub handle: Box<dyn ProcessHandle>,
    pub started_at: Instant,
    pub monitors: Vec<crate::monitoring::MonitoringHook>,
    pub collect: Option<TaskClosure>,
    pub workdir: std::path::PathBuf,
    pub collect_failure_policy: crate::task::CollectFailurePolicy,
    pub last_periodic_fire: HashMap<String, Instant>,
}

pub struct Workflow {
    pub name: String,
    tasks: HashMap<String, Task>,
    max_parallel: usize,
    pub(crate) interrupt: Arc<AtomicBool>,
    log_dir: Option<std::path::PathBuf>,
    root_dir: Option<std::path::PathBuf>,
    queued_submitter: Option<Arc<dyn crate::process::QueuedSubmitter>>,
    computed_successors: Option<TaskSuccessors>,
}

impl Workflow {
    /// Creates a new Workflow instance.
    pub fn new(name: impl Into<String>) -> Self {
        let max_parallel = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            name: name.into(),
            tasks: HashMap::new(),
            max_parallel,
            interrupt: Arc::new(AtomicBool::new(false)),
            log_dir: None,
            root_dir: None,
            queued_submitter: None,
            computed_successors: None,
        }
    }

    /// Sets the maximum parallel execution limit.
    pub fn with_max_parallel(mut self, n: usize) -> Result<Self, WorkflowError> {
        if n == 0 {
            return Err(WorkflowError::InvalidConfig(
                "max_parallel must be at least 1".into(),
            ));
        }
        self.max_parallel = n;
        Ok(self)
    }

    /// Sets the directory for log file creation.
    pub fn with_log_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.log_dir = Some(path.into());
        self
    }

    /// Sets the QueuedSubmitter for Queued execution mode tasks.
    pub fn with_queued_submitter(mut self, qs: Arc<dyn crate::process::QueuedSubmitter>) -> Self {
        self.queued_submitter = Some(qs);
        self
    }

    /// Sets a root directory. Relative `task.workdir` values are resolved against it.
    pub fn with_root_dir(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.root_dir = Some(path.into());
        self
    }

    /// Returns the computed successor map after `run()` has been called.
    /// Returns `None` if `run()` has not yet been called.
    pub fn successor_map(&self) -> Option<&TaskSuccessors> {
        self.computed_successors.as_ref()
    }

    pub fn add_task(&mut self, task: Task) -> Result<(), WorkflowError> {
        if self.tasks.contains_key(&task.id) {
            return Err(WorkflowError::DuplicateTaskId(task.id.clone()));
        }
        self.tasks.insert(task.id.clone(), task);
        Ok(())
    }

    pub fn dry_run(&self) -> Result<Vec<String>, WorkflowError> {
        Ok(self.build_dag()?.topological_order())
    }

    /// Runs the workflow with dependency injection for state, runner, and hook executor.
    ///
    /// # Panics (debug only)
    /// Asserts that the workflow has tasks. Tasks are consumed from the `Workflow` on dispatch;
    /// calling `run()` twice on the same instance will silently process no tasks on the second call.
    /// Construct a new `Workflow` to re-run.
    pub fn run(
        &mut self,
        state: &mut dyn StateStore,
        runner: Arc<dyn ProcessRunner>,
        hook_executor: Arc<dyn HookExecutor>,
    ) -> Result<WorkflowSummary, WorkflowError> {
        debug_assert!(
            !self.tasks.is_empty(),
            "run() called on a Workflow with no tasks — tasks are consumed on dispatch"
        );
        signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&self.interrupt)).ok();
        signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&self.interrupt)).ok();

        let resolved_log_dir = self.log_dir.as_ref().map(|dir| {
            if dir.is_absolute() {
                dir.clone()
            } else if let Some(ref root) = self.root_dir {
                root.join(dir)
            } else {
                dir.clone()
            }
        });

        if let Some(ref dir) = resolved_log_dir {
            std::fs::create_dir_all(dir).map_err(WorkflowError::Io)?;
        }

        let dag = self.build_dag()?;

        // Compute and store task dependency graph for CLI retrieval
        let successors: HashMap<String, Vec<String>> = dag.task_ids()
            .map(|id| (id.clone(), dag.successors(id)))
            .collect();
        self.computed_successors = Some(TaskSuccessors::new(successors));

        // Initialize state for all tasks
        for id in dag.task_ids() {
            if state.get_status(id).is_none() {
                state.set_status(id, TaskStatus::Pending);
            }
        }
        state.save()?;

        let mut handles: HashMap<String, InFlightTask> = HashMap::new();
        let workflow_start = Instant::now();

        // Task timeout tracking
        let mut task_timeouts: HashMap<String, Duration> = HashMap::new();

        loop {
            // Interrupt check — must be first
            if self.interrupt.load(Ordering::SeqCst) {
                for id in handles.keys() {
                    state.set_status(id, TaskStatus::Pending);
                }
                for (_, t) in handles.iter_mut() {
                    t.handle.terminate().ok();
                }
                state.save()?;
                return Err(WorkflowError::Interrupted);
            }

            let finished = poll_finished(&mut handles, &task_timeouts, state)?;

            // Remove and process finished tasks
            for id in finished {
                if let Some(t) = handles.remove(&id) {
                    process_finished(&id, t, state, hook_executor.as_ref())?;
                }
            }

            propagate_skips(&dag, state, &self.tasks)?;

            // Fire periodic hooks for in-flight tasks
            for (task_id, t) in handles.iter_mut() {
                for hook in &t.monitors {
                    if let crate::monitoring::HookTrigger::Periodic { interval_secs } = hook.trigger {
                        let last = t.last_periodic_fire
                            .entry(hook.name.clone())
                            .or_insert(t.started_at);
                        if last.elapsed() >= Duration::from_secs(interval_secs) {
                            let ctx = crate::monitoring::HookContext {
                                task_id: task_id.clone(),
                                workdir: t.workdir.clone(),
                                phase: crate::monitoring::TaskPhase::Running,
                                exit_code: None,
                            };
                            if let Err(e) = hook_executor.execute_hook(hook, &ctx) {
                                tracing::warn!(
                                    "Periodic hook '{}' for task '{}' failed: {}",
                                    hook.name, task_id, e
                                );
                            }
                            *last = Instant::now();
                        }
                    }
                }
            }

            // Dispatch ready tasks
            let done_set: HashSet<String> = state
                .all_tasks()
                .into_iter()
                .filter(|(_, v)| {
                    matches!(
                        v,
                        TaskStatus::Completed
                            | TaskStatus::Skipped
                            | TaskStatus::SkippedDueToDependencyFailure
                    )
                })
                .map(|(k, _)| k)
                .collect();

            let mut dispatched_this_iter = 0;
            for id in dag.ready_tasks(&done_set) {
                if handles.len() >= self.max_parallel {
                    break;
                }
                if matches!(state.get_status(&id), Some(TaskStatus::Pending)) {
                    // Take task from HashMap (consume it)
                    if let Some(task) = self.tasks.remove(&id) {
                        state.mark_running(&id);

                        // Resolve workdir against root_dir if configured
                        let resolved_workdir = if task.workdir.is_absolute() {
                            task.workdir.clone()
                        } else if let Some(ref root) = self.root_dir {
                            root.join(&task.workdir)
                        } else {
                            task.workdir.clone()
                        };

                        // Execute setup closure if present
                        if let Some(setup) = &task.setup {
                            if let Err(e) = setup(&resolved_workdir) {
                                state.mark_failed(&id, e.to_string());
                                state.save()?;
                                continue;
                            }
                        }

                        let handle = match &task.mode {
                            ExecutionMode::Direct { command, args, env, timeout } => {
                                if let Some(d) = timeout {
                                    task_timeouts.insert(id.to_string(), *d);
                                }
                                match runner.spawn(&resolved_workdir, command, args, env) {
                                    Ok(h) => h,
                                    Err(e) => {
                                        state.mark_failed(&id, e.to_string());
                                        state.save()?;
                                        continue;
                                    }
                                }
                            }
                            ExecutionMode::Queued => {
                                let qs = match self.queued_submitter.as_ref() {
                                    Some(qs) => qs,
                                    None => {
                                        state.mark_failed(&id, format!(
                                            "task '{}': Queued mode requires a QueuedSubmitter", id
                                        ));
                                        state.save()?;
                                        continue;
                                    }
                                };
                                let log_dir = resolved_log_dir.as_deref()
                                    .unwrap_or(resolved_workdir.as_path());
                                match qs.submit(&resolved_workdir, &id, log_dir) {
                                    Ok(h) => h,
                                    Err(e) => {
                                        state.mark_failed(&id, e.to_string());
                                        state.save()?;
                                        continue;
                                    }
                                }
                            }
                        };

                        let monitors = task.monitors.clone();
                        let task_workdir = resolved_workdir.clone();

                        fire_hooks(
                            &monitors,
                            &task_workdir,
                            crate::monitoring::TaskPhase::Running,
                            None,
                            &id,
                            hook_executor.as_ref(),
                        );

                        handles.insert(id.to_string(), InFlightTask {
                            handle,
                            started_at: Instant::now(),
                            monitors,
                            collect: task.collect,
                            workdir: task_workdir,
                            collect_failure_policy: task.collect_failure_policy,
                            last_periodic_fire: HashMap::new(),
                        });
                        dispatched_this_iter += 1;
                    }
                }
            }

            // Persist the "Running" status to disk after dispatching in this
            // iteration, so the on-disk state file reflects in-flight tasks,
            // not just terminal (Completed/Failed) states.
            if dispatched_this_iter > 0 {
                state.save()?;
            }

            // Check if all done
            let all_done = dag.task_ids().all(|id| {
                matches!(
                    state.get_status(id),
                    Some(TaskStatus::Completed)
                        | Some(TaskStatus::Failed { .. })
                        | Some(TaskStatus::Skipped)
                        | Some(TaskStatus::SkippedDueToDependencyFailure)
                )
            });

            if all_done && handles.is_empty() {
                break;
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        Ok(build_summary(state, workflow_start))
    }

    fn build_dag(&self) -> Result<Dag, WorkflowError> {
        let mut dag = Dag::new();
        for id in self.tasks.keys() {
            dag.add_node(id.clone())?;
        }
        for task in self.tasks.values() {
            for dep in &task.dependencies {
                dag.add_edge(dep, &task.id)?;
            }
        }
        Ok(dag)
    }
}

/// Fires monitoring hooks that match the given trigger conditions.
///
/// Logs warnings for individual hook failures but does not propagate them.
fn fire_hooks(
    monitors: &[crate::monitoring::MonitoringHook],
    workdir: &std::path::Path,
    phase: crate::monitoring::TaskPhase,
    exit_code: Option<i32>,
    task_id: &str,
    hook_executor: &dyn HookExecutor,
) {
    let ctx = crate::monitoring::HookContext {
        task_id: task_id.to_string(),
        workdir: workdir.to_path_buf(),
        phase,
        exit_code,
    };
    for hook in monitors {
        let should_fire = matches!(
            (&hook.trigger, phase),
            (crate::monitoring::HookTrigger::OnStart, crate::monitoring::TaskPhase::Running)
                | (crate::monitoring::HookTrigger::OnComplete, crate::monitoring::TaskPhase::Completed)
                | (crate::monitoring::HookTrigger::OnFailure, crate::monitoring::TaskPhase::Failed)
        );
        if should_fire {
            if let Err(e) = hook_executor.execute_hook(hook, &ctx) {
                tracing::warn!(
                    "Hook '{}' for task '{}' failed: {}",
                    hook.name,
                    task_id,
                    e
                );
            }
        }
    }
}

/// Processes a single finished task: waits for exit, updates state, runs collect, fires hooks.
///
/// If the task is already marked as Failed (e.g., timed out), returns immediately without calling `wait()`.
fn process_finished(
    id: &str,
    mut t: InFlightTask,
    state: &mut dyn StateStore,
    hook_executor: &dyn HookExecutor,
) -> Result<(), WorkflowError> {
    // Guard: skip wait() if already marked failed (e.g., timed out)
    if matches!(state.get_status(id), Some(TaskStatus::Failed { .. })) {
        return Ok(());
    }

    // Determine final phase and mark the task accordingly
    let (exit_ok, exit_code) = if let Ok(process_result) = t.handle.wait() {
        match process_result.exit_code {
            Some(0) => (true, Some(0i32)),
            _ => {
                state.mark_failed(
                    id,
                    format!("exit code {}", process_result.exit_code.unwrap_or(-1)),
                );
                (false, process_result.exit_code)
            }
        }
    } else {
        state.mark_failed(id, "process terminated".to_string());
        (false, None)
    };

    let task_phase = if exit_ok {
        // Run collect closure BEFORE deciding final phase
        if let Some(ref collect) = t.collect {
            if let Err(e) = collect(&t.workdir) {
                match t.collect_failure_policy {
                    crate::task::CollectFailurePolicy::FailTask => {
                        state.mark_failed(id, e.to_string());
                    }
                    crate::task::CollectFailurePolicy::WarnOnly => {
                        tracing::warn!(
                            "Collect closure for task '{}' failed: {}",
                            id,
                            e
                        );
                    }
                }
            }
        }
        // Re-read after potential collect failure override
        if matches!(state.get_status(id), Some(TaskStatus::Failed { .. })) {
            crate::monitoring::TaskPhase::Failed
        } else {
            state.mark_completed(id);
            crate::monitoring::TaskPhase::Completed
        }
    } else {
        crate::monitoring::TaskPhase::Failed
    };

    fire_hooks(
        &t.monitors,
        &t.workdir,
        task_phase,
        exit_code,
        id,
        hook_executor,
    );
    state.save()?;

    Ok(())
}

/// Propagates skip status to tasks whose dependencies have failed or been skipped.
///
/// Runs a fixpoint loop: repeatedly finds Pending tasks with failed/skipped
/// dependencies and marks them SkippedDueToDependencyFailure until stable.
fn propagate_skips(
    dag: &Dag,
    state: &mut dyn StateStore,
    tasks: &HashMap<String, Task>,
) -> Result<(), WorkflowError> {
    let mut any_skipped = false;
    let mut changed = true;
    while changed {
        changed = false;
        let to_skip: Vec<String> = dag
            .task_ids()
            .filter(|id| matches!(state.get_status(id), Some(TaskStatus::Pending)))
            .filter(|id| {
                tasks
                    .get(*id)
                    .map(|t| {
                        t.dependencies.iter().any(|dep| {
                            matches!(
                                state.get_status(dep.as_str()),
                                Some(TaskStatus::Failed { .. })
                                    | Some(TaskStatus::Skipped)
                                    | Some(TaskStatus::SkippedDueToDependencyFailure)
                            )
                        })
                    })
                    .unwrap_or(false)
            })
            .cloned()
            .collect();
        if !to_skip.is_empty() {
            changed = true;
            any_skipped = true;
            for id in to_skip.iter() {
                state.mark_skipped_due_to_dep_failure(id);
            }
        }
    }
    if any_skipped {
        state.save()?;
    }
    Ok(())
}

/// Builds the workflow execution summary from final task states.
fn build_summary(state: &dyn StateStore, workflow_start: Instant) -> WorkflowSummary {
    let mut succeeded = Vec::new();
    let mut failed = Vec::new();
    let mut skipped = Vec::new();

    for (id, status) in state.all_tasks() {
        match status {
            TaskStatus::Completed => succeeded.push(id),
            TaskStatus::Failed { error } => failed.push(FailedTask { id, error }),
            TaskStatus::Skipped | TaskStatus::SkippedDueToDependencyFailure => {
                skipped.push(id)
            }
            _ => {}
        }
    }

    WorkflowSummary {
        succeeded,
        failed,
        skipped,
        duration: workflow_start.elapsed(),
    }
}

/// Polls in-flight task handles for completion or timeout.
///
/// Returns the IDs of tasks that have finished (either naturally or via timeout).
/// Timed-out tasks are terminated and marked failed before being returned.
fn poll_finished(
    handles: &mut HashMap<String, InFlightTask>,
    task_timeouts: &HashMap<String, Duration>,
    state: &mut dyn StateStore,
) -> Result<Vec<String>, WorkflowError> {
    let mut finished: Vec<String> = Vec::new();
    for (id, t) in handles.iter_mut() {
        if let Some(&timeout) = task_timeouts.get(id) {
            if t.started_at.elapsed() >= timeout {
                t.handle.terminate().ok();
                state.mark_failed(
                    id,
                    WorkflowError::TaskTimeout(id.clone()).to_string(),
                );
                state.save()?;
                finished.push(id.clone());
                continue;
            }
        }
        if !t.handle.is_running() {
            finished.push(id.clone());
        }
    }
    Ok(finished)
}

/// A task that failed during workflow execution.
#[derive(Debug, Clone)]
pub struct FailedTask {
    pub id: String,
    pub error: String,
}

/// Summary of workflow execution results.
#[derive(Debug, Clone)]
pub struct WorkflowSummary {
    pub succeeded: Vec<String>,
    pub failed: Vec<FailedTask>,
    pub skipped: Vec<String>,
    pub duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::JsonStateStore;
    use std::collections::HashMap;
    use std::io::Write;

    struct StubRunner;
    impl ProcessRunner for StubRunner {
        fn spawn(
            &self,
            workdir: &std::path::Path,
            command: &str,
            args: &[String],
            env: &HashMap<String, String>,
        ) -> Result<Box<dyn ProcessHandle>, WorkflowError> {
            let child = std::process::Command::new(command)
                .args(args)
                .envs(env)
                .current_dir(workdir)
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .map_err(WorkflowError::Io)?;
            Ok(Box::new(StubHandle {
                child: Some(child),
                start: std::time::Instant::now(),
            }))
        }
    }

    struct StubHandle {
        child: Option<std::process::Child>,
        start: std::time::Instant,
    }

    impl ProcessHandle for StubHandle {
        fn is_running(&mut self) -> bool {
            match &mut self.child {
                Some(child) => child.try_wait().ok().flatten().is_none(),
                None => false,
            }
        }
        fn terminate(&mut self) -> Result<(), WorkflowError> {
            match &mut self.child {
                Some(child) => child.kill().map_err(WorkflowError::Io),
                None => Ok(()),
            }
        }
        fn wait(&mut self) -> Result<crate::process::ProcessResult, WorkflowError> {
            let child = self
                .child
                .take()
                .ok_or_else(|| WorkflowError::InvalidConfig("wait() called twice".into()))?;
            let output = child.wait_with_output().map_err(WorkflowError::Io)?;
            Ok(crate::process::ProcessResult {
                exit_code: output.status.code(),
                output: crate::process::OutputLocation::Captured {
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
                },
                duration: self.start.elapsed(),
            })
        }
    }

    struct StubHookExecutor;
    impl HookExecutor for StubHookExecutor {
        fn execute_hook(
            &self,
            _hook: &crate::monitoring::MonitoringHook,
            _ctx: &crate::monitoring::HookContext,
        ) -> Result<crate::monitoring::HookResult, WorkflowError> {
            Ok(crate::monitoring::HookResult {
                success: true,
                output: String::new(),
            })
        }
    }

    #[test]
    fn single_task_completes() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();

        let mut wf = Workflow::new("wf_single").with_max_parallel(4)?;

        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();

        let runner: Arc<dyn ProcessRunner> = Arc::new(StubRunner);
        let executor: Arc<dyn HookExecutor> = Arc::new(StubHookExecutor);
        let state_path = dir.path().join(".wf_single.workflow.json");
        let mut state = Box::new(JsonStateStore::new("wf_single", state_path));

        let summary = wf.run(state.as_mut(), runner, executor)?;
        assert_eq!(summary.succeeded.len(), 1);
        assert!(summary.failed.is_empty());
        Ok(())
    }

    #[test]
    fn chain_respects_order() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();
        let log_file = dir.path().join("log.txt");
        let log_for_a = log_file.clone();
        let log_for_b = log_file.clone();

        let mut wf = Workflow::new("wf_chain").with_max_parallel(4)?;

        wf.add_task(
            Task::new(
                "a",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .setup(move |_| -> Result<(), std::io::Error> {
                let mut f = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_for_a)?;
                writeln!(f, "a")?;
                Ok(())
            }),
        )
        .unwrap();

        wf.add_task(
            Task::new(
                "b",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .depends_on("a")
            .setup(move |_| -> Result<(), std::io::Error> {
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&log_for_b)?;
                writeln!(f, "b")?;
                Ok(())
            }),
        )
        .unwrap();

        let runner: Arc<dyn ProcessRunner> = Arc::new(StubRunner);
        let executor: Arc<dyn HookExecutor> = Arc::new(StubHookExecutor);
        let state_path = dir.path().join(".wf_chain.workflow.json");
        let mut state = Box::new(JsonStateStore::new("wf_chain", state_path));

        wf.run(state.as_mut(), runner, executor)?;

        let log = std::fs::read_to_string(&log_file).unwrap();
        assert_eq!(log.lines().collect::<Vec<_>>(), vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn failed_task_skips_dependent() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();

        let mut wf = Workflow::new("wf_skip").with_max_parallel(4)?;

        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "false".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();

        wf.add_task(
            Task::new(
                "b",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .depends_on("a"),
        )
        .unwrap();

        let runner: Arc<dyn ProcessRunner> = Arc::new(StubRunner);
        let executor: Arc<dyn HookExecutor> = Arc::new(StubHookExecutor);
        let state_path = dir.path().join(".wf_skip.workflow.json");
        let mut state = Box::new(JsonStateStore::new("wf_skip", state_path.clone()));

        wf.run(state.as_mut(), runner, executor)?;

        // Verify in-memory state shows skip propagation actually worked
        assert!(matches!(
            state.get_status("b"),
            Some(TaskStatus::SkippedDueToDependencyFailure)
        ));

        let state = JsonStateStore::load(state_path).unwrap();
        // After load, SkippedDueToDependencyFailure resets to Pending for crash recovery
        assert!(matches!(state.get_status("b"), Some(TaskStatus::Pending)));
        Ok(())
    }

    #[test]
    fn dry_run_returns_topo_order() -> Result<(), WorkflowError> {
        let mut wf = Workflow::new("wf_dry");

        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();

        wf.add_task(
            Task::new(
                "b",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .depends_on("a"),
        )
        .unwrap();

        let order = wf.dry_run()?;
        let pa = order.iter().position(|x| x == "a").unwrap();
        let pb = order.iter().position(|x| x == "b").unwrap();
        assert!(pa < pb);
        Ok(())
    }

    #[test]
    fn duplicate_task_id_errors() -> Result<(), WorkflowError> {
        let mut wf = Workflow::new("wf_dup");

        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();

        assert!(matches!(
            wf.add_task(Task::new(
                "a",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )),
            Err(WorkflowError::DuplicateTaskId(_))
        ));
        Ok(())
    }

    #[test]
    fn valid_dependency_add() -> Result<(), WorkflowError> {
        let mut wf = Workflow::new("wf_dep");

        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();

        assert!(wf
            .add_task(
                Task::new(
                    "b",
                    ExecutionMode::Direct {
                        command: "true".into(),
                        args: vec![],
                        env: HashMap::new(),
                        timeout: None,
                    },
                )
                .depends_on("a")
            )
            .is_ok());
        Ok(())
    }

    #[test]
    fn builder_with_custom_max_parallel() {
        let wf = Workflow::new("test").with_max_parallel(4).unwrap();
        assert_eq!(wf.max_parallel, 4);
    }

    #[test]
    fn three_task_chain_skip_propagation() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();
        let mut wf = Workflow::new("wf_chain_skip").with_max_parallel(4)?;

        wf.add_task(Task::new("a", ExecutionMode::Direct {
            command: "false".into(),
            args: vec![],
            env: HashMap::new(),
            timeout: None,
        })).unwrap();
        wf.add_task(Task::new("b", ExecutionMode::Direct {
            command: "true".into(),
            args: vec![],
            env: HashMap::new(),
            timeout: None,
        }).depends_on("a")).unwrap();
        wf.add_task(Task::new("c", ExecutionMode::Direct {
            command: "true".into(),
            args: vec![],
            env: HashMap::new(),
            timeout: None,
        }).depends_on("b")).unwrap();

        let runner: Arc<dyn ProcessRunner> = Arc::new(StubRunner);
        let executor: Arc<dyn HookExecutor> = Arc::new(StubHookExecutor);
        let state_path = dir.path().join(".wf_chain_skip.workflow.json");
        let mut state = Box::new(JsonStateStore::new("wf_chain_skip", state_path));

        wf.run(state.as_mut(), runner, executor)?;

        assert!(matches!(state.get_status("a"), Some(TaskStatus::Failed { .. })));
        assert!(matches!(state.get_status("b"), Some(TaskStatus::SkippedDueToDependencyFailure)));
        assert!(matches!(state.get_status("c"), Some(TaskStatus::SkippedDueToDependencyFailure)));
        Ok(())
    }

    #[test]
    fn builder_validation_zero_parallelism() {
        let result = Workflow::new("test").with_max_parallel(0);
        assert!(result.is_err());
    }

    #[test]
    fn resume_loads_existing_state() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join(".wf_resume.workflow.json");

        // First run
        let mut state1 = Box::new(JsonStateStore::new("wf_resume", state_path.clone()));
        let mut wf1 = Workflow::new("wf_resume");
        wf1.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();
        wf1.run(
            state1.as_mut(),
            Arc::new(StubRunner),
            Arc::new(StubHookExecutor),
        )?;

        // Second run (resume)
        let mut state2 = Box::new(JsonStateStore::load(&state_path).unwrap());
        let mut wf2 = Workflow::new("wf_resume");
        wf2.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "false".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();
        wf2.run(
            state2.as_mut(),
            Arc::new(StubRunner),
            Arc::new(StubHookExecutor),
        )?;

        // Task "a" should still be Completed (not re-run)
        assert!(state2.is_completed("a"));
        Ok(())
    }

    #[test]
    fn interrupt_before_run_dispatches_nothing() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();
        let mut wf = Workflow::new("wf_interrupt").with_max_parallel(4)?;
        wf.add_task(Task::new(
            "a",
            ExecutionMode::Direct {
                command: "true".into(),
                args: vec![],
                env: HashMap::new(),
                timeout: None,
            },
        ))
        .unwrap();
        wf.interrupt.store(true, Ordering::SeqCst);
        let mut state = JsonStateStore::new(
            "wf_interrupt",
            dir.path().join(".wf_interrupt.workflow.json"),
        );
        let result = wf.run(&mut state, Arc::new(StubRunner), Arc::new(StubHookExecutor));
        assert!(matches!(result.unwrap_err(), WorkflowError::Interrupted));
        assert!(!matches!(
            state.get_status("a"),
            Some(TaskStatus::Completed)
        ));
        Ok(())
    }

    #[test]
    fn interrupt_mid_run_stops_dispatch() -> Result<(), WorkflowError> {
        let dir = tempfile::tempdir().unwrap();
        let mut wf = Workflow::new("wf_interrupt2").with_max_parallel(4)?;
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        wf.add_task(
            Task::new(
                "a",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .setup(move |_| -> Result<(), std::io::Error> {
                flag_clone.store(true, Ordering::SeqCst);
                Ok(())
            }),
        )
        .unwrap();
        wf.add_task(
            Task::new(
                "b",
                ExecutionMode::Direct {
                    command: "true".into(),
                    args: vec![],
                    env: HashMap::new(),
                    timeout: None,
                },
            )
            .depends_on("a"),
        )
        .unwrap();
        wf.interrupt = Arc::clone(&flag);
        let mut state = JsonStateStore::new(
            "wf_interrupt2",
            dir.path().join(".wf_interrupt2.workflow.json"),
        );
        let result = wf.run(&mut state, Arc::new(StubRunner), Arc::new(StubHookExecutor));
        assert!(matches!(result.unwrap_err(), WorkflowError::Interrupted));
        Ok(())
    }
}
