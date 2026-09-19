//! S4U Damping Mechanism Validation Sweep
//!
//! Sweeps over s4u_beta, s4u_gamma, and mix_spin_amp for NiO AFM-II
//! to determine which damping mechanisms are necessary.
//!
//! Usage:
//!   s4u_damping_sweep --beta "0.0,0.7" --gamma "0.30,0.70,1.0" --spin-amp "0.5,2.0" --local

mod config;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use itertools::iproduct;
use workflow_core::prelude::*;
use workflow_core::process::ProcessRunner;
use workflow_core::task::ExecutionMode;
use workflow_utils::prelude::*;

use config::SweepConfig;

const SEED_NAME: &str = "NiO";
const SEED_CELL: &str = include_str!("../seeds/NiO.cell");
const SEED_PARAM: &str = include_str!("../seeds/NiO.param");

fn main() -> anyhow::Result<()> {
    workflow_core::init_default_logging().ok();
    let config = SweepConfig::parse();

    let tasks = build_all_tasks(&config)?;

    let mut workflow = Workflow::new("s4u_damping_sweep")
        .with_max_parallel(config.max_parallel)?
        .with_log_dir("runs/logs");

    for task in tasks {
        workflow.add_task(task)?;
    }

    if config.dry_run {
        let order = workflow.dry_run()?;
        println!("Dry-run topological order ({} tasks):", order.len());
        for task_id in &order {
            println!("  {task_id}");
        }
        return Ok(());
    }

    let state_path = PathBuf::from(".s4u_damping_sweep.workflow.json");
    let mut state = JsonStateStore::new("s4u_damping_sweep", state_path);
    let runner: Arc<dyn ProcessRunner> = Arc::new(
        SystemProcessRunner::with_log_dir("runs/logs")
    );
    let executor: Arc<dyn HookExecutor> = Arc::new(ShellHookExecutor);
    let summary = workflow.run(&mut state, runner, executor)?;

    println!(
        "Workflow complete: {} succeeded, {} failed, {} skipped ({:.1}s)",
        summary.succeeded.len(),
        summary.failed.len(),
        summary.skipped.len(),
        summary.duration.as_secs_f64()
    );

    if !summary.failed.is_empty() {
        println!("\nFailed tasks:");
        for f in &summary.failed {
            println!("  {}: {}", f.id, f.error);
        }
    }

    Ok(())
}

fn build_all_tasks(config: &SweepConfig) -> anyhow::Result<Vec<Task>> {
    let betas: Vec<f64> = config
        .beta_values
        .split(',')
        .map(|s| s.trim().parse::<f64>().map_err(|e| anyhow::anyhow!("Invalid beta value '{}': {}", s, e)))
        .collect::<Result<_, _>>()?;
    let gammas: Vec<f64> = config
        .gamma_values
        .split(',')
        .map(|s| s.trim().parse::<f64>().map_err(|e| anyhow::anyhow!("Invalid gamma value '{}': {}", s, e)))
        .collect::<Result<_, _>>()?;
    let spin_amps: Vec<f64> = config
        .spin_amp_values
        .split(',')
        .map(|s| s.trim().parse::<f64>().map_err(|e| anyhow::anyhow!("Invalid spin_amp value '{}': {}", s, e)))
        .collect::<Result<_, _>>()?;

    if betas.is_empty() || gammas.is_empty() || spin_amps.is_empty() {
        anyhow::bail!("All parameter lists must be non-empty");
    }

    let combinations: Vec<_> = iproduct!(betas, gammas, spin_amps).collect();
    let mut tasks = Vec::with_capacity(combinations.len());

    for (beta, gamma, spin_amp) in combinations {
        let task = build_one_task(config, beta, gamma, spin_amp)?;
        tasks.push(task);
    }

    Ok(tasks)
}

fn build_one_task(
    config: &SweepConfig,
    beta: f64,
    gamma: f64,
    spin_amp: f64,
) -> anyhow::Result<Task> {
    let task_id = format!("beta{:.1}_gamma{:.2}_spin{:.1}", beta, gamma, spin_amp)
        .replace('.', "p");
    let workdir = config.workdir.join(&task_id);
    let castep_cmd = config.castep_command.clone();
    let setup_seed = SEED_NAME.to_string();
    let collect_seed = SEED_NAME.to_string();
    let mode_seed = SEED_NAME.to_string();

    let mode = if config.local {
        let mut env = HashMap::new();
        env.insert("OMP_NUM_THREADS".to_string(), "1".to_string());
        let np_str = config.mpi_np.to_string();
        ExecutionMode::Direct {
            command: "mpirun".to_string(),
            args: vec![
                "-np".to_string(),
                np_str,
                castep_cmd,
                mode_seed,
            ],
            env,
            timeout: None,
        }
    } else {
        ExecutionMode::Queued
    };
    let task = Task::new(task_id.clone(), mode)
        .workdir(workdir.clone())
        .setup(move |path: &Path| -> Result<(), WorkflowError> {
            std::fs::create_dir_all(path).map_err(|e| WorkflowError::IoWithPath {
                path: path.to_path_buf(),
                source: e,
            })?;

            // Build devel_code line
            let devel_code = format!(
                "devel_code: PROF:*:ENDPROF s4u=true s4u_beta={} s4u_gamma={}",
                beta, gamma
            );

            // Substitute placeholders in param
            let param_content = SEED_PARAM
                .replace("__SPIN_AMP__", &format!("{}", spin_amp))
                .replace("__DEVEL_CODE__", &devel_code);

            // Write .cell (unchanged from seed)
            write_file(&path.join(format!("{}.cell", setup_seed)), SEED_CELL)?;

            // Write .param with substitutions
            write_file(&path.join(format!("{}.param", setup_seed)), &param_content)?;

            Ok(())
        })
        .collect(move |path: &Path| -> Result<(), WorkflowError> {
            let output = read_file(path.join(format!("{}.castep", collect_seed)))?;
            if !output.contains("Total time") {
                return Err(WorkflowError::InvalidConfig(
                    "CASTEP output missing 'Total time' marker".into(),
                ));
            }
            Ok(())
        });

    Ok(task)
}
