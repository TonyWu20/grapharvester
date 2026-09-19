use std::path::PathBuf;

use clap::Parser;

/// S4U damping mechanism validation — parameter sweep over beta, gamma, spin_amp.
#[derive(Parser, Debug, Clone)]
#[command(name = "s4u_damping_sweep")]
pub struct SweepConfig {
    /// Comma-separated s4u_beta values (SCF-level U damping)
    #[arg(long, default_value = "0.0,0.7")]
    pub beta_values: String,

    /// Comma-separated s4u_gamma values (geometry-level U mixing)
    #[arg(long, default_value = "0.30,0.70,1.0")]
    pub gamma_values: String,

    /// Comma-separated mix_spin_amp values
    #[arg(long, default_value = "0.5,2.0")]
    pub spin_amp_values: String,

    /// Maximum number of concurrent tasks (10-core Mac: only 1 with np=7)
    #[arg(long, default_value = "1")]
    pub max_parallel: usize,

    /// Run locally (no SLURM submission)
    #[arg(long)]
    pub local: bool,

    /// Print task order without executing
    #[arg(long)]
    pub dry_run: bool,

    /// CASTEP MPI binary path
    #[arg(long, default_value = "castep.mpi")]
    pub castep_command: String,

    /// Number of MPI processes
    #[arg(long, default_value = "7")]
    pub mpi_np: usize,

    /// Root directory for experiment run directories
    #[arg(long, default_value = "runs")]
    pub workdir: PathBuf,
}
