use breathe_capacity::config::{APP, CapacityConfig};
use breathe_capacity::plan::Step;
use breathe_capacity::{ClusterState, render, values};
use clap::{Args, Parser, Subcommand};
use serde_json::{Map, Value, json};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = APP, about = "Plan a safe, stepped capacity raise and apply it one step at a time")]
struct Cli {
    #[command(flatten)]
    config: shikumi::cli::ConfigArgs,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Default)]
struct Flags {
    #[arg(long, global = true)]
    target: Option<String>,
    #[arg(long, global = true)]
    service: Option<String>,
    #[arg(long, global = true, value_name = "QUANTITY")]
    memory_request: Option<String>,
    #[arg(long, global = true, value_name = "QUANTITY")]
    cpu_request: Option<String>,
    #[arg(long, global = true)]
    min_replicas: Option<u32>,
    #[arg(long, global = true)]
    max_replicas: Option<u32>,
    #[arg(long, global = true)]
    replicas: Option<u32>,
    #[arg(long, global = true, value_name = "QUANTITY")]
    mem_p95: Option<String>,
    #[arg(long, global = true, value_name = "QUANTITY")]
    cpu_p95: Option<String>,
    #[arg(long, global = true)]
    db_conns_per_pod: Option<u32>,
    #[arg(long, global = true)]
    db_max_connections: Option<u32>,
    #[arg(long, global = true, help = "kubectl get nodes -o json")]
    nodes: Option<PathBuf>,
    #[arg(long, global = true, help = "kubectl get pods -A -o json")]
    pods: Option<PathBuf>,
    #[arg(long, global = true)]
    repo_root: Option<PathBuf>,
    #[arg(long, global = true)]
    max_step_pct: Option<u32>,
    #[arg(long, global = true)]
    surge_pct: Option<u32>,
}

impl Flags {
    fn partial(&self) -> Result<Value, Failure> {
        let raises: Vec<(&str, Value)> = [
            self.memory_request.as_ref().map(|v| ("memoryRequest", json!(v))),
            self.cpu_request.as_ref().map(|v| ("cpuRequest", json!(v))),
            self.min_replicas.map(|v| ("minReplicas", json!(v))),
            self.max_replicas.map(|v| ("maxReplicas", json!(v))),
        ]
        .into_iter()
        .flatten()
        .collect();
        if raises.len() > 1 {
            return Err(Failure::Input("pass at most one of --memory-request, --cpu-request, --min-replicas, --max-replicas".into()));
        }
        let raise = raises.into_iter().next().map(|(k, v)| Value::Object(Map::from_iter([(k.to_string(), v)])));
        Ok(json!({
            "repoRoot": self.repo_root,
            "policy": { "maxStepPct": self.max_step_pct, "surgePct": self.surge_pct },
            "run": {
                "target": self.target,
                "service": self.service,
                "raise": raise,
                "observed": {
                    "replicas": self.replicas,
                    "memP95": self.mem_p95,
                    "cpuP95": self.cpu_p95,
                    "dbConnsPerPod": self.db_conns_per_pod,
                },
                "db": { "maxConnections": self.db_max_connections },
                "nodes": self.nodes,
                "pods": self.pods,
            }
        }))
    }
}

#[derive(Subcommand)]
enum Cmd {
    Plan {
        #[command(flatten)]
        flags: Flags,
        #[arg(long)]
        json: bool,
    },
    Next {
        #[command(flatten)]
        flags: Flags,
    },
    Targets {
        #[command(flatten)]
        flags: Flags,
    },
    ConfigShow(shikumi::cli::ConfigShowCommand),
}

enum Failure {
    Input(String),
    Refused(Vec<breathe_capacity::Refusal>),
}

fn input<E: std::fmt::Display>(e: E) -> Failure {
    Failure::Input(e.to_string())
}

fn load(args: &shikumi::cli::ConfigArgs, flags: &Flags) -> Result<CapacityConfig, Failure> {
    Ok(args.resolve::<CapacityConfig>(APP, &flags.partial()?).map_err(input)?.value().clone())
}

fn read(p: &PathBuf) -> Result<String, Failure> {
    std::fs::read_to_string(p).map_err(|e| Failure::Input(format!("{}: {e}", p.display())))
}

fn json_file(p: &PathBuf) -> Result<Value, Failure> {
    serde_json::from_str(&read(p)?).map_err(|e| Failure::Input(format!("{}: {e}", p.display())))
}

fn compute(cfg: &CapacityConfig) -> Result<(breathe_capacity::Plan, String, String, PathBuf), Failure> {
    let r = cfg.resolve().map_err(input)?;
    let text = read(&r.values)?;
    let current = values::read_sizing(&text, &r.request.chart, &r.request.service).map_err(input)?;
    let cluster = ClusterState::from_kubectl(&json_file(&r.nodes)?, &json_file(&r.pods)?).map_err(input)?;
    let p = breathe_capacity::plan(&r.request, current, &cluster).map_err(Failure::Refused)?;
    Ok((p, text, r.request.chart.root, r.values))
}

fn run(cli: Cli) -> Result<(), Failure> {
    match cli.cmd {
        Cmd::ConfigShow(cmd) => cmd.run::<CapacityConfig>("BREATHE_CAPACITY_TIER").map_err(input)?,
        Cmd::Targets { flags } => {
            let cfg = load(&cli.config, &flags)?;
            for (name, t) in &cfg.targets {
                println!("{name}\t{}\t{:?}\t{}", t.tenant, t.environment, t.context.as_deref().unwrap_or("-"));
            }
        }
        Cmd::Plan { flags, json } => {
            let (p, ..) = compute(&load(&cli.config, &flags)?)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&p).expect("plan serializes"));
            } else {
                print!("{}", render::markdown(&p));
            }
        }
        Cmd::Next { flags } => {
            let (p, text, root, path) = compute(&load(&cli.config, &flags)?)?;
            match p.steps.first() {
                None => println!("nothing to do: the values file is already at the target"),
                Some(Step::PoolCeiling { pool, from, to, .. }) => {
                    return Err(Failure::Input(format!(
                        "the next step is the node pool `{pool}` ceiling {from} -> {to}; land and apply it, then re-run"
                    )));
                }
                Some(Step::PodSizing { index, edits, .. }) => {
                    let out = values::apply(&text, &root, edits).map_err(input)?;
                    std::fs::write(&path, out).map_err(input)?;
                    for e in edits {
                        println!("step {index}: {}.{} {} -> {}", e.service, e.key, e.from, e.to);
                    }
                    println!("{} step(s) remain after this one", p.steps.len() - 1);
                }
            }
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Input(m)) => {
            eprintln!("error: {m}");
            ExitCode::from(2)
        }
        Err(Failure::Refused(rs)) => {
            for r in &rs {
                eprintln!("refused: {}", render::refusal(r));
            }
            ExitCode::from(3)
        }
    }
}
