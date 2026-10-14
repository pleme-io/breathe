use breathe_capacity::plan::Step;
use breathe_capacity::{ClusterState, Request, plan, render, values};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "breathe-capacity", about = "Plan a safe, stepped capacity raise and apply it one step at a time")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args)]
struct Inputs {
    #[arg(long)]
    request: PathBuf,
    #[arg(long)]
    values: PathBuf,
    #[arg(long, help = "kubectl get nodes -o json")]
    nodes: PathBuf,
    #[arg(long, help = "kubectl get pods -A -o json")]
    pods: PathBuf,
}

#[derive(Subcommand)]
enum Cmd {
    Plan {
        #[command(flatten)]
        inputs: Inputs,
        #[arg(long)]
        json: bool,
    },
    Next {
        #[command(flatten)]
        inputs: Inputs,
    },
}

enum Failure {
    Input(String),
    Refused(Vec<breathe_capacity::Refusal>),
}

fn read(p: &PathBuf) -> Result<String, Failure> {
    std::fs::read_to_string(p).map_err(|e| Failure::Input(format!("{}: {e}", p.display())))
}

fn json(p: &PathBuf) -> Result<serde_json::Value, Failure> {
    serde_json::from_str(&read(p)?).map_err(|e| Failure::Input(format!("{}: {e}", p.display())))
}

fn compute(i: &Inputs) -> Result<(breathe_capacity::Plan, String, String), Failure> {
    let req: Request = serde_yaml::from_str(&read(&i.request)?).map_err(|e| Failure::Input(format!("request: {e}")))?;
    let text = read(&i.values)?;
    let current = values::read_sizing(&text, &req.chart, &req.service).map_err(|e| Failure::Input(e.to_string()))?;
    let cluster = ClusterState::from_kubectl(&json(&i.nodes)?, &json(&i.pods)?).map_err(|e| Failure::Input(e.to_string()))?;
    let p = plan(&req, current, &cluster).map_err(Failure::Refused)?;
    Ok((p, text, req.chart.root))
}

fn run(cli: Cli) -> Result<(), Failure> {
    match cli.cmd {
        Cmd::Plan { inputs, json } => {
            let (p, _, _) = compute(&inputs)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&p).expect("plan serializes"));
            } else {
                print!("{}", render::markdown(&p));
            }
        }
        Cmd::Next { inputs } => {
            let (p, text, root) = compute(&inputs)?;
            match p.steps.first() {
                None => println!("nothing to do: the values file is already at the target"),
                Some(Step::PoolCeiling { pool, from, to, .. }) => {
                    return Err(Failure::Input(format!(
                        "the next step is the node pool `{pool}` ceiling {from} -> {to}; land and apply it, then re-run"
                    )));
                }
                Some(Step::PodSizing { index, edits, .. }) => {
                    let out = values::apply(&text, &root, edits).map_err(|e| Failure::Input(e.to_string()))?;
                    std::fs::write(&inputs.values, out).map_err(|e| Failure::Input(e.to_string()))?;
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
