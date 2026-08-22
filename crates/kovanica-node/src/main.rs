//! * `kovanica-node` (or `kovanica-node serve`) — a REPL: read one command per
//!   line from stdin, print the response to stdout. `quit`/`exit` ends it.
//! * `kovanica-node demo` — replay a scripted end-to-end scenario, printing each
//!   command and its response, so the whole stack can be exercised in one run.
//! * `kovanica-node explorer [addr]` — self-hosted BlockDAG explorer (JSON API +
//!   UI) on `addr`, default `0.0.0.0:8080`. TCP P2P (the only network path)
//!   binds `KOVANICA_LISTEN` (default `0.0.0.0:9000`) in the same process.

use kovanica_node::{rpc, Node};
use std::io::{self, BufRead, Write};

fn main() {
    let mode = std::env::args().nth(1);
    match mode.as_deref() {
        None | Some("serve") => serve(),
        Some("demo") => demo(),
        Some("explorer") => {
            let addr = std::env::args()
                .nth(2)
                .unwrap_or_else(|| "0.0.0.0:8080".into());
            if let Err(e) = kovanica_node::serve_explorer(&addr) {
                eprintln!("explorer failed: {e}");
                std::process::exit(1);
            }
        }
        Some("help") | Some("-h") | Some("--help") => {
            println!("usage: kovanica-node [serve|demo|explorer [addr]]");
            println!("  serve     read commands from stdin (default)");
            println!("  demo      run a scripted end-to-end scenario");
            println!("  explorer  HTTP UI + JSON API (default 0.0.0.0:8080)");
            println!("            TCP P2P on KOVANICA_LISTEN (default 0.0.0.0:9000)");
            println!("            env: KOVANICA_DATA  KOVANICA_MINE=0|1  KOVANICA_MINE_SECS=120");
            println!("                 KOVANICA_FAUCET=0|1  KOVANICA_TAP=0|1");
            println!("                 KOVANICA_ALLOW_RESET=0|1  KOVANICA_OPERATOR=0|1");
            println!("                 KOVANICA_LISTEN=0.0.0.0:9000   (off to disable)");
            println!("                 KOVANICA_PEERS=seed.kovanica.online:9000");
            println!("                 KOVANICA_POW=0|1  (default 1, consensus hash target)");
            println!();
            println!("{}", rpc::HELP);
        }
        Some(other) => {
            eprintln!("unknown mode '{other}' (use: serve | demo | explorer | help)");
            std::process::exit(2);
        }
    }
}

/// Read commands from stdin, print one response line each.
fn serve() {
    let mut node = Node::new();
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let _ = writeln!(stdout, "kovanica-node ready — type `help`, `quit` to exit");
    let _ = stdout.flush();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let trimmed = line.trim();
        if trimmed == "quit" || trimmed == "exit" {
            break;
        }
        let response = rpc::execute_line(&mut node, trimmed);
        if writeln!(stdout, "{response}").is_err() {
            break;
        }
        let _ = stdout.flush();
    }
}

/// Replay a fixed scenario, printing a transcript.
fn demo() {
    let mut node = Node::new();
    let script = [
        "genesis 3 1000 500 1",
        "balance 1",
        "send 1 200 2",
        "balance 1",
        "balance 2",
        "pool 2 50 3",
        "pending",
        "produce",
        "pending",
        "balance 2",
        "balance 3",
        "tip",
        "len",
    ];
    for cmd in script {
        println!("> {cmd}");
        println!("{}", rpc::execute_line(&mut node, cmd));
    }
}
