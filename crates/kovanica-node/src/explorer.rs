//! Self-hosted BlockDAG explorer: JSON API + a static UI, served from the
//! Rust node. The page never reimplements consensus — it only renders what
//! [`Mesh`] / [`Node`] already computed.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use kovanica_dag::BlockId;
use kovanica_state::{Address, Transaction};

use crate::net::{encode_records, pull_blocks_timeout};
use crate::node::{Node, HALVING_ERA};
use crate::p2p::Mesh;

const UI: &str = include_str!("explorer.html");
const BIP39: &str = include_str!("bip39-english.txt");
const DOCS: &str = include_str!("../../../TESTNET.md");
/// 1 KVNC = 10^8 base units (atoms).
const ATOM: u64 = 100_000_000;
const GENESIS_SUBSIDY: u64 = 50 * ATOM;
const GENESIS_PREMINE: u64 = 50 * ATOM;
const NETWORK: &str = "kovanica-testnet-1";
const ACTORS: [u64; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
/// Single P2P path: plaintext TCP. Not 80/443/3010/8080 and not libp2p :30333.
const P2P_LISTEN_DEFAULT: &str = "0.0.0.0:9000";
const P2P_BOOTSTRAP: &str = "explorer.kovanica.online:9000";

/// Bind `addr` (e.g. `0.0.0.0:8080`) and serve the explorer until killed.
pub fn serve(addr: impl ToSocketAddrs) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let bound = listener.local_addr()?;
    eprintln!("kovanica explorer on http://{bound}");
    let mut app = Explorer::boot_persist();
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(e) = handle(&mut app, stream) {
                    eprintln!("explorer: {e}");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                app.tick();
                thread::sleep(Duration::from_millis(40));
            }
            Err(e) => return Err(e),
        }
    }
}

struct Explorer {
    mesh: Mesh,
    selected: String,
    mining: bool,
    ticks: u64,
    rotate: usize,
    faucet: bool,
    allow_reset: bool,
    operator: bool,
    listen: Option<TcpListener>,
    listen_addr: String,
    peers: Vec<String>,
    origins: HashMap<String, u64>,
}

impl Explorer {
    fn boot() -> Self {
        let mut mesh = line_mesh();
        mesh.drain(16);
        Self {
            mesh,
            selected: "alpha".into(),
            mining: false,
            ticks: 0,
            rotate: 0,
            faucet: true,
            allow_reset: true,
            operator: true,
            listen: None,
            listen_addr: String::new(),
            peers: Vec::new(),
            origins: HashMap::new(),
        }
    }

    fn tick(&mut self) {
        self.mesh.tick();
        self.ticks += 1;
        self.tick_p2p();
        if self.mining && self.ticks % 120 == 0 {
            let names = self.mesh.names();
            if !names.is_empty() {
                let name = &names[self.rotate % names.len()];
                let _ = self.mesh.produce_empty(name);
                self.rotate += 1;
                persist_all(&self.mesh);
            }
        }
    }

    fn tick_p2p(&mut self) {
        if let Some(listener) = self.listen.as_ref() {
            if let Ok((mut stream, peer)) = listener.accept() {
                if let Some(n) = self.mesh.node("alpha") {
                    let bytes = encode_records(&n.export());
                    let _ = stream.write_all(&bytes);
                    let _ = stream.flush();
                    eprintln!("kovanica p2p served {} bytes to {peer}", bytes.len());
                }
            }
        }
        if !self.peers.is_empty() && self.ticks % 250 == 0 {
            self.sync_peers(Duration::from_millis(800), false);
        }
    }

    fn sync_peers(&mut self, timeout: Duration, log: bool) {
        let peers = self.peers.clone();
        if peers.is_empty() {
            return;
        }
        if let Some(n) = self.mesh.node_mut("alpha") {
            for addr in peers {
                match pull_blocks_timeout(&addr, n, timeout) {
                    Ok(k) if k > 0 => {
                        eprintln!("kovanica p2p pulled {k} records from {addr}");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if log {
                            eprintln!("kovanica p2p pull {addr}: {e}");
                        }
                    }
                }
            }
        }
        persist_all(&self.mesh);
    }

    fn boot_persist() -> Self {
        let _ = fs::create_dir_all(data_dir());
        ensure_network();
        let mut mesh = Mesh::new();
        mesh.add("alpha", load_or_genesis("alpha"));
        if env_flag("KOVANICA_DEMO_MESH", false) {
            mesh.add("beta", load_or_genesis("beta"));
            mesh.add("gamma", load_or_genesis("gamma"));
            let _ = mesh.connect("alpha", "beta");
            let _ = mesh.connect("beta", "gamma");
        }
        mesh.drain(16);
        persist_all(&mesh);
        let listen = bind_p2p();
        let listen_addr = listen
            .as_ref()
            .and_then(|l| l.local_addr().ok())
            .map(|a| a.to_string())
            .unwrap_or_default();
        let peers = peer_list();
        if !listen_addr.is_empty() || !peers.is_empty() {
            eprintln!("kovanica p2p listen={listen_addr} peers={peers:?}");
        }
        let mut app = Self {
            mesh,
            selected: "alpha".into(),
            mining: env_flag("KOVANICA_MINE", false),
            ticks: 0,
            rotate: 0,
            faucet: env_flag("KOVANICA_FAUCET", false),
            allow_reset: env_flag("KOVANICA_ALLOW_RESET", false),
            operator: env_flag("KOVANICA_OPERATOR", false),
            listen,
            listen_addr,
            peers,
            origins: load_origins(),
        };
        app.sync_peers(Duration::from_secs(3), true);
        app
    }

    fn select(&mut self, name: &str) {
        if self.mesh.node(name).is_some() {
            self.selected = name.to_string();
        }
    }
}

fn data_dir() -> PathBuf {
    std::env::var("KOVANICA_DATA")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"))
}

fn snap_path(name: &str) -> PathBuf {
    data_dir().join(format!("{name}.snap"))
}

fn miner_path(name: &str) -> PathBuf {
    data_dir().join(format!("{name}.miner"))
}

fn persist_all(mesh: &Mesh) {
    let _ = fs::create_dir_all(data_dir());
    for name in mesh.names() {
        if let Some(n) = mesh.node(&name) {
            if let Some(p) = snap_path(&name).to_str() {
                let _ = n.save(p);
            }
            if let Some(m) = n.miner() {
                let _ = fs::write(miner_path(&name), m.to_hex());
            }
        }
    }
}

fn wipe_data() {
    let dir = data_dir();
    if let Ok(rd) = fs::read_dir(&dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().and_then(|s| s.to_str()) == Some("snap")
                || p.extension().and_then(|s| s.to_str()) == Some("miner")
            {
                let _ = fs::remove_file(p);
            }
        }
    }
}

fn load_or_genesis(name: &str) -> Node {
    let snap = snap_path(name);
    if snap.is_file() {
        let mut node = Node::new();
        if let Some(p) = snap.to_str() {
            if node.load(p).is_ok() {
                if let Ok(h) = fs::read_to_string(miner_path(name)) {
                    if let Ok(addr) = parse_addr(h.trim()) {
                        node.set_miner(addr);
                    }
                } else {
                    node.set_miner(Node::address(1));
                }
                if env_flag("KOVANICA_POW", true) {
                    let _ = node.set_proof_of_work(true);
                }
                return node;
            }
        }
    }
    genesis_node()
}

fn line_mesh() -> Mesh {
    let mut mesh = Mesh::new();
    mesh.add("alpha", genesis_node());
    mesh.add("beta", genesis_node());
    mesh.add("gamma", genesis_node());
    let _ = mesh.connect("alpha", "beta");
    let _ = mesh.connect("beta", "gamma");
    mesh
}

fn genesis_node() -> Node {
    let mut node = Node::new();
    node.set_now_ms(1_700_000_000_000 + 1_000);
    node.genesis(3, GENESIS_SUBSIDY, GENESIS_PREMINE, 1).expect("genesis");
    if env_flag("KOVANICA_POW", true) {
        let _ = node.set_proof_of_work(true);
    }
    node
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"),
        Err(_) => default,
    }
}

fn ensure_network() {
    let marker = data_dir().join("network");
    let ok = fs::read_to_string(&marker)
        .map(|s| s.trim() == NETWORK)
        .unwrap_or(false);
    if !ok {
        wipe_data();
        let _ = fs::create_dir_all(data_dir());
        let _ = fs::write(marker, NETWORK);
    }
}

fn bind_p2p() -> Option<TcpListener> {
    let raw = std::env::var("KOVANICA_LISTEN").unwrap_or_else(|_| P2P_LISTEN_DEFAULT.into());
    if env_off(&raw) {
        eprintln!("kovanica p2p listen disabled");
        return None;
    }
    match TcpListener::bind(&raw) {
        Ok(listener) => {
            if let Err(e) = listener.set_nonblocking(true) {
                eprintln!("kovanica p2p listen {raw} nonblocking failed: {e}");
                return None;
            }
            if let Ok(local) = listener.local_addr() {
                eprintln!("kovanica p2p listen {local}");
            }
            Some(listener)
        }
        Err(e) => {
            eprintln!("kovanica p2p listen {raw} failed: {e}");
            None
        }
    }
}

fn peer_list() -> Vec<String> {
    match std::env::var("KOVANICA_PEERS") {
        Ok(s) if env_off(s.trim()) => Vec::new(),
        Ok(s) => s
            .split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect(),
        Err(_) => vec![P2P_BOOTSTRAP.to_string()],
    }
}

fn env_off(v: &str) -> bool {
    matches!(v, "" | "0" | "off" | "none" | "false" | "FALSE")
}

fn origins_path() -> PathBuf {
    data_dir().join("origins.txt")
}

fn load_origins() -> HashMap<String, u64> {
    let mut map = HashMap::new();
    let Ok(text) = fs::read_to_string(origins_path()) else {
        return map;
    };
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        let Some(iso) = parts.next() else {
            continue;
        };
        let Some(n) = parts.next().and_then(|s| s.parse().ok()) else {
            continue;
        };
        if iso.len() == 3 && iso.chars().all(|c| c.is_ascii_alphabetic()) {
            map.insert(iso.to_ascii_uppercase(), n);
        }
    }
    map
}

fn save_origins(map: &HashMap<String, u64>) {
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by(|a, b| a.0.cmp(b.0));
    let body: String = rows
        .into_iter()
        .map(|(k, v)| format!("{k} {v}\n"))
        .collect();
    let _ = fs::create_dir_all(data_dir());
    let _ = fs::write(origins_path(), body);
}

fn origins_json(map: &HashMap<String, u64>) -> String {
    let mut rows: Vec<_> = map.iter().collect();
    rows.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
    let items = rows
        .into_iter()
        .map(|(iso, n)| format!("{{\"iso3\":{},\"pulses\":{}}}", jstr(iso), n));
    format!("{{\"pulses\":{}}}", jarr(items))
}

fn handle(app: &mut Explorer, mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = [0u8; 8192];
    let n = match stream.read(&mut buf) {
        Ok(0) => return Ok(()),
        Ok(n) => n,
        Err(_) => return Ok(()),
    };
    let req = String::from_utf8_lossy(&buf[..n]);
    let mut lines = req.split("\r\n");
    let first = lines.next().unwrap_or("");
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or("GET");
    let target = parts.next().unwrap_or("/");
    let (path, query) = split_query(target);

    if method == "HEAD" && (path == "/" || path == "/index.html" || path == "/wallet") {
        return respond(&mut stream, 200, "text/html; charset=utf-8", b"");
    }
    if method == "GET" && (path == "/" || path == "/index.html" || path == "/wallet") {
        return respond(&mut stream, 200, "text/html; charset=utf-8", UI.as_bytes());
    }
    if method == "GET" && path == "/bip39.txt" {
        return respond(&mut stream, 200, "text/plain; charset=utf-8", BIP39.as_bytes());
    }
    if method == "GET" && path == "/kovanica-explorer-wallet.patch" {
        let body = std::fs::read("/workspace/kovanica-explorer-wallet.patch")
            .or_else(|_| std::fs::read("/tmp/kovanica-explorer-wallet.patch"))
            .unwrap_or_default();
        return respond_download(
            &mut stream,
            "text/x-patch; charset=utf-8",
            "kovanica-explorer-wallet.patch",
            &body,
        );
    }
    if method == "GET" && path == "/docs" {
        return respond(&mut stream, 200, "text/plain; charset=utf-8", DOCS.as_bytes());
    }
    if method == "GET" && path == "/api/bootstrap" {
        let n = app.mesh.node(&app.selected);
        let genesis = n
            .and_then(|n| n.ledger().ok())
            .map(|l| l.genesis().to_string())
            .unwrap_or_default();
        let tip = n
            .and_then(|n| n.selected_tip().ok())
            .map(|t| t.to_string())
            .unwrap_or_default();
        let pow = n.map(|n| n.proof_of_work()).unwrap_or(false);
        let min_fee = n.map(|n| n.min_fee()).unwrap_or(0);
        let peers = jarr(
            app.peers
                .iter()
                .cloned()
                .chain(std::iter::once(app.listen_addr.clone()).filter(|s| !s.is_empty()))
                .map(|s| jstr(&s)),
        );
        let body = format!(
            "{{\"network\":{},\"genesis\":{},\"tip\":{},\"listen\":{},\"peers\":{},\"pow\":{},\"min_fee\":{},\"atom\":{},\"token\":\"KVNC\",\"k\":3}}",
            jstr(NETWORK),
            jstr(&genesis),
            jstr(&tip),
            jstr(&app.listen_addr),
            peers,
            pow,
            min_fee,
            ATOM
        );
        return respond(&mut stream, 200, "application/json", body.as_bytes());
    }
    if method == "GET" && path == "/api/state" {
        if let Some(node) = query.get("node") {
            app.select(node);
        }
        let body = snapshot(app);
        return respond(&mut stream, 200, "application/json", body.as_bytes());
    }
    if method == "GET" && path == "/api/head" {
        if let Some(n) = app.mesh.node(&app.selected) {
            let genesis = n
                .ledger()
                .ok()
                .map(|l| l.genesis().to_string())
                .unwrap_or_default();
            let tip = n.selected_tip().map(|t| t.to_string()).unwrap_or_default();
            let blocks = n.block_count().unwrap_or(0);
            let body = format!(
                "{{\"network\":{},\"genesis\":{},\"tip\":{},\"blocks\":{},\"min_fee\":{},\"atom\":{}}}",
                jstr(NETWORK),
                jstr(&genesis),
                jstr(&tip),
                blocks,
                n.min_fee(),
                ATOM
            );
            return respond(&mut stream, 200, "application/json", body.as_bytes());
        }
    }
    if method == "GET" && path == "/api/p2p" {
        let body = format!(
            "{{\"path\":\"tcp\",\"listen\":{},\"peers\":{},\"bootstrap\":{}}}",
            jstr(&app.listen_addr),
            jarr(app.peers.iter().map(|s| jstr(s))),
            jstr(P2P_BOOTSTRAP)
        );
        return respond(&mut stream, 200, "application/json", body.as_bytes());
    }
    if method == "GET" && path == "/api/origins" {
        return respond(
            &mut stream,
            200,
            "application/json",
            origins_json(&app.origins).as_bytes(),
        );
    }
    if method == "GET" && path == "/api/blocks" {
        if let Some(n) = app.mesh.node(&app.selected) {
            let bytes = encode_records(&n.export());
            return respond(&mut stream, 200, "application/octet-stream", &bytes);
        }
    }
    if method == "GET" && path == "/api/history" {
        match history_json(app, &query) {
            Ok(body) => {
                return respond(&mut stream, 200, "application/json", body.as_bytes())
            }
            Err(e) => {
                return respond(&mut stream, 400, "text/plain; charset=utf-8", e.as_bytes())
            }
        }
    }
    if method == "GET" && path == "/api/utxos" {
        match utxos_json(app, &query) {
            Ok(body) => {
                return respond(&mut stream, 200, "application/json", body.as_bytes())
            }
            Err(e) => {
                return respond(&mut stream, 400, "text/plain; charset=utf-8", e.as_bytes())
            }
        }
    }
    if method == "POST" && path.starts_with("/api/") {
        let action = path.trim_start_matches("/api/");
        if let Some(node) = query.get("node") {
            app.select(node);
        }
        match dispatch(app, action, &query) {
            Ok(body) => {
                persist_all(&app.mesh);
                return respond(&mut stream, 200, "application/json", body.as_bytes())
            }
            Err(e) => {
                return respond(&mut stream, 400, "text/plain; charset=utf-8", e.as_bytes())
            }
        }
    }
    respond(&mut stream, 404, "text/plain; charset=utf-8", b"not found")
}

fn dispatch(
    app: &mut Explorer,
    action: &str,
    q: &std::collections::HashMap<String, String>,
) -> Result<String, String> {
    let node = q.get("node").cloned().unwrap_or_else(|| app.selected.clone());
    match action {
        "produce" => {
            match app.mesh.produce(&node).map_err(|e| e.to_string())? {
                Some(_) => {}
                None => {
                    if !app.operator {
                        return Err("mempool empty".into());
                    }
                    app.mesh.produce_empty(&node).map_err(|e| e.to_string())?;
                }
            }
        }
        "empty" | "send" | "pool" | "parallel" | "fork" | "mining" | "miner" => {
            if !app.operator {
                return Err("operator only".into());
            }
            match action {
                "empty" => {
                    app.mesh.produce_empty(&node).map_err(|e| e.to_string())?;
                }
                "send" => {
                    let from = parse_u64(q, "from", 1)?;
                    let amount = parse_u64(q, "amount", 50)?;
                    let to = parse_u64(q, "to", 2)?;
                    app.mesh
                        .send(&node, from, amount, to)
                        .map_err(|e| e.to_string())?;
                }
                "pool" => {
                    let from = parse_u64(q, "from", 1)?;
                    let amount = parse_u64(q, "amount", 50)?;
                    let to = parse_u64(q, "to", 2)?;
                    app.mesh
                        .pool(&node, from, amount, to)
                        .map_err(|e| e.to_string())?;
                }
                "parallel" => {
                    let _ = app.mesh.send("alpha", 1, ATOM, 2);
                    let _ = app.mesh.send("beta", 1, ATOM, 3);
                }
                "fork" => {
                    for name in app.mesh.names() {
                        let _ = app.mesh.produce_empty(&name);
                    }
                }
                "mining" => {
                    app.mining = q.get("on").map(|v| v != "0").unwrap_or(true);
                }
                "miner" => {
                    let addr = parse_addr(q.get("addr").ok_or("addr required")?)?;
                    let n = app
                        .mesh
                        .node_mut(&node)
                        .ok_or_else(|| "unknown node".to_string())?;
                    n.set_miner(addr);
                    return Ok(format!("{{\"ok\":true,\"miner\":{}}}", jstr(&addr.to_hex())));
                }
                _ => {}
            }
        }
        "reset" => {
            if !app.allow_reset {
                return Err("reset disabled on this network".into());
            }
            wipe_data();
            *app = Explorer::boot_persist();
        }
        "origin" => {
            let iso = q.get("iso3").ok_or("iso3 required")?;
            if iso.len() != 3 || !iso.chars().all(|c| c.is_ascii_alphabetic()) {
                return Err("iso3 required".into());
            }
            let code = iso.to_ascii_uppercase();
            let pulses = {
                let n = app.origins.entry(code.clone()).or_insert(0);
                *n = n.saturating_add(1);
                *n
            };
            save_origins(&app.origins);
            return Ok(format!(
                "{{\"ok\":true,\"iso3\":{},\"pulses\":{}}}",
                jstr(&code),
                pulses
            ));
        }
        "prepare" => {
            let from = parse_addr(q.get("from").ok_or("from address required")?)?;
            let to = parse_addr(q.get("to").ok_or("to address required")?)?;
            let amount = parse_u64(q, "amount", 0)?;
            let n = app.mesh.node(&node).ok_or("unknown node")?;
            let p = n
                .prepare_transfer(from, amount, to)
                .map_err(|e| e.to_string())?;
            return Ok(format!(
                "{{\"ok\":true,\"sighash\":{},\"value\":{},\"fee\":{},\"change\":{},\"outpoint\":{{\"tx\":{},\"index\":{}}}}}",
                jstr(&hex::encode(p.sighash)),
                p.value,
                p.fee,
                p.value.saturating_sub(amount.saturating_add(p.fee)),
                jstr(&p.outpoint.tx.to_string()),
                p.outpoint.index
            ));
        }
        "submit" => {
            let from = parse_addr(q.get("from").ok_or("from address required")?)?;
            let to = parse_addr(q.get("to").ok_or("to address required")?)?;
            let amount = parse_u64(q, "amount", 0)?;
            let sig = parse_sig(q.get("sig").ok_or("sig required")?)?;
            let id = app
                .mesh
                .submit_signed(&node, from, amount, to, sig)
                .map_err(|e| e.to_string())?;
            app.mesh.drain(8);
            return Ok(format!("{{\"ok\":true,\"tx\":{}}}", jstr(&id.to_string())));
        }
        "faucet" => {
            if !app.faucet {
                return Err("faucet disabled".into());
            }
            let to = parse_addr(q.get("to").ok_or("to address required")?)?;
            let amount = parse_u64(q, "amount", ATOM)?;
            let block = app
                .mesh
                .send_to(&node, 1, amount, to)
                .map_err(|e| e.to_string())?;
            app.mesh.drain(8);
            return Ok(format!(
                "{{\"ok\":true,\"block\":{}}}",
                jstr(&block.to_string())
            ));
        }
        other => return Err(format!("unknown action {other}")),
    }
    app.mesh.drain(8);
    Ok("{\"ok\":true}".into())
}

fn history_json(
    app: &Explorer,
    q: &std::collections::HashMap<String, String>,
) -> Result<String, String> {
    let addr = parse_addr(q.get("address").ok_or("address required")?)?;
    let node_name = q
        .get("node")
        .cloned()
        .unwrap_or_else(|| app.selected.clone());
    let n = app.mesh.node(&node_name).ok_or("unknown node")?;
    let ledger = n.ledger().map_err(|e| e.to_string())?;
    let order = ledger.dag().linearize();
    let mut by_id: HashMap<kovanica_state::TxId, kovanica_state::Transaction> = HashMap::new();
    let mut items = Vec::new();
    for id in &order {
        let Some(rec) = n.block_record(id) else {
            continue;
        };
        for tx in rec.txs {
            by_id.insert(tx.id(), tx);
        }
    }
    for id in &order {
        let Some(rec) = n.block_record(id) else {
            continue;
        };
        for tx in &rec.txs {
            let mut delta: i128 = 0;
            for o in tx.outputs() {
                if o.owner == addr {
                    delta += o.value as i128;
                }
            }
            for inp in tx.inputs() {
                if let Some(prev) = by_id.get(&inp.outpoint.tx) {
                    if let Some(o) = prev.outputs().get(inp.outpoint.index as usize) {
                        if o.owner == addr {
                            delta -= o.value as i128;
                        }
                    }
                }
            }
            if delta == 0 {
                continue;
            }
            let kind = if tx.is_coinbase() {
                "coinbase"
            } else if delta > 0 {
                "in"
            } else {
                "out"
            };
            items.push(format!(
                "{{\"block\":{},\"tx\":{},\"kind\":{},\"delta\":{}}}",
                jstr(&id.to_string()),
                jstr(&tx.id().to_string()),
                jstr(kind),
                delta
            ));
        }
    }
    Ok(format!(
        "{{\"address\":{},\"balance\":{},\"txs\":{}}}",
        jstr(&addr.to_hex()),
        n.balance(&addr).map_err(|e| e.to_string())?,
        jarr(items.into_iter())
    ))
}

fn utxos_json(
    app: &Explorer,
    q: &std::collections::HashMap<String, String>,
) -> Result<String, String> {
    let addr = parse_addr(q.get("address").ok_or("address required")?)?;
    let node = q
        .get("node")
        .cloned()
        .unwrap_or_else(|| app.selected.clone());
    let n = app.mesh.node(&node).ok_or("unknown node")?;
    let bal = n.balance(&addr).map_err(|e| e.to_string())?;
    let rows = n.utxos_of(&addr).map_err(|e| e.to_string())?;
    let items = rows.into_iter().map(|(op, value)| {
        format!(
            "{{\"tx\":{},\"index\":{},\"value\":{}}}",
            jstr(&op.tx.to_string()),
            op.index,
            value
        )
    });
    Ok(format!(
        "{{\"address\":{},\"balance\":{},\"utxos\":{}}}",
        jstr(&addr.to_hex()),
        bal,
        jarr(items)
    ))
}

fn parse_addr(s: &str) -> Result<Address, String> {
    let bytes = hex::decode(s.trim()).map_err(|_| "address is not hex".to_string())?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| "address must be 32 bytes".to_string())?;
    Ok(Address::from_bytes(arr))
}

fn parse_sig(s: &str) -> Result<[u8; 64], String> {
    let bytes = hex::decode(s.trim()).map_err(|_| "sig is not hex".to_string())?;
    bytes
        .try_into()
        .map_err(|_| "sig must be 64 bytes".to_string())
}

fn parse_u64(
    q: &std::collections::HashMap<String, String>,
    key: &str,
    default: u64,
) -> Result<u64, String> {
    match q.get(key) {
        None => Ok(default),
        Some(s) => s.parse().map_err(|_| format!("bad {key}")),
    }
}

fn split_query(target: &str) -> (&str, std::collections::HashMap<String, String>) {
    match target.split_once('?') {
        None => (target, std::collections::HashMap::new()),
        Some((path, q)) => {
            let mut map = std::collections::HashMap::new();
            for pair in q.split('&') {
                if let Some((k, v)) = pair.split_once('=') {
                    map.insert(
                        k.to_string(),
                        urlencoding_decode(v),
                    );
                }
            }
            (path, map)
        }
    }
}

fn urlencoding_decode(s: &str) -> String {
    // Query values here are digits / node names; keep it strict.
    s.replace('+', " ")
}

fn respond(stream: &mut TcpStream, code: u16, ctype: &str, body: &[u8]) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        _ => "Not Found",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn respond_download(
    stream: &mut TcpStream,
    ctype: &str,
    filename: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Disposition: attachment; filename=\"{filename}\"\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn snapshot(app: &Explorer) -> String {
    let selected = &app.selected;
    let node = app.mesh.node(selected).expect("selected exists");
    let mut nodes = Vec::new();
    for name in app.mesh.names() {
        let n = app.mesh.node(&name).expect("named");
        let tip = n.selected_tip().map(|t| t.to_string()).unwrap_or_default();
        nodes.push(format!(
            "{{\"name\":{},\"blocks\":{},\"tip\":{},\"peers\":{},\"mempool\":{}}}",
            jstr(&name),
            n.block_count().unwrap_or(0),
            jstr(&tip),
            jarr(app.mesh.peers_of(&name).iter().map(|s| jstr(s))),
            n.pending_count()
        ));
    }
    let events: Vec<String> = app
        .mesh
        .events()
        .iter()
        .rev()
        .take(48)
        .map(|e| {
            format!(
                "{{\"at\":{},\"from\":{},\"to\":{},\"kind\":{}}}",
                e.at,
                jstr(&e.from),
                jstr(&e.to),
                jstr(&format!("{:?}", e.kind).to_lowercase())
            )
        })
        .collect();
    format!(
        "{{\"selected\":{},\"mining\":{},\"faucet\":{},\"allow_reset\":{},\"operator\":{},\"network\":{},\"listen\":{},\"peers\":{},\"mesh\":{{\"now\":{},\"queued\":{},\"nodes\":{},\"events\":{}}},\"node\":{},\"wallets\":{}}}",
        jstr(selected),
        app.mining,
        app.faucet,
        app.allow_reset,
        app.operator,
        jstr(NETWORK),
        jstr(&app.listen_addr),
        jarr(app.peers.iter().map(|s| jstr(s))),
        app.mesh.now(),
        app.mesh.queued(),
        jarr(nodes.into_iter()),
        jarr(events.into_iter()),
        node_json(node),
        wallets_json(node),
    )
}

fn node_json(node: &Node) -> String {
    let Ok(ledger) = node.ledger() else {
        return "{\"blocks\":0,\"dag\":[],\"order\":[],\"tips\":[],\"pending\":[]}".into();
    };
    let dag = ledger.dag();
    let selected_tip = node.selected_tip().map(|t| t.to_string()).unwrap_or_default();
    let chain: Vec<BlockId> = dag.selected_chain();
    let chain_set: HashSet<BlockId> = chain.iter().copied().collect();
    let tip_id = node.selected_tip().ok();
    let gd = tip_id.as_ref().and_then(|id| dag.ghostdag(id));
    let blue_set: HashSet<BlockId> = gd
        .map(|g| g.blue_anticone_sizes.keys().copied().collect())
        .unwrap_or_default();
    let genesis = dag.genesis();
    let order = dag.linearize();
    let mut tx_count = 0usize;
    let dag_json: Vec<String> = order
        .iter()
        .filter_map(|id| {
            let rec = node.block_record(id)?;
            tx_count += rec.txs.len();
            let colour = colour_of(*id, genesis, &chain_set, &blue_set);
            let g = dag.ghostdag(id)?;
            Some(format!(
                "{{\"id\":{},\"parents\":{},\"selected_parent\":{},\"work\":{},\"timestamp_ms\":{},\"nonce\":{},\"blue_score\":{},\"colour\":{},\"txs\":{}}}",
                jstr(&id.to_string()),
                jarr(rec.parents.iter().map(|p| jstr(&p.to_string()))),
                match g.selected_parent {
                    Some(sp) => jstr(&sp.to_string()),
                    None => "null".into(),
                },
                rec.work,
                rec.timestamp_ms,
                rec.nonce,
                g.blue_score,
                jstr(colour),
                jarr(rec.txs.iter().map(tx_json)),
            ))
        })
        .collect();
    let pending = jarr(node.pending_txs().iter().map(|tx| pending_json(node, tx)));
    let utxo = ledger.ledger_state();
    format!(
        "{{\"blocks\":{},\"tips\":{},\"selected_tip\":{},\"blue_score\":{},\"blue_work\":{},\"k\":{},\"subsidy\":{},\"issuance\":{},\"halving_era\":{},\"min_fee\":{},\"genesis\":{},\"supply\":{},\"token\":{},\"decimals\":{},\"miner\":{},\"atom\":{},\"pow\":{},\"ui\":{},\"utxos\":{},\"chain_len\":{},\"mempool\":{},\"tx_count\":{},\"dag\":{},\"order\":{},\"pending\":{}}}",
        dag.len(),
        jarr(dag.tips().iter().map(|t| jstr(&t.to_string()))),
        jstr(&selected_tip),
        gd.map(|g| g.blue_score).unwrap_or(0),
        gd.map(|g| g.blue_work).unwrap_or(0),
        dag.k(),
        ledger.subsidy(),
        node.issuance().unwrap_or(0),
        HALVING_ERA,
        node.min_fee(),
        jstr(&ledger.genesis().to_string()),
        utxo.total_value(),
        jstr("KVNC"),
        8,
        match node.miner() {
            Some(m) => jstr(&m.to_hex()),
            None => "null".into(),
        },
        ATOM,
        node.proof_of_work(),
        jstr("v5"),
        utxo.len(),
        chain.len(),
        node.pending_count(),
        tx_count,
        jarr(dag_json.into_iter()),
        jarr(order.iter().map(|id| jstr(&id.to_string()))),
        pending,
    )
}

fn colour_of<'a>(
    id: BlockId,
    genesis: BlockId,
    chain: &HashSet<BlockId>,
    blue: &HashSet<BlockId>,
) -> &'a str {
    if id == genesis {
        "genesis"
    } else if chain.contains(&id) {
        "chain"
    } else if blue.contains(&id) {
        "blue"
    } else {
        "red"
    }
}

fn tx_json(tx: &Transaction) -> String {
    format!(
        "{{\"id\":{},\"coinbase\":{},\"inputs\":{},\"outputs\":{}}}",
        jstr(&tx.id().to_string()),
        tx.is_coinbase(),
        tx.inputs().len(),
        jarr(tx.outputs().iter().map(|o| format!(
            "{{\"value\":{},\"owner\":{}}}",
            o.value,
            jstr(&o.owner.to_hex())
        ))),
    )
}

fn pending_json(node: &Node, tx: &Transaction) -> String {
    let fee = if let Ok(ledger) = node.ledger() {
        let utxo = ledger.ledger_state();
        let mut sum_in = 0u64;
        for input in tx.inputs() {
            if let Some(prev) = utxo.get(&input.outpoint) {
                sum_in = sum_in.saturating_add(prev.value);
            }
        }
        let sum_out: u64 = tx.outputs().iter().map(|o| o.value).sum();
        sum_in.saturating_sub(sum_out)
    } else {
        0
    };
    format!(
        "{{\"id\":{},\"coinbase\":{},\"fee\":{},\"inputs\":{},\"outputs\":{}}}",
        jstr(&tx.id().to_string()),
        tx.is_coinbase(),
        fee,
        tx.inputs().len(),
        jarr(tx.outputs().iter().map(|o| format!(
            "{{\"value\":{},\"owner\":{}}}",
            o.value,
            jstr(&o.owner.to_hex())
        ))),
    )
}

fn wallets_json(node: &Node) -> String {
    let rows: Vec<String> = ACTORS
        .iter()
        .map(|seed| {
            let addr = Node::address(*seed);
            let bal = node.balance(&addr).unwrap_or(0);
            format!(
                "{{\"seed\":{},\"address\":{},\"balance\":{}}}",
                seed,
                jstr(&addr.to_hex()),
                bal
            )
        })
        .collect();
    jarr(rows.into_iter())
}

fn jstr(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn jarr(items: impl Iterator<Item = String>) -> String {
    let mut out = String::from("[");
    for (i, item) in items.enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&item);
    }
    out.push(']');
    out
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_has_three_nodes_and_genesis() {
        let app = Explorer::boot();
        let json = snapshot(&app);
        assert!(json.contains("\"alpha\""));
        assert!(json.contains("\"beta\""));
        assert!(json.contains("\"gamma\""));
        assert!(json.contains("\"genesis\""));
        assert!(json.contains("\"supply\":5000000000"));
        assert!(json.contains("\"token\":\"KVNC\""));
        assert!(json.contains("\"ui\":\"v5\""));
        assert!(json.contains("\"pow\":true"));
        assert!(json.contains("\"network\":\"kovanica-testnet-1\""));
        assert!(json.contains("\"subsidy\":5000000000"));
    }

    #[test]
    fn empty_block_mints_kvnc_subsidy_to_miner() {
        let mut app = Explorer::boot();
        app.mining = false;
        let founder = kovanica_state::KeyPair::from_u64(1).address();
        let before = app
            .mesh
            .node("alpha")
            .unwrap()
            .balance(&founder)
            .unwrap();
        app.mesh.produce_empty("alpha").unwrap();
        let after = app
            .mesh
            .node("alpha")
            .unwrap()
            .balance(&founder)
            .unwrap();
        assert_eq!(after, before + u128::from(GENESIS_SUBSIDY));
    }

    #[test]
    fn produce_block_mints_kvnc_subsidy_with_the_spend() {
        let mut app = Explorer::boot();
        app.mining = false;
        let founder = kovanica_state::KeyPair::from_u64(1).address();
        app.mesh.pool("alpha", 1, ATOM, 2).unwrap();
        app.mesh.produce("alpha").unwrap();
        let n = app.mesh.node("alpha").unwrap();
        assert_eq!(
            n.balance(&kovanica_state::KeyPair::from_u64(2).address())
                .unwrap(),
            ATOM.into()
        );
        assert_eq!(n.balance(&founder).unwrap(), u128::from(GENESIS_PREMINE - ATOM + GENESIS_SUBSIDY));
    }

    #[test]
    fn wallet_signs_off_node_and_mempool_accepts() {
        use kovanica_state::KeyPair;

        let mut app = Explorer::boot();
        app.mining = false;
        let from = KeyPair::from_u64(1);
        let to = KeyPair::from_u64(9);
        let prepared = app
            .mesh
            .node("alpha")
            .unwrap()
            .prepare_transfer(from.address(), ATOM, to.address())
            .unwrap();
        let sig = from.sign(&prepared.sighash);
        let id = app
            .mesh
            .submit_signed("alpha", from.address(), ATOM, to.address(), sig)
            .unwrap();
        assert_eq!(app.mesh.node("alpha").unwrap().pending_count(), 1);
        assert!(!id.to_string().is_empty());
        app.mesh.produce("alpha").unwrap();
        app.mesh.drain(8);
        assert_eq!(
            app.mesh
                .node("alpha")
                .unwrap()
                .balance(&to.address())
                .unwrap(),
            ATOM.into()
        );
    }

    #[test]
    fn history_lists_credit_to_an_address() {
        let mut app = Explorer::boot();
        app.mining = false;
        app.mesh.pool("alpha", 1, ATOM, 2).unwrap();
        app.mesh.produce("alpha").unwrap();
        let addr = kovanica_state::KeyPair::from_u64(2).address().to_hex();
        let mut q = std::collections::HashMap::new();
        q.insert("address".into(), addr);
        q.insert("node".into(), "alpha".into());
        let json = history_json(&app, &q).unwrap();
        assert!(json.contains("\"kind\":\"in\""));
        assert!(json.contains(&ATOM.to_string()));
    }

    #[test]
    fn issuance_halves_each_era() {
        assert_eq!(Node::issuance_at(50 * ATOM, 0), 50 * ATOM);
        assert_eq!(Node::issuance_at(50 * ATOM, 999), 50 * ATOM);
        assert_eq!(Node::issuance_at(50 * ATOM, 1000), 25 * ATOM);
        assert_eq!(Node::issuance_at(50 * ATOM, 2000), (50 * ATOM) >> 2);
    }

    #[test]
    fn min_fee_scales_with_subsidy_cap() {
        let app = Explorer::boot();
        let fee = app.mesh.node("alpha").unwrap().min_fee();
        assert_eq!(fee, (GENESIS_SUBSIDY / 500_000).max(1));
        assert!(fee > 1);
    }

    #[test]
    fn p2p_off_tokens() {
        assert!(env_off("off"));
        assert!(env_off("none"));
        assert!(env_off("0"));
        assert!(!env_off(P2P_LISTEN_DEFAULT));
        assert_eq!(P2P_BOOTSTRAP, "explorer.kovanica.online:9000");
    }

    #[test]
    fn origin_pulse_increments_and_lists() {
        let mut app = Explorer::boot();
        let mut q = std::collections::HashMap::new();
        q.insert("iso3".into(), "hrv".into());
        let body = dispatch(&mut app, "origin", &q).unwrap();
        assert!(body.contains("HRV"));
        assert!(body.contains("\"pulses\":1"));
        let listed = origins_json(&app.origins);
        assert!(listed.contains("HRV"));
        let bad = dispatch(&mut app, "origin", &std::collections::HashMap::new());
        assert!(bad.is_err());
    }
}
