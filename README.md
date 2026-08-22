# Kovanica Ledger

**Default branch: `vps-live`.** This is the Rust node running on the VPS (`kovanica-testnet`).

| Host | Process |
| --- | --- |
| `explorer.kovanica.online` | `kovanica-node explorer 127.0.0.1:8080` (this repo) |
| `kovanica.online` / `wallet` / `map` | [kovanica-web](https://github.com/KovanicaDAG/kovanica-web) on `:3010` |

```sh
cargo run -p kovanica-node -- explorer 127.0.0.1:8080
```

Env on the public node: `KOVANICA_POW=1` `KOVANICA_MINE=0` `KOVANICA_FAUCET=0` `KOVANICA_ALLOW_RESET=0` `KOVANICA_LISTEN=0.0.0.0:9000` `KOVANICA_PEERS=off`.

TCP **:9000** is the only P2P path (libp2p/30333 removed). Clones: `KOVANICA_PEERS=explorer.kovanica.online:9000`. Cloneable tree: [kovanica-node](https://github.com/KovanicaDAG/kovanica-node).

Do **not** rebuild from `claude/claude-md-docs-*` — that line has no HTTP `explorer` mode.
UI is TypeScript only: [kovanica-web](https://github.com/KovanicaDAG/kovanica-web). GHOSTDAG / UTXO / Ed25519 stay here.

See [`TESTNET.md`](./TESTNET.md) for `kovanica-testnet`.

---

A **DAG-based distributed ledger**: a BlockDAG where blocks reference multiple
parents so they can be produced in parallel and merged, rather than forming a
single linear chain. Consensus follows **GHOSTDAG** (the PHANTOM/GHOSTDAG
protocol behind Kaspa).

> Early stage. The block DAG + GHOSTDAG consensus core, a UTXO ledger applied in
> GHOSTDAG order (with per-block state, snapshot persistence, and an incremental
> append-only log), and a runnable node binary with a mempool, multi-node gossip,
> an in-process overlay (`p2p::Mesh`) and a long-lived TCP relay
> (`relay::RelaySession`) are implemented and tested.

## What's here

`crates/kovanica-dag` — the consensus core:

- **Block DAG** with multi-parent blocks and BLAKE3 block ids.
- **GHOSTDAG**: selected parent, mergeset, and the k-cluster blue/red colouring.
- **Linearization**: a deterministic total order over the whole DAG.
- **Proof-of-work** (`pow` + `Dag::set_proof_of_work`): Nakamoto-style hash-target. Opt-in in the library; **on** for the public explorer (`KOVANICA_POW=1`).
- **Difficulty** (`difficulty::Retarget` + `Dag::set_difficulty`).
- **Reachability oracle** (`reachability::Reachability`).

`crates/kovanica-state` — the UTXO ledger (Ed25519 spends, GHOSTDAG-ordered apply, snapshots, finality).

`crates/kovanica-node` — line RPC (`serve` / `demo`) **and** `explorer [addr]` (JSON API + HTML from this process). Mempool, TCP gossip, `relay::RelaySession`, `LedgerStore`.

## Run the node

```sh
cargo run -p kovanica-node -- explorer 127.0.0.1:8080
cargo run -p kovanica-node -- demo
cargo run -p kovanica-node           # REPL
```

```text
> genesis 3 1000 500 1
> send 1 200 2
> pool 2 50 3
> produce
> balance 3
> save ledger.snap
```

## Build & test

```sh
cargo build --release -p kovanica-node
cargo test
```

## License

Dual-licensed under MIT or Apache-2.0.
