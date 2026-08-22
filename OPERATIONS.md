# Seed operations runbook (`kovanica-testnet`)

The public seed runs on VPS `srv1745734` as the pm2 process
**kovanica-explorer**, serving:

| What | Where |
| --- | --- |
| Explorer HTTP | `127.0.0.1:8080` → Caddy → `explorer.kovanica.online` (Cloudflare-proxied) |
| P2P (TCP) | `0.0.0.0:9000` + `[::]:9000` (v6only) → `seed.kovanica.online` (grey-cloud DNS) |
| Data dir | `/root/kovanica-ledger/data` (`KOVANICA_DATA`) |
| Source | `/root/kovanica-ledger` — a plain clone of `KovanicaDAG/kovanica-ledger@main` |

## Process env (as started; `pm2 describe kovanica-explorer` to view)

```
KOVANICA_LISTEN=0.0.0.0:9000   KOVANICA_PEERS=off     KOVANICA_MINE=0
KOVANICA_FAUCET=0              KOVANICA_TAP=1         KOVANICA_ALLOW_RESET=0
KOVANICA_OPERATOR=0            KOVANICA_POW=1         KOVANICA_DATA=/root/kovanica-ledger/data
```

Do **not** use `ecosystem.config.js` from the repo on this box — it is a
template with an unrelated cwd.

## Post-deploy checks (after every deploy)

```sh
curl -s localhost:8080/api/head      # network "kovanica-testnet", blocks>0, tip advancing
ss -tlnp | grep 9000                 # TWO lines: 0.0.0.0:9000 and [::]:9000
pm2 ls                               # kovanica-explorer online, restart count stable
```

From outside:

```sh
nc -vz seed.kovanica.online 9000     # must succeed for v4 AND v6
curl -s https://explorer.kovanica.online/api/head
```

Peer exchange sanity: a fresh clone with `KOVANICA_PEERS=seed.kovanica.online:9000`
should pull records within seconds of boot (see its stderr log).

## Restart drill

```sh
pm2 restart kovanica-explorer        # seconds of downtime, chain resumes from data/
curl -s localhost:8080/api/head      # same tip as before restart
```

A restart replays the append-only store; block count and tip must match
pre-restart values. If they do not, stop and check `data/` before any
further action.

## Backup / restore

Everything authoritative lives in `data/` (snapshots + replay log +
taps/origins). The DAG itself is consensus state — losing it means
re-genesis, not recoverable from anywhere else.

```sh
# backup (while running is fine: files are written atomically per save)
tar -C /root/kovanica-ledger -czf /root/backups/kovanica-data-$(date +%F).tar.gz data/

# restore
pm2 stop kovanica-explorer
rm -rf /root/kovanica-ledger/data
tar -C /root/kovanica-ledger -xzf /root/backups/<file>.tar.gz
pm2 start kovanica-explorer && curl -s localhost:8080/api/head
```

Keep at least the last 7 daily backups; test a restore quarterly.

## Logs

```sh
pm2 logs kovanica-explorer --lines 200       # live
ls /root/.pm2/logs/                          # rotated files (pm2-logrotate)
```

What to look for after a deploy: `p2p exchanged with <peer>` lines,
no repeated `listen ... failed`, no `exchange ...: decode/apply error` storms.

## Network marker & re-genesis

`data/network` stores the network id. A node whose marker differs from its
build's `NETWORK` const wipes `data/` and starts a new genesis on first boot.
That is how the `kovanica-testnet-1 → kovanica-testnet` rename was rolled out.
Never edit the marker by hand.

## Known non-goals

- The seed deliberately runs `KOVANICA_MINE=0` and `KOVANICA_PEERS=off`:
  it serves state and accepts exchanges; it does not mine or dial out.
- TAP rate limit (40/day/address) persists in `data/taps.txt`; wiping it
  resets faucet limits only.
