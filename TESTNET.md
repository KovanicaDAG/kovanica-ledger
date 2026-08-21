# kovanica-testnet-1

Public BlockDAG testnet. Native token **KVNC** (8 decimals).

| | |
| --- | --- |
| Explorer | https://explorer.kovanica.online |
| Wallet | https://wallet.kovanica.online |
| Node source | https://github.com/KovanicaDAG/kovanica-node |
| Network | `kovanica-testnet-1` |
| Premine | 50 KVNC (founder) |
| Subsidy cap | 50 KVNC / block, halves every 1000 blocks |
| Min fee | 0.0001 KVNC at genesis |
| k | 3 (GHOSTDAG) |
| PoW | on (`KOVANICA_POW=1`) |
| P2P | **TCP only** `KOVANICA_LISTEN` (default `0.0.0.0:9000`) |
| Bootstrap | `KOVANICA_PEERS=explorer.kovanica.online:9000` |

Live genesis and tip: `GET https://explorer.kovanica.online/api/head`  
P2P status on a running node: `GET /api/p2p`  
Block dump (same bytes a clone pulls over TCP): `GET /api/blocks`  
Bootstrap blob: `GET https://explorer.kovanica.online/api/bootstrap`

There is no second network path. libp2p / 30333 was removed: it bound a port
and never gossiped blocks.

## Tokenomics

- 1 KVNC = 10^8 atoms.
- New coins only from coinbase (issuance + fees to the miner).
- Faucet and empty-block minting are **off** on the public explorer.
- Wallet `prepare` / `submit` stays open: you sign in the browser; the node never sees the seed.

## Run

See [README.md](./README.md).
