# token_checker

A single-process loop: read wallet rows from a Grist document, ask api.etherscan.io for
the balance of a token (or of ETH) on one wallet, write the result back to Grist. One
wallet per iteration; a wallet is "due" when its `Value` cell is empty and it has an
address, so an operator re-arms a wallet by clearing that cell.

No inbound service — the only port it opens is the local `/health` endpoint, and the only
persistent thing it touches is the Grist document. The chain is chosen per request from
the document's `Chains` table, so one Etherscan V2 key covers every chain.

## Quick start

```bash
make install           # create .venv and install dev/test deps
make env               # cp .env.example .env
$EDITOR .env           # fill in the four required variables
make test              # run the suite
make run               # start the loop
```

`make help` lists every target.

## Configuration

Everything comes from the environment (or `.env` locally) through `src/settings.py`.
See `.env.example` for the annotated list.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `GRIST_SERVER` | yes | — | Grist server URL |
| `GRIST_DOC_ID` | yes | — | Grist document id holding the `Wallets` / `Settings` / `Chains` tables |
| `GRIST_API_KEY` | yes | — | Grist API key |
| `ETHERSCAN_API_KEY` | yes | — | Etherscan V2 API key, used for both ETH and token calls |
| `TELEGRAM_BOT_TOKEN` | no | — | Where the watchdog announces that it is killing the process |
| `TELEGRAM_CHAT_ID` | no | — | Addressee of that notice |
| `HEALTH_PORT` | no | `8080` | Port the `/health` server listens on |

A missing or invalid variable prints a message naming it and exits 1 — it does not start.
A value that is present but blank counts as missing: a stray space in the stack's
`environment:` block would otherwise behave exactly like an empty string while looking
configured.

The two Telegram variables are both-or-neither. With only one of them the watchdog logs
"Telegram notification skipped" and dies quietly, which is a supported configuration — the
container restart and the log line are the primary signal either way.

`HEALTH_PORT`'s default is a production contract: the `crypt-common` stack does not pass
the variable, and the image's `HEALTHCHECK` curls the same default. Changing it here
without changing the Dockerfile leaves docker probing a port nothing listens on.

## Liveness

The service answers `GET /health` on `HEALTH_PORT`, and that endpoint is what the image's
`HEALTHCHECK` curls. A watchdog acts as a dead-man switch: when a loop step hangs it kills
the process so the restart policy can replace it, and the endpoint reports 503 before that
happens rather than after, so a restart driven by docker's probe comes from a service that
already knows it is stuck.

In production the container carries `io.portainer.autoheal.enable`, so an unhealthy
container gets restarted, and `io.portainer.update.enable`, so a new `:latest` is rolled
out automatically.

## Deployment

CI (Gitea Actions) runs the tests, builds the image, smoke-tests it inside a container and
pushes `gitea.vvzvlad.xyz/projects/token_checker:<sha>` followed by `:latest`.
Production is the `tokenchecker` service of the `crypt-common` stack on nebula;
`docker-compose.yml` in this repo is the standalone reference for that service.
