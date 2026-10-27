# Deployment guides

Choose the deployment shape before provisioning configuration or credentials.

| Guide | Use it for |
| --- | --- |
| [Single-host quick deployment](SINGLE-HOST-DEPLOYMENT.md) | Interactive `axis init`, `axis serve`, Docker/Compose, Linux systemd, executor profiles, upgrades, backups, logs and troubleshooting. |
| [Multi-host advanced deployment](MULTI-HOST-DEPLOYMENT.md) | Separate Relay/Router, Node and Worker processes, routing, preflight, macOS managed host lifecycle and coordinated recovery. |
| [Public webhook exposure](WEBHOOK-EXPOSURE.md) | Cloudflare Tunnel, ngrok or a TLS reverse proxy in front of Relay for Linear OAuth and webhooks. |
| [Configuration reference](CONFIGURATION.md) | TOML schema, initialization flags, path resolution, secret ownership and embedding APIs. |

For a first working agent, start with the [README quick start](../README.md#quick-start-about-10-minutes).
Use one active owner per journal and one active Relay per Relay database.
