# Expose Linear OAuth and webhooks

Linear must reach Relay over public HTTPS for OAuth installation and task
events. Host `axis serve` listens on `127.0.0.1:8787` by default. Compose
publishes only `127.0.0.1:8787` on the host (Relay binds `0.0.0.0` inside the
container). Run the forwarder on that host; another container's loopback is a
different network namespace.

Choose one option below and keep it running during `axis init` and normal
operation. Install its CLI first. All domains and credentials below are
fictional placeholders; replace them with values from your own deployment.
The wizard creates neither DNS records, tunnels nor TLS certificates.

## URLs to register in Linear

For public origin `https://axis.example.com`, enter these exact values in the
Linear OAuth application (Settings > API > OAuth applications):

| Field | Value |
| --- | --- |
| Callback URL | `https://axis.example.com/oauth/linear/callback` |
| Webhook URL | `https://axis.example.com/webhooks/linear` |

Enter the same origin, `https://axis.example.com`, at the wizard's public URL
prompt, without a path, query or fragment. These are exactly the paths printed
by `axis init`. The wizard also prints the capabilities, scopes and event
subscriptions; follow [interactive setup](SINGLE-HOST-DEPLOYMENT.md#interactive-single-host-setup).
Use the actual assigned HTTPS origin if your tunnel provider gives you a URL.
Changing a temporary tunnel address requires updating the application URLs and
`linear.redirectUri`/`relay.url` coherently; use a stable origin for persistent
installations. Managed installations require [configuration replacement](MULTI-HOST-DEPLOYMENT.md#host-local-install-and-upgrade).

## Cloudflare Tunnel

For a stable hostname, use a domain managed in your Cloudflare account. Log in,
create a named tunnel and route its DNS name:

```sh
cloudflared tunnel login
cloudflared tunnel create axis
cloudflared tunnel route dns axis axis.example.com
```

The create command prints a tunnel UUID and writes its credentials file. Keep
that file private. Write `cloudflared.yml` with the actual UUID and the absolute
path of that credentials file (the shown filename is a placeholder):

```yaml
tunnel: REPLACE_WITH_TUNNEL_ID
credentials-file: /etc/cloudflared/REPLACE_WITH_TUNNEL_ID.json
ingress:
  - hostname: axis.example.com
    path: ^/(oauth/linear/callback|webhooks/linear)$
    service: http://127.0.0.1:8787
  - service: http_status:404
```

```sh
cloudflared tunnel --config ./cloudflared.yml run axis
```

Give Linear the callback and webhook URLs in the table above. Cloudflare
terminates public TLS; the local hop stays on loopback. The tunnel requires no
inbound port forwarding. Keep its credentials outside the repository and do
not put an interactive Cloudflare Access login in front of the webhook: Linear
cannot complete that login. The path rule limits forwarding to the two required
routes; other requests get 404. If `cloudflared` logs QUIC connection failures,
the network blocks outbound UDP; add `--protocol http2` to `cloudflared tunnel`
to connect over TCP instead.

## ngrok

Configure the token for your ngrok account, then forward to Relay:

```sh
ngrok config add-authtoken REPLACE_WITH_NGROK_TOKEN
ngrok http http://127.0.0.1:8787
```

The CLI prints a public HTTPS forwarding URL. Use that actual origin both in
`axis init` and for Linear's callback/webhook URLs, appending exactly
`/oauth/linear/callback` and `/webhooks/linear`. For example, a configured custom
origin `https://axis.example.com` uses the table above; it is not automatically
assigned by the command. Keep ngrok running while the wizard's temporary Relay
performs OAuth, and while `serve` handles tasks. A temporary URL may change on
restart; a reserved domain avoids reconfiguration.

The auth command stores a local credential: protect the ngrok configuration
and avoid real tokens in shell history or shared transcripts. ngrok's request
inspection can retain webhook bodies and OAuth query values; restrict access
to that data. Forward only Relay, without browser-login middleware, and never
forward an internal listener.

## TLS reverse proxy with Caddy

Point `axis.example.com` DNS at the proxy host and allow public TCP ports 80
and 443 for certificate issuance and HTTPS. Caddy and Axis run on the same host.
Save this as `Caddyfile`:

```caddyfile
axis.example.com {
    @linear path /oauth/linear/callback /webhooks/linear
    handle @linear {
        reverse_proxy 127.0.0.1:8787
    }
    handle {
        respond 404
    }
}
```

```sh
caddy validate --config ./Caddyfile --adapter caddyfile
caddy run --config ./Caddyfile --adapter caddyfile
```

Caddy obtains/renews the TLS certificate and redirects HTTP to HTTPS for this
public hostname. Ensure its service account can bind the public ports and write
its certificate state. Give Linear exactly the two URLs in the table above.
The catch-all keeps health, operator and optional API routes off this public
proxy. With nginx or another TLS proxy, preserve these two paths and request
bodies unchanged and apply the same route restriction.

## Security and verification

- Register **HTTPS only**. Leave Relay bound/published to host loopback behind
  the forwarder. For Compose, only Relay binds all interfaces inside the
  container; its host publication stays on loopback.
- Relay verifies Linear's webhook signature using `LINEAR_WEBHOOK_SECRET`
  against the raw body and rejects stale deliveries. A tunnel or proxy does
  not replace signature verification. Do not rewrite the body or strip
  `Linear-Signature` or required request headers.
- Never publish Router (8788), Node (9080/9081) or Worker (9082) defaults. These
  are authenticated internal transports, not webhook endpoints. Do not expose
  the data directory, keys, TOML, env file or native executor home.
- Protect OAuth client secrets, signing secrets and tunnel credentials. Avoid
  proxy access logs or request dumps containing OAuth codes/query strings,
  webhook bodies, prompts or provider output. Keep diagnostic records private.
- Check local readiness with `curl --fail http://127.0.0.1:8787/health/ready`
  after starting Axis. The path-restricted public proxy intentionally returns
  404 for that health path. Complete OAuth through the wizard, then mention
  @Axis in the selected Team and check Linear delivery status and Activities.
  Merely receiving a response from the public host does not prove valid signed
  webhook delivery or executor readiness.
