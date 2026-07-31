# sock5gw

Ubuntu single-NIC gateway that assigns one idle SOCKS5 proxy to each client IP.
Clients point their default gateway and DNS at the Ubuntu host. The service
captures TCP/DNS, maps domains through FakeIP, and forwards new TCP connections
through the SOCKS5 proxy currently leased to the client.

## Behavior

- A SOCKS5 proxy is exclusive to one client while active.
- `POST /v1/lease/refresh` switches only new connections to a new idle proxy.
- The old proxy stays `draining` until old connections close.
- If no idle proxy exists, the request returns `pending`; refresh keeps the old
  proxy usable.
- Lease expiry immediately closes that client's connections and releases the
  proxy.
- Client identity is the source IP.

## Build

```sh
go mod download
go build ./cmd/sock5gw
```

Linux is required for transparent original-destination lookup.

## Front Proxy (P0)

The optional front proxy adds one SOCKS5 hop before every leased proxy on the
`proxy` TCP path:

```text
sock5gw -> local SOCKS5 sidecar -> front transport -> leased SOCKS5 -> target
```

The recommended front transport is a maintained Shadowsocks implementation
such as `shadowsocks-rust`. Run `sslocal` as a separate process listening on
`127.0.0.1:11080`; sock5gw intentionally does not implement the Shadowsocks
protocol or cryptography. This keeps transport updates and failures isolated
from lease management.

```json
"front_proxy": {
  "enabled": true,
  "protocol": "socks5",
  "address": "127.0.0.1:11080",
  "username": "",
  "password": "",
  "fail_open": false
}
```

The front proxy is fail closed. `fail_open: true` is rejected during config
loading, and sock5gw never bypasses an unavailable sidecar. The management API
remains available while proxied connections fail. `direct` routes remain
direct. P0 covers TCP only; generic UDP/QUIC and the DNS upstream are outside
this front-proxy path.

See the [Chinese implementation plan](docs/front-proxy-p0-plan.md) and the
[sslocal deployment guide](deployments/README.front-proxy.md).

## API

Use bearer tokens from `config.json`.
Responses use these statuses:

- `active`: client has a usable SOCKS5 assignment.
- `pending`: client is waiting for an idle SOCKS5; refresh keeps the old proxy
  in the response while new connections continue to use it.
- `blocked`: client has no lease and new TCP connections are rejected.

```sh
curl -H 'Authorization: Bearer change-client-token' \
  -X POST http://GATEWAY_IP:8080/v1/lease

curl -H 'Authorization: Bearer change-client-token' \
  -X POST http://GATEWAY_IP:8080/v1/lease/refresh

curl -H 'Authorization: Bearer change-client-token' \
  http://GATEWAY_IP:8080/v1/lease

curl -H 'Authorization: Bearer change-admin-token' \
  http://GATEWAY_IP:8080/v1/admin/status
```

## Web Admin

Open:

```text
http://GATEWAY_IP:8080/admin/
```

The page shows:

- online clients, lease status, assigned proxy, active connection count, and
  current exit IP;
- proxy pool status, health details, assigned client, active connections, and
  detected exit IP;
- front SOCKS5 URL, credential presence, health status, and runtime enable or
  disable controls;
- add/delete proxies;
- enable/disable proxies without restarting the service.

The embedded page uses `admin_key` from the config as its bearer token. In a real
deployment, expose the API only on a trusted management network or behind an
authenticating reverse proxy.

Admin proxy API:

```sh
curl -H 'Authorization: Bearer change-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"id":"proxy-002","address":"1.2.3.4:1080","username":"","password":""}' \
  http://GATEWAY_IP:8080/v1/admin/proxies

curl -H 'Authorization: Bearer change-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"disabled":true}' \
  -X POST http://GATEWAY_IP:8080/v1/admin/proxies/proxy-002/disabled
```

Front proxy updates are persisted to the JSON config and apply to new
connections immediately. Existing connections are not interrupted. The API
never returns the saved username or password; omitting user info from a new URL
retains saved credentials unless `clear_credentials` is true.

```sh
curl -H 'Authorization: Bearer change-admin-token' \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"url":"socks5://user:password@127.0.0.1:11080"}' \
  -X PUT http://GATEWAY_IP:8080/v1/admin/front-proxy
```

## Ubuntu Deployment Sketch

1. Install the binary at `/usr/local/bin/sock5gw`.
2. Copy `config.example.json` to `/etc/sock5gw/config.json` and replace tokens,
   proxy pool, and LAN settings.
3. Create `/var/lib/sock5gw`.
4. Install `deployments/sock5gw.service`.
5. Adapt `deployments/nftables.example.nft` to the client interface, LAN CIDR,
   gateway address, and configured ports.
6. Install `deployments/sock5gw-nftables.service` and
   `deployments/sock5gw.service.d/nftables.conf`. The strong systemd dependency
   prevents sock5gw from starting when the nft rules fail to load.
7. Set clients' default gateway and DNS server to the Ubuntu host IP.
8. Disable or block IPv6 in the client LAN for v1.

See `deployments/README.nftables.md` for installation, failure-injection,
verification, and rollback commands.

When enabling the front proxy, install the `sslocal` sidecar and its sock5gw
systemd drop-in before restarting sock5gw. The deployment guide includes the
required `0600` secret-file permissions.

Transparent DNS/TCP redirects do not require kernel IPv4 forwarding. On a
dedicated gateway, keep it disabled so a missing or flushed nft table cannot
turn the host into a direct-routing bypass:

```sh
sysctl -w net.ipv4.ip_forward=0
```

Only enable forwarding when the host intentionally routes excluded private
networks, and install an independent host `FORWARD` baseline before connecting
clients. See the nftables deployment guide for the first-install and ruleset
loss boundary. Persist the chosen setting in `/etc/sysctl.d/99-sock5gw.conf`.

## Notes

- The nftables example uses NAT `redirect`, which is enough for IPv4
  `SO_ORIGINAL_DST` lookup. Test in a maintenance window before routing a real
  LAN through it.
- The API should be reachable only from trusted client/admin networks.
- The v1 service blocks AAAA DNS responses and assumes IPv6 is disabled or
  filtered on the LAN.
- Browser DoH/DoQ can bypass normal DNS interception and should be disabled or
  blocked at the network edge.
