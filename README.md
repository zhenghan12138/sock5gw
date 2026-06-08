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

## Ubuntu Deployment Sketch

1. Install the binary at `/usr/local/bin/sock5gw`.
2. Copy `config.example.json` to `/etc/sock5gw/config.json` and replace tokens,
   proxy pool, and LAN settings.
3. Create `/var/lib/sock5gw`.
4. Install `deployments/sock5gw.service`.
5. Adapt `deployments/nftables.example.nft` to the client LAN CIDR and load it.
6. Set clients' default gateway and DNS server to the Ubuntu host IP.
7. Disable or block IPv6 in the client LAN for v1.

Enable IPv4 forwarding:

```sh
sysctl -w net.ipv4.ip_forward=1
```

Persist it in `/etc/sysctl.d/99-sock5gw.conf`.

## Notes

- The nftables example uses NAT `redirect`, which is enough for IPv4
  `SO_ORIGINAL_DST` lookup. Test in a maintenance window before routing a real
  LAN through it.
- The API should be reachable only from trusted client/admin networks.
- The v1 service blocks AAAA DNS responses and assumes IPv6 is disabled or
  filtered on the LAN.
- Browser DoH/DoQ can bypass normal DNS interception and should be disabled or
  blocked at the network edge.
