# sock5gw API 接口文档

本文档基于 `internal/manager/api.go` 中注册的实际路由。默认示例地址为
`http://GATEWAY_IP:8080`，实际监听地址由 `config.json` 的 `api.listen` 决定。

## 通用约定

除管理页面外，所有接口均通过 Bearer Token 鉴权：

```http
Authorization: Bearer <token>
```

客户端租约接口使用 `api.client_key`，`/v1/admin/*` 接口使用
`api.admin_key`。JSON 请求应设置 `Content-Type: application/json`。成功的 JSON
响应使用 `application/json`；删除成功返回 `204 No Content`；错误响应为纯文本。
时间字段采用 RFC 3339 格式。

客户端身份默认取连接源 IP。仅当 `api.trust_proxy_headers=true` 时，才依次信任
`X-Forwarded-For` 的第一个地址和 `X-Real-IP`。只应在可信反向代理之后启用该项。

## 接口总览

| 方法 | 路径 | 鉴权 | 说明 |
| --- | --- | --- | --- |
| GET | `/admin/` | 无 | 返回内置管理页面 |
| POST | `/v1/lease` | Client | 申请或获取当前租约 |
| POST | `/v1/lease/refresh` | Client | 为新连接刷新出口代理 |
| GET | `/v1/lease` | Client | 查询当前租约 |
| GET | `/v1/admin/status` | Admin | 查询网关完整运行状态 |
| GET | `/v1/admin/routing` | Admin | 查询路由配置 |
| PUT | `/v1/admin/routing` | Admin | 更新并持久化路由配置 |
| GET | `/v1/admin/front-proxy` | Admin | 查询前置代理配置与状态 |
| PUT | `/v1/admin/front-proxy` | Admin | 更新并持久化前置代理 |
| POST | `/v1/admin/front-proxy/test` | Admin | 测试前置 SOCKS5 公网连通性 |
| GET | `/v1/admin/proxy-api` | Admin | 查询动态代理 API 配置 |
| PUT | `/v1/admin/proxy-api` | Admin | 更新并持久化动态代理 API 配置 |
| POST | `/v1/admin/proxy-api/test` | Admin | 申请并测试一个供应商代理 |
| POST | `/v1/admin/ip-geo` | Admin | 批量查询 IP 国家代码 |
| POST | `/v1/admin/leases` | Admin | 为指定客户端申请租约 |
| DELETE | `/v1/admin/leases/{ip}` | Admin | 释放指定客户端租约 |
| POST | `/v1/admin/leases/{ip}/refresh` | Admin | 刷新指定客户端出口 |
| POST | `/v1/admin/proxies` | Admin | 新增出口代理 |
| PUT | `/v1/admin/proxies/{id}` | Admin | 更新出口代理 |
| POST | `/v1/admin/proxies/{id}/disabled` | Admin | 启用或停用出口代理 |
| DELETE | `/v1/admin/proxies/{id}` | Admin | 删除空闲出口代理 |
| POST | `/v1/admin/proxies/import` | Admin | 从文本或 JSON 批量导入 |
| POST | `/v1/admin/proxies/import-url` | Admin | 从订阅 URL 导入 |
| POST | `/v1/admin/proxies/batch/disabled` | Admin | 批量启用或停用 |
| POST | `/v1/admin/proxies/batch/delete` | Admin | 批量删除 |
| POST | `/v1/admin/proxies/clear` | Admin | 清理所有未占用代理 |

## 客户端租约接口

### 申请租约

```http
POST /v1/lease
Authorization: Bearer <client_key>
```

无请求体时，服务按调用方 IP 从本地代理池分配代理。也可携带动态申请参数：

```json
{"country": "US", "duration_minutes": 10}
```

`country` 接受两位国家代码或 `Rand`，`duration_minutes` 为正整数分钟。两个字段
必须同时提供。参数与现有动态租约相同时直接复用；参数不同时申请新代理并自动
替换，旧连接继续排空。

### 刷新租约

```http
POST /v1/lease/refresh
Authorization: Bearer <client_key>
```

刷新只影响新连接；旧代理会保持 `draining`，直到原连接关闭。当前为动态租约且
无请求体时，沿用原国家和时长重新申请；也可提交新的国家和时长。静态租约无
请求体时仍从本地代理池刷新。动态申请失败会保留旧租约。

### 查询租约

```http
GET /v1/lease
Authorization: Bearer <client_key>
```

三个租约接口均返回 Assignment：

```json
{
  "client_ip": "192.168.2.10",
  "proxy_id": "proxy-001",
  "status": "active",
  "mode": "api",
  "country": "US",
  "duration_minutes": 10,
  "expires_at": "2026-08-04T12:00:00Z"
}
```

`status` 取值：

- `active`：存在可用租约。
- `pending`：等待新代理；刷新时可能同时返回旧 `proxy_id`。
- `blocked`：当前没有租约，仅查询接口会返回该状态。

`proxy_id` 和 `expires_at` 在无对应租约时省略。

动态申请失败使用结构化 JSON：

```json
{
  "code": "proxy_api_request_failed",
  "message": "proxy API request failed",
  "current_lease": {
    "client_ip": "192.168.2.10",
    "proxy_id": "api-0123456789abcdef",
    "status": "active",
    "mode": "api"
  }
}
```

参数错误返回 `400`，API 模式未启用返回 `503`，供应商响应或代理连通性错误返回
`502`。`current_lease` 仅在旧租约仍有效时出现。

## 管理状态接口

### 查询完整状态

```http
GET /v1/admin/status
Authorization: Bearer <admin_key>
```

响应结构：

```json
{
  "proxies": [],
  "clients": [],
  "leases": [],
  "pending_new": [],
  "pending_refresh": [],
  "front_proxy": {
    "enabled": false,
    "status": "disabled"
  }
}
```

`proxies` 元素包含 `id`、`address`、`status`、`client_ip`、`draining_for`、
`active_connections`、`failure_count`、`success_count`、`last_health_detail`、
`exit_ip`、`disabled`、`source`、`country`、`duration_minutes` 和
`provider_expires_at`。用户名和密码不会返回。`source` 为 `pool` 或 `api`。
代理状态可能为 `idle`、
`checking`、`active`、`draining`、`unhealthy` 或 `disabled`。

`clients` 元素包含 `client_ip`、`status`、`proxy_id`、`proxy_address`、
`exit_ip`、`active_connections`、`expires_at` 和 `queued`。`leases` 返回内部租约
视图；两个 `pending_*` 字段分别列出新申请和刷新等待队列中的客户端 IP。

## 路由配置接口

### 查询路由配置

```http
GET /v1/admin/routing
Authorization: Bearer <admin_key>
```

### 更新路由配置

```http
PUT /v1/admin/routing
Authorization: Bearer <admin_key>
Content-Type: application/json

{
  "enabled": true,
  "geosite_path": "/etc/sock5gw/geosite.dat",
  "default_action": "proxy",
  "direct_domains": ["internal.example"],
  "proxy_domains": [],
  "block_domains": [],
  "rules": [
    {"type": "geosite", "value": "geosite:cn", "action": "direct"},
    {"type": "domain_suffix", "value": "example.com", "action": "proxy"}
  ]
}
```

成功时返回完整的新配置，并原子写入当前 JSON 配置文件，随后立即应用于新流量。
常用 `action` 为 `direct`、`proxy`、`block`；规则类型支持 `domain_suffix`、
`domain_exact`、`keyword`、`regex` 和 `geosite`。无效规则返回 `400`，不会提交
新配置。

## 前置代理接口

### 查询前置代理

```http
GET /v1/admin/front-proxy
Authorization: Bearer <admin_key>
```

```json
{
  "enabled": true,
  "url": "socks5://127.0.0.1:11080",
  "credentials_configured": true,
  "status": {
    "enabled": true,
    "protocol": "socks5",
    "address": "127.0.0.1:11080",
    "status": "healthy",
    "last_checked_at": "2026-08-03T12:00:00Z"
  }
}
```

响应不会包含已保存的用户名或密码。状态还可能包含 `last_error` 和
`circuit_open_until`；`status` 常见值为 `disabled`、`unknown`、`healthy`、
`unhealthy`、`half_open`。

### 更新前置代理

```http
PUT /v1/admin/front-proxy
Authorization: Bearer <admin_key>
Content-Type: application/json

{
  "enabled": true,
  "url": "socks5://user:password@127.0.0.1:11080",
  "clear_credentials": false
}
```

仅支持 `socks5://`，且 URL 必须包含主机和端口，不能包含路径、查询参数或片段。
省略 URL 中的用户信息会保留已有凭据；设置 `clear_credentials=true` 可清除凭据。
配置成功后会持久化，并立即用于新连接；现有连接不会中断。响应格式与查询接口
相同。

### 测试前置代理

```http
POST /v1/admin/front-proxy/test
Authorization: Bearer <admin_key>
```

无需请求体。接口仅测试“前置 SOCKS5 -> 公网健康检查目标”的单跳链路，不读取或
测试普通代理池及动态出口代理。

```json
{
  "ok": true,
  "code": "healthy",
  "status": {
    "enabled": true,
    "status": "healthy"
  }
}
```

`code` 可能为 `healthy`、`disabled`、`canceled` 或 `unhealthy`。

## 动态代理 API 配置

### 查询配置

```http
GET /v1/admin/proxy-api
Authorization: Bearer <admin_key>
```

### 更新配置

```http
PUT /v1/admin/proxy-api
Authorization: Bearer <admin_key>
Content-Type: application/json

{
  "enabled": true,
  "url": "https://white.1024proxy.com/white/api?region=Rand&num=1&time=10&format=1&type=json",
  "country_param": "region",
  "duration_param": "time"
}
```

URL 必须使用公网 HTTPS、不得包含 userinfo 或片段。调用时会保留 URL 中的固定
参数，并以客户端值覆盖 `country_param` 和 `duration_param` 对应的查询参数。
首版只配置一个供应商，完整 URL 应固定申请一个代理，例如保留 `num=1`。
启用 `front_proxy` 时，供应商 HTTPS 请求也会通过该 SOCKS5 前置代理发出；供应商
白名单应添加前置代理的公网出口 IP。未启用时则添加网关主机的公网出口 IP。

供应商响应可以是单个对象、顶层数组、`data` 数组，或单行文本。对象字段支持
`host`/`ip`、`port`、可选的 `username`、`password`；文本支持
`host:port[:username:password]`、`socks5://...` 和 `socks://...`。最终必须只解析
出一个端点。供应商返回纯文本错误（例如白名单拒绝）时，接口会将其作为失败原因
返回，而不会误报为零个端点。

### 测试申请

```http
POST /v1/admin/proxy-api/test
Authorization: Bearer <admin_key>
Content-Type: application/json

{"country": "Rand", "duration_minutes": 10}
```

该接口会实际向供应商申请一个代理并完成 SOCKS5 健康检查，因此会消耗一次额度；
测试结果不会写入代理池或数据库。

```json
{
  "ok": true,
  "code": "healthy",
  "address": "198.51.100.10:1080",
  "exit_ip": "203.0.113.10",
  "elapsed_ms": 523
}
```

## IP 地理信息接口

```http
POST /v1/admin/ip-geo
Authorization: Bearer <admin_key>
Content-Type: application/json

{"ips": ["8.8.8.8", "1.1.1.1"]}
```

响应是以 IP 为键的对象：

```json
{
  "8.8.8.8": {"country_code": "US"},
  "1.1.1.1": {"country_code": "AU"}
}
```

结果依赖 `api.geoip_db_path` 指向的 GeoIP 数据库。无效 IP、查不到国家或数据库
不可用时，对应键会被省略。

## 管理员租约接口

### 为指定客户端申请租约

```http
POST /v1/admin/leases
Authorization: Bearer <admin_key>
Content-Type: application/json

{"client_ip": "192.168.2.10"}
```

管理员也可同时提供 `country` 和 `duration_minutes`，为指定 IPv4 客户端申请动态
租约。

### 刷新指定客户端租约

```http
POST /v1/admin/leases/192.168.2.10/refresh
Authorization: Bearer <admin_key>
```

上述两个接口只接受有效 IPv4 地址，成功响应均为 Assignment。

### 释放指定客户端租约

```http
DELETE /v1/admin/leases/192.168.2.10
Authorization: Bearer <admin_key>
```

成功返回 `204 No Content`，并关闭该客户端现有连接、释放其代理。无效 IP 返回
`400`。

## 出口代理接口

代理写入对象格式如下：

```json
{
  "id": "proxy-002",
  "address": "203.0.113.10:1080",
  "username": "user",
  "password": "password",
  "disabled": false
}
```

`address` 必须为 `host:port`，且不能与前置代理地址相同。响应中的 Proxy 对象
不会回显 `username` 和 `password`。

### 新增代理

```http
POST /v1/admin/proxies
Authorization: Bearer <admin_key>
Content-Type: application/json
```

请求体使用完整代理写入对象。`id` 必填且不能重复，成功返回 Proxy 对象。

### 更新代理

```http
PUT /v1/admin/proxies/proxy-002
Authorization: Bearer <admin_key>
Content-Type: application/json
```

路径中的 `{id}` 为目标代理 ID；请求体中的 `id` 不会用于改名。请求体应提供新的
`address`、`username`、`password` 和 `disabled`，成功返回更新后的 Proxy。

### 启用或停用代理

```http
POST /v1/admin/proxies/proxy-002/disabled
Authorization: Bearer <admin_key>
Content-Type: application/json

{"disabled": true}
```

成功返回更新后的 Proxy。正在使用的代理被停用后不会分配给新租约，但现有连接
不会立即中断。

### 删除代理

```http
DELETE /v1/admin/proxies/proxy-002
Authorization: Bearer <admin_key>
```

成功返回 `204 No Content`。处于 `active`、`checking`、`draining` 或仍有活动连接
的代理不能删除。

### 批量导入代理

纯文本方式：

```http
POST /v1/admin/proxies/import
Authorization: Bearer <admin_key>
Content-Type: text/plain

203.0.113.10:1080:user:password
socks5://user:password@203.0.113.11:1080
```

也可使用 JSON：

```json
{"text": "203.0.113.10:1080:user:password\n"}
```

空行和以 `#` 开头的行会被忽略。支持 `host:port[:username:password]`、
`socks5://...` 和 `socks://...`。请求体上限为 16 MiB。未显式提供 ID 时，服务
根据地址和凭据生成稳定 ID。

响应：

```json
{"imported": 1, "skipped": 1, "errors": ["line 2: invalid port"]}
```

### 从 URL 导入代理

```http
POST /v1/admin/proxies/import-url
Authorization: Bearer <admin_key>
Content-Type: application/json

{"url": "https://provider.example/subscription"}
```

订阅响应可为上述文本格式，也可为包含 `data` 数组的 JSON；数组元素支持 `ip`
或 `host`、`port`、`username`、`password`。下载体上限为 32 MiB，响应仍为导入
结果对象。

### 批量启用或停用

```http
POST /v1/admin/proxies/batch/disabled
Authorization: Bearer <admin_key>
Content-Type: application/json

{"ids": ["proxy-001", "proxy-002"], "disabled": true}
```

响应示例：

```json
{"updated": 2, "deleted": 0, "skipped": 0}
```

### 批量删除

```http
POST /v1/admin/proxies/batch/delete
Authorization: Bearer <admin_key>
Content-Type: application/json

{"ids": ["proxy-001", "proxy-002"]}
```

### 清理未占用代理

```http
POST /v1/admin/proxies/clear
Authorization: Bearer <admin_key>
```

无需请求体。接口删除所有未处于使用、检查或排空状态且没有活动连接的代理。
批量删除和清理接口返回 BatchResult：

```json
{
  "updated": 0,
  "deleted": 2,
  "skipped": 1,
  "errors": ["proxy-003: proxy is in use"]
}
```

导入和批量接口允许部分成功，具体失败项通过 `skipped` 和 `errors` 返回，通常仍
使用 HTTP `200`。

## HTTP 状态码

| 状态码 | 含义 |
| --- | --- |
| `200` | 查询、创建、更新或批量操作成功 |
| `204` | 删除或释放成功，无响应体 |
| `400` | JSON、地址、配置或当前资源状态无效 |
| `401` | Bearer Token 缺失或不正确 |
| `404` | 路径不存在 |
| `405` | 路径存在但 HTTP 方法不匹配 |
| `503` | 运行时配置组件不可用 |

动态供应商调用失败使用 `502`；API 模式未启用时动态申请使用 `503`。

## 管理页面安全说明

`GET /admin/` 本身不校验 Bearer Token，并会把配置的 `admin_key` 注入页面供其
调用管理 API。必须只在可信管理网络开放该页面和 API，或置于具备认证能力的
反向代理之后；不要直接暴露到互联网。
