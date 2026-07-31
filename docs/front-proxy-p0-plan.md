# sock5gw 前置代理实施规划

更新时间：2026-07-22

状态：**P0 代码与自动化检查已完成**；Linux network namespace、nftables 和抓包
验收需在目标 Linux 主机执行。P1、P2 为后续规划，不包含在本次交付中。

## 1. 决策与目标

前置传输采用独立 sidecar，sock5gw 不原生实现 Shadowsocks。推荐使用持续维护的
`shadowsocks-rust/sslocal`，由它在本机提供 SOCKS5 端口并负责 SS 加密、服务端
兼容和安全更新。sock5gw 只实现通用的 SOCKS5 链式拨号。

维护状态核对日期为 2026-07-22：官方
[`shadowsocks/shadowsocks-rust`](https://github.com/shadowsocks/shadowsocks-rust)
仓库未归档且当天仍有提交，最近稳定版为 `v1.24.0`（2025-12-10 发布）。部署时
仍应固定经过验证的具体版本和校验值，而不是自动跟随最新版。

本接口只依赖 sidecar 暴露的标准 SOCKS5，因此第三方 SSR 客户端若能稳定提供
标准 SOCKS5 监听，链路在技术上也可以接入；但 SSR 实现和维护状态较分散，本次
不提供推荐实现、配置样例或兼容性承诺，也不把 SSR 纳入 P0 验收。生产默认选择
`shadowsocks-rust` 的 SS sidecar。这里的 `sslocal` 只实现 SS，不能连接依赖
SSR 特有 protocol/obfs 的服务端；SSR 必须另行部署能暴露标准 SOCKS5 的 sidecar。

本次 P0 的目标是为所有 `proxy` TCP 连接增加一个可选的全局第一跳，同时保持
未配置或关闭前置时的现有行为不变。

```text
客户端 TCP
  -> sock5gw
  -> 127.0.0.1:11080（sslocal 提供的 SOCKS5）
  -> SS 加密隧道
  -> SS 服务端
  -> 租赁 SOCKS5
  -> 目标地址
```

责任边界如下：

| 组件 | 负责 | 不负责 |
| --- | --- | --- |
| sock5gw | 客户端租约、路由、FakeIP 映射、两级 SOCKS5 CONNECT、连接及健康状态 | SS 密码学、SS 插件、SS 服务端兼容 |
| sslocal | 本地 SOCKS5、SS 加密传输、将第一级 CONNECT 发往 SS 服务端 | 租赁 SOCKS5 分配、客户端策略 |
| SS 服务端 | 代表本机连接租赁 SOCKS5 | sock5gw 管理 API 和出口池状态 |

## 2. P0 范围（本次实施）

### 2.1 包含

- 单个、全局、静态配置的 SOCKS5 前置代理。
- 仅作用于路由判定为 `proxy` 的 TCP 连接。
- 前置 SOCKS5 可选用户名和密码认证。
- Gateway 业务流量、租约分配前探测、周期健康检查和出口 IP 探测复用同一链路。
- 分阶段错误，用于区分前置故障和租赁出口故障。
- 脱敏的前置状态、5 秒熔断和单请求半开恢复。
- 前置故障时 fail closed，禁止自动直连绕过。
- 配置加载校验、单元测试、两级 SOCKS5 测试和 sidecar 部署示例。
- 通过重启加载配置；不在 P0 中提供运行时热更新。

### 2.2 不包含

- 原生 Shadowsocks 或 ShadowsocksR 协议实现。
- HTTP CONNECT、Trojan、WireGuard 等其他前置协议。
- 多前置节点、负载均衡、自动切换或按客户端选择前置。
- 通用 UDP、QUIC、SOCKS5 UDP ASSOCIATE。
- `direct` 路由的转发；`direct` 仍按现有逻辑直连目标。
- DNS upstream 的代理。前置链路会转交其中的 SOCKS5 域名，但不会自动解决
  `dns.upstream` 或客户端 DoH/DoQ 的污染及绕过问题。
- P0 原始范围不包含管理页面编辑前置配置、凭据托管和无重启切换；当前版本已在
  P0 之后补充脱敏管理界面、配置持久化和新连接热切换。

## 3. 配置契约

配置示例：

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

加载规则：

- 缺失 `front_proxy` 或 `enabled=false` 时保持原有直连租赁 SOCKS5 的行为。
- `enabled=true` 且未填写 `protocol` 时，默认使用 `socks5`。
- P0 只接受小写 `socks5`。
- `address` 必须是非空 host 加 1 到 65535 的数字端口；IPv6 必须使用
  `[::1]:11080` 形式。
- 前置地址不得与任何配置或存量租赁 SOCKS5 地址相同，以免形成自连接循环。
- SOCKS5 用户名和密码分别不得超过 255 字节；长度按 UTF-8 编码后的字节数计算。
- `fail_open=true` 在启动加载配置时直接报错，无论 `enabled` 当前值如何。
- 凭据不得出现在日志、状态 API、管理页面或错误文本中。

Gateway 业务请求使用 `gateway.dial_timeout`，健康检查使用
`health_check.timeout`；每次调用通过 context 传递自己的完整建链预算。取消请求
或达到 deadline 后必须关闭已建立的半连接，不能继续后台拨号。

## 4. 数据面行为

前置关闭时：

```text
sock5gw --TCP--> 租赁 SOCKS5 --CONNECT--> 目标
```

前置开启时：

1. sock5gw TCP 连接本地前置 SOCKS5。
2. 在前置连接上发送第一次 SOCKS5 CONNECT，目标是租赁 SOCKS5 的原始
   `host:port`。
3. 第一次 CONNECT 成功后，同一条流中发送租赁 SOCKS5 greeting；完成租赁
   SOCKS5 认证后再发送第二次 CONNECT，目标是客户端请求的域名或 IP。
4. 两次握手均成功后才开始双向复制业务流量。
5. 任一步骤失败即关闭连接并返回错误，不尝试绕过前置。

`shadowsocks-rust/sslocal` 在连上 SS 传输端点后即可返回第一次 CONNECT 成功；
此时它通常尚未把租赁出口地址发给 SS 服务端。因此第一次 `REP=0` 不能证明 SS
密码正确、SS 服务端已解密请求或租赁 SOCKS 可达。只有收到租赁 SOCKS 返回的
有效 SOCKS5 greeting，才构成前置链路已经到达该出口的成功证据。

租赁 SOCKS5 使用域名配置时，不应在 sock5gw 主机预解析。第一级 SOCKS5
请求应保留域名，使 SS 服务端一侧完成解析。目标域名同理由租赁 SOCKS5 解析。

连接到 `direct` 目标时不调用该链式连接器；该语义是路由策略的一部分，而不是
前置故障时的降级路径。

## 5. 错误语义与健康隔离

P0 必须保留错误发生阶段，不能只返回无法归因的普通拨号错误。

| 阶段 | 示例 | 健康状态语义 |
| --- | --- | --- |
| `front_dial` | 本地端口拒绝、sidecar 未启动 | 全局前置故障；不得将租赁出口标为 unhealthy |
| `front_circuit` | 熔断期间的新请求、已有半开探测 | 全局前置故障；快速失败且不得影响出口池 |
| `front_handshake` | 前置返回非法 SOCKS5 greeting | 全局前置故障；打开熔断且不得影响出口池 |
| `front_auth` | 前置认证方法不支持、凭据错误 | 全局前置配置故障；打开熔断且不得影响出口池 |
| `front_connect_exit` | 前置无法 CONNECT 所选出口，或第一次 CONNECT 成功但未收到有效的租赁 SOCKS5 greeting | 标记为 `ambiguous`；先轮转或只读探测其他出口，不立即污染出口池或打开熔断 |
| `exit_dial` | 前置关闭时无法直连租赁出口 | 选中出口故障；沿用既有失败阈值 |
| `exit_handshake` / `exit_auth` | 前置关闭时的租赁 SOCKS5 协议错误，或已收到有效 greeting 后的认证错误 | 选中出口故障；沿用既有失败阈值处理该出口 |
| `exit_connect_target` | 租赁 SOCKS5 拒绝或无法到达探测目标 | 选中出口的端到端探测失败；沿用既有阈值 |
| `canceled` | 调用方主动取消 | 不改变前置或出口健康，不触发后台重拨，并关闭半连接 |
| `timeout` | 建链预算耗尽 | 按实际发生阶段归因，不触发直连回退，并关闭半连接 |

`front_dial`、`front_handshake` 或 `front_auth` 发生后，共享熔断立即打开 5 秒。
窗口内的新请求以 `front_circuit` 快速失败，队列等待剩余窗口；窗口结束后只允许
一个半开请求验证恢复。该请求必须至少收到一个租赁出口的有效 SOCKS5 greeting
后才关闭熔断；明确的共享失败或请求取消会重新进入 5 秒窗口。若半开请求停在
`front_connect_exit`，仍按下述跨出口规则处理，不能仅凭这一条歧义结果重开熔断。

`front_connect_exit` 使用 P0 的保守交叉验证流程：

1. 单次失败只把前置状态置为 `unknown`，候选出口暂不记失败，并立即尝试不同的
   idle 出口；必要时可对不同的 active/draining 出口做一次不改变租约的只读探测。
2. 若另一出口返回有效的 SOCKS5 greeting，即使随后在出口认证或目标 CONNECT
   阶段失败，也足以证明前置链路到达该出口；之前的 ambiguous 结果才可归因
   对应出口。仅收到 `sslocal` 对第一次 CONNECT 的成功响应不算此类证据。
3. 至少两个不同 endpoint 的可验证出口都返回 ambiguous，且该批次中没有更新的
   成功证据时，才确认共享前置故障、恢复候选出口为 idle、保持租约 pending 并
   打开 5 秒熔断。多个 ID 指向同一 endpoint 时只算一个样本。
4. 每个不确定批次携带 generation 和有序 token。并发请求产生的更新成功证据，
   或旧批次的延迟结果，不能覆盖较新的前置状态。

对 `sslocal` 这类 sidecar，第一次 SOCKS5 CONNECT 成功也无法可靠区分“SS 密码
或加密链路不可用”和“所选租赁出口不可达”，因为 SS 服务端连接目标失败时不会
返回 SOCKS 风格的结果。第二级 greeting 的跨出口证据避免坏首节点永久阻塞后续
出口，也避免 SS 故障批量污染出口池。只有一个可验证出口时无法进一步归因，
租约会保持 pending，队列只在本地退避 5 秒，不打开共享熔断；同步 API 请求同样
遵守该本地退避。P1 可用独立前置探测和指标继续细化。

管理状态 API 暴露 `disabled`、`unknown`、`healthy`、`unhealthy` 或 `half_open`
状态，以及脱敏后的最近公开错误和熔断截止时间；不返回凭据。管理 API 和已有
连接不应因 sidecar 进程退出而被主动关闭。

## 6. P0 实施项

| 顺序 | 实施项 | 本次状态 |
| --- | --- | --- |
| P0.1 | `FrontProxy` 配置、默认值和严格校验 | 已实现 |
| P0.2 | 独立 outbound connector 和可复用 SOCKS5 握手 | 已实现 |
| P0.3 | Gateway 的 `proxy` TCP 路径接入 connector | 已实现 |
| P0.4 | 租约探测、周期健康检查、出口 IP 检测接入 connector | 已实现 |
| P0.5 | 前置与出口错误分阶段，前置状态及 5 秒熔断/半开恢复 | 已实现 |
| P0.6 | context、deadline、连接清理和 fail-closed 行为 | 已实现 |
| P0.7 | 配置、握手、回归、race 测试及 sidecar 部署文档 | 已实现；Linux 实链路待验收 |

实现约束：

- 共享 connector 由 Manager 创建并持有；Gateway 通过 `Manager.ConnectProxy`
  复用它，避免两套拨号逻辑。
- 认证和 CONNECT 使用结构化 SOCKS5 编解码，不拼接非结构化字符串。
- 未启用前置时只增加很薄的调用层，不改变租约、路由和已有连接语义。
- sidecar 只监听 loopback；配置文件使用 `0600`，服务由 systemd 独立重启。

## 7. P0 验收标准

### 7.1 配置与兼容

- 旧配置不含 `front_proxy` 时可正常启动，行为和升级前一致。
- 前置启用但省略协议时默认 `socks5`。
- 非 SOCKS5 协议、非法地址、0 或大于 65535 的端口、超过 255 字节的凭据均
  在启动时拒绝。
- 任何 `fail_open=true` 配置均拒绝，错误中不包含用户名或密码。

### 7.2 正常链路

- 使用可控 mock 或测试代理验证两次 SOCKS5 CONNECT 的顺序和目标地址。
- 启用前置后，主机不直接连接租赁 SOCKS5；抓包只能看到本机 sidecar 连接以及
  sidecar 到 SS 服务端的加密连接。
- 租赁出口和最终目标使用域名时，sock5gw 本机不提前解析这些域名。
- `direct` 规则仍直接连接目标，不进入 sidecar。
- 业务转发、租约探测、健康检查和出口 IP 检测均能通过完整链路成功。

### 7.3 故障与安全

- 停止 `sslocal` 后，新建 `proxy` 连接在拨号超时内失败且没有直连租赁出口。
- sidecar 停止、认证错误或前置握手超时不会把所有租赁出口标为 unhealthy。
- 明确的共享前置失败立即熔断 5 秒；`front_connect_exit` 经跨出口确认后才熔断。
  期间请求快速失败，窗口结束后只有一个半开请求，成功时恢复为 healthy，失败
  时重新打开熔断。
- 单一 endpoint（包括多个 ID 指向同一地址）的 `front_connect_exit` 失败只触发
  5 秒本地队列退避，不打开共享熔断，也不把该出口标为 unhealthy。
- 收到租赁 SOCKS5 的有效 greeting 后，后续认证或目标 CONNECT 错误仍按既有
  阈值只影响该出口；有效 greeting 之前的 EOF、超时或非法响应保持 ambiguous。
- sidecar 故障期间管理 API 可访问，已有连接按 TCP 自身状态结束。
- 管理状态可查看前置状态、最近公开错误和熔断截止时间；日志、API 响应及 Web
  页面不泄漏前置用户名或密码。
- 每个失败或取消路径都关闭临时 socket，无持续文件描述符增长。

### 7.4 自动化检查

```sh
go test ./...
go test -race ./...
go vet ./...
```

Linux 上另执行 network namespace、nftables 和抓包的端到端验收。race 测试通过
不能替代实际链路的防绕过检查。

## 8. 部署顺序

1. 安装并固定 `shadowsocks-rust` 版本和校验值。
2. 以独立无登录用户部署 `sslocal`，配置与认证文件设为 `0600`，仅监听
   `127.0.0.1:11080`。
3. 独立验证本地 SOCKS5 到 SS 服务端链路。
4. 安装 `sslocal.service` 和 sock5gw 的 `Wants=/After=` drop-in。
5. 在 sock5gw 配置中启用 `front_proxy`，先在测试流量上重启验证。
6. 完成抓包、防绕过、故障隔离和管理 API 验收后再切换真实 LAN。

具体命令见 `deployments/README.front-proxy.md`。

## 9. 回滚

回滚不依赖删除数据库或修改租约：

1. 将 `front_proxy.enabled` 改为 `false`，确保 `fail_open` 仍为 `false`。
2. 重启 sock5gw，验证它恢复原有的“直连租赁 SOCKS5，再 CONNECT 目标”链路。
3. 确认管理 API、现有配置和出口池状态正常。
4. 最后停止并禁用 `sslocal.service`；按变更管理要求保留或移除其配置。

不要在前置仍启用时仅停止 sidecar，这属于故障演练而不是回滚，会按 fail-closed
设计阻断所有新的 `proxy` 连接。

如果新版本本身无法启动，恢复上一版本二进制并使用不含 `front_proxy` 的旧配置
即可。数据库格式在 P0 中不变，无需数据迁移或回退。

## 10. P1：生产运维能力

- 不依赖租赁出口的独立前置健康探测，以及可配置的熔断窗口和连接并发限制。
- 按阶段统计成功率、耗时、超时和认证错误，接入日志与监控告警。
- 独立的前置配置候选连通性测试；原子写盘、凭据脱敏和新连接无中断热更新已实现。
- 用独立本地 resolver 或 sidecar 的 DoH/DoT 能力解决 DNS upstream 污染。
- 自动化 Linux network namespace、nftables、抓包和 sidecar 重启测试。
- 固定 sidecar 供应链来源、升级窗口、校验值和回滚版本。

## 11. P2：协议与拓扑扩展

- 多个前置节点、优先级、健康切换和容量控制。
- HTTP CONNECT 等通用前置协议适配。
- 按租户、客户端或出口选择前置链路。
- 可选让订阅下载和其他控制面流量走指定前置。
- 通过成熟 sidecar/TUN 方案评估 UDP 与 QUIC，不在 sock5gw 内自行实现传输协议。
- 外部密钥管理、短期凭据和审计能力。

即使进入 P2，也不建议自行实现 SS/SSR 密码学。只有出现无法运行 sidecar、必须
单二进制交付等硬约束时，才重新评估集成成熟 SS 库的成本和安全责任。
