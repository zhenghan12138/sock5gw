# sock5gw nftables 启动依赖

本方案在隔离 network namespace 中验证候选规则，再让
`sock5gw-nftables.service` 加载到宿主机。规则文件、配置或关键安全语义有误时，
`sock5gw.service` 不会启动，候选规则也不会先进入宿主机 ruleset。

## 设计约束

- 规则文件只允许替换 `table inet sock5gw`，拒绝 `flush`、`include`、其他
  table 和命令式增删操作，避免影响 SSH、Docker、VPN 或其他防火墙规则。
- `sock5gw-nftables-validate.service` 使用 `PrivateNetwork=yes`，在隔离 namespace
  加载候选规则，并通过 `nft --json` 与 `jq` 检查唯一表、base chain
  type/hook/priority/policy、精确规则集合与顺序、统一接口和客户端网段、本机绕过、
  redirect 端口及 IPv4/IPv6 fail-closed drop。
- validator 在同一把排他锁内复制规则和 `config.json`，所有语法与语义检查都
  针对权限为 `0600` 的私有快照。验证成功后才向 apply 原子发布快照及 SHA-256
  manifest；宿主机 apply 会复核源文件、快照和 manifest 一致，并且只加载已经
  验证过的规则快照，关闭验证与加载之间的文件变更窗口。
- validator 是不设置 `RemainAfterExit` 的 oneshot，因此每次启动 sock5gw 都会
  重新运行。loader 使用 `RemainAfterExit=yes` 负责开机加载；主服务的
  `ExecStartPre` 仍会应用刚验证的当前文件。
- 规则文件以 `destroy table inet sock5gw` 开始。宿主机 transaction 解析或执行
  失败时，旧表保持不变。
- 不配置删除规则的 `ExecStop`。停止服务时保留内核规则，使客户端保持
  fail-closed。
- v1 只代理 IPv4 TCP。客户端 IPv6和未重定向的转发流量会被显式丢弃。
- 不要另外启用会执行 `flush ruleset` 的发行版 `nftables.service`。若系统必须
  使用它，应先把所有规则统一到同一加载源；运行中手动 flush 仍可删除专用表。
- systemd 的启动依赖只保证“规则失败则进程不启动”。首次安装尚无旧表，或运行
  中规则曾被 flush 时，若 `net.ipv4.ip_forward=1`，仅阻止进程并不能阻止内核
  转发。接入客户端前必须先成功加载并验收本表；专用透明网关建议保持
  `net.ipv4.ip_forward=0`。确需转发被排除的私网流量时，应先部署独立于本表的
  宿主 FORWARD 基线策略并监控规则丢失。

## 安装

安装 `nftables`、`jq` 和提供 `flock` 的 `util-linux`。本方案已在 Ubuntu 24.04
的 nftables 1.0.9 上验证；其他版本必须先确认支持 `destroy table`：

```sh
sudo apt-get install nftables jq util-linux
```

根据生产配置修改 `nftables.example.nft` 顶部的五个变量：

```nft
define client_if = "eth0"
define client_lan = 192.168.1.0/24
define gateway_ipv4 = 192.168.1.10
define dns_port = 5353
define gateway_port = 15001
```

`dns_port` 和 `gateway_port` 必须分别等于 `config.json` 中的 `dns.listen` 与
`gateway.listen` 端口。两个 listener 必须监听 `0.0.0.0` 或
`gateway_ipv4`，且 `gateway.transparent_proxy` 必须为 `true`。

覆盖现有文件前先建立权限为 `0700` 的备份目录，至少保存 `/etc/sock5gw`、原
loader、主服务 drop-in 和当前 ruleset：

```sh
backup_dir=/var/backups/sock5gw/nft-preinstall
sudo install -d -o root -g root -m 0700 "$backup_dir"
sudo cp -a /etc/sock5gw "$backup_dir/etc-sock5gw"
sudo cp -a /etc/systemd/system/sock5gw-nftables.service \
  "$backup_dir/" 2>/dev/null || true
sudo cp -a /etc/systemd/system/sock5gw.service.d \
  "$backup_dir/" 2>/dev/null || true
sudo nft -nn list ruleset | sudo tee "$backup_dir/ruleset.nft" >/dev/null
```

安装正式规则、两个 helper、validator、loader 和主服务 drop-in：

```sh
sudo install -o root -g root -m 0644 \
  deployments/nftables.example.nft /etc/sock5gw/nftables.nft
sudo install -d -o root -g root -m 0755 /usr/local/libexec
sudo install -o root -g root -m 0755 \
  deployments/sock5gw-nftables-validate \
  /usr/local/libexec/sock5gw-nftables-validate
sudo install -o root -g root -m 0755 \
  deployments/sock5gw-nftables-apply \
  /usr/local/libexec/sock5gw-nftables-apply
sudo install -o root -g root -m 0644 \
  deployments/sock5gw-nftables-validate.service \
  /etc/systemd/system/sock5gw-nftables-validate.service
sudo install -o root -g root -m 0644 \
  deployments/sock5gw-nftables.service \
  /etc/systemd/system/sock5gw-nftables.service
sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/sock5gw.service.d
sudo install -o root -g root -m 0644 \
  deployments/sock5gw.service.d/nftables.conf \
  /etc/systemd/system/sock5gw.service.d/nftables.conf
```

先做不修改宿主机 ruleset 的检查：

```sh
sudo nft --check --file /etc/sock5gw/nftables.nft
sh -n /usr/local/libexec/sock5gw-nftables-validate
sh -n /usr/local/libexec/sock5gw-nftables-apply
sudo systemctl daemon-reload
sudo systemd-analyze verify \
  /etc/systemd/system/sock5gw-nftables-validate.service \
  /etc/systemd/system/sock5gw-nftables.service \
  /etc/systemd/system/sock5gw.service
```

在维护窗口启用 loader 并重启主服务。启动事务会先运行隔离验证，再加载规则：

```sh
sudo systemctl enable sock5gw-nftables.service
sudo systemctl restart sock5gw.service
```

首次安装必须在客户端尚未使用本机作为网关时完成。只有下方验收全部通过后才接入
客户端；不能把“sock5gw 启动失败”当作缺少宿主防火墙时的转发兜底。

## 验收

```sh
sudo systemctl is-active sock5gw-nftables.service sock5gw.service
sudo systemctl show sock5gw-nftables-validate.service \
  -p Result -p ExecMainStatus
sudo systemctl show sock5gw.service \
  -p Requires -p After -p ExecStartPre -p RestartPreventExitStatus
sudo nft --json list table inet sock5gw |
  jq -e 'any(.nftables[]; .rule?.comment == "sock5gw:tcp-redirect")'
systemd-analyze critical-chain sock5gw.service
```

正常状态是 validator 的最近结果为 `success`、loader 为 `active (exited)`、
sock5gw 为 `active (running)`。DNS 和 TCP redirect counter 应随客户端流量增加；
forward drop counter 只在流量试图绕过网关时增加。

设备侧验证：

1. 同二层内网和网关本机地址仍可访问。需要经本机路由的其他 RFC1918 网段默认
   fail-closed，若确实需要放行，必须同步修改规则模板和 validator。
2. A 查询返回 `198.18.0.0/15` 中的 FakeIP。
3. IPv4 TCP 网站可访问，出口地址等于租赁代理出口。
4. IPv6、QUIC 和普通 UDP 不发生直连绕过。

## 失败注入

测试前保持独立 SSH 会话和自动恢复手段。下例备份正式规则、换入语法错误文件，
然后验证主服务无法启动：

```sh
sudo install -m 0600 /etc/sock5gw/nftables.nft \
  /root/sock5gw-nftables.known-good
sudo cp /root/sock5gw-nftables.known-good /etc/sock5gw/nftables.nft
sudo sed -i '1i this is deliberately invalid nft syntax' \
  /etc/sock5gw/nftables.nft
sudo systemctl stop sock5gw.service sock5gw-nftables.service
sudo systemctl start sock5gw.service
```

预期结果：

- `systemctl start sock5gw.service` 返回非零。
- validator 为 `failed`，loader 和 sock5gw 不进入运行态。
- 配置的 DNS、Gateway 和 API 端口均没有监听。
- 候选只在隔离 namespace 中处理，宿主机旧 `table inet sock5gw` 保持不变。
- journal 明确记录来源范围、语法、配置或语义验证失败。

把任一必需的 `sock5gw:*` rule comment 改名可验证“语法有效但语义缺失”的失败
路径；结果同样必须是旧宿主机表不变、主服务不启动。

恢复后执行：

```sh
sudo install -m 0644 /root/sock5gw-nftables.known-good \
  /etc/sock5gw/nftables.nft
sudo systemctl reset-failed \
  sock5gw-nftables-validate.service \
  sock5gw-nftables.service \
  sock5gw.service
sudo systemctl start sock5gw.service
```

复验完成后可按备份保留策略删除
`/root/sock5gw-nftables.known-good`。

## 回滚

不能在客户端仍把本机作为网关时移除持久化 nft 依赖。优先从安装前备份恢复旧
helper、unit、drop-in 和规则文件，并在独立 SSH 会话中验证旧启动路径。若要完全
停用 sock5gw，先把客户端迁移到其他网关，再执行：

```sh
sudo systemctl disable --now sock5gw.service
sudo systemctl disable --now sock5gw-nftables.service
sudo rm /etc/systemd/system/sock5gw.service.d/nftables.conf
sudo rm /etc/systemd/system/sock5gw-nftables.service
sudo rm /etc/systemd/system/sock5gw-nftables-validate.service
sudo systemctl daemon-reload
```

确认客户端已经迁走且不再需要透明网关后，才删除 `table inet sock5gw` 和两个
helper。不要先删规则或继续启用缺少持久化规则依赖的主服务，否则客户端流量可能
直接进入系统转发路径。
