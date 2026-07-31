# Shadowsocks 前置 sidecar 部署

本示例使用 `shadowsocks-rust` 的 `sslocal` 在
`127.0.0.1:11080` 提供本地 SOCKS5。sock5gw 只调用该 SOCKS5 端口，不直接
实现或保存 Shadowsocks 协议逻辑。

## 前提

- 从可信的软件源安装 `shadowsocks-rust`，将 `sslocal` 放到
  `/usr/local/bin/sslocal`，并固定、记录版本和文件校验值。
- SS 服务端地址、端口、密码和加密方法已经确认。客户端的 `method` 必须与
  服务端一致；不要使用已淘汰的 stream cipher。
- 租赁 SOCKS5 必须能从 SS 服务端所在网络访问。若租赁服务使用来源 IP 白名单，
  应放行 SS 服务端的出口 IP，而不是 sock5gw 主机的出口 IP。
- 本地端口只监听 `127.0.0.1`，不要暴露到 LAN 或公网。
- loopback 不能隔离同机进程。本样例启用本地 SOCKS5 认证；仍应使用受信任的
  专用主机，不要把该端口视为进程级安全边界。

## 安装

创建无登录权限的运行用户和受限配置目录：

```sh
sudo useradd --system --user-group --home-dir /nonexistent \
  --shell /usr/sbin/nologin sslocal
sudo install -d -o root -g sslocal -m 0750 /etc/sock5gw
sudo install -o sslocal -g sslocal -m 0600 \
  deployments/sslocal.example.json /etc/sock5gw/sslocal.json
sudo install -o sslocal -g sslocal -m 0600 \
  deployments/sslocal-auth.example.json /etc/sock5gw/sslocal-auth.json
```

编辑 `/etc/sock5gw/sslocal.json`，替换 `.invalid` 示例地址和 SS 服务端密码，
并按服务端配置调整端口及 `method`。再编辑
`/etc/sock5gw/sslocal-auth.json`，为本地 SOCKS5 设置独立强密码。不要复用 SS
服务端密码，也不要把真实密码放进仓库、systemd 命令行或日志。两个配置文件
必须保持 `0600`：

```sh
sudo stat -c '%a %U:%G %n' \
  /etc/sock5gw/sslocal.json /etc/sock5gw/sslocal-auth.json
```

安装 sidecar 服务以及 sock5gw 的弱依赖 drop-in：

```sh
sudo install -o root -g root -m 0644 deployments/sslocal.service \
  /etc/systemd/system/sslocal.service
sudo install -d -o root -g root -m 0755 \
  /etc/systemd/system/sock5gw.service.d
sudo install -o root -g root -m 0644 \
  deployments/sock5gw.service.d/sslocal.conf \
  /etc/systemd/system/sock5gw.service.d/sslocal.conf
sudo systemctl daemon-reload
sudo systemctl enable --now sslocal.service
```

`sslocal.service` 的 `ExecStartPost` 最多等待 5 秒，确认本地端口开始监听；该检查
计入 `Before=/After=` 排序，但不验证 SS 服务端或目标网络。drop-in 不使用
`Requires=`，避免把两个服务绑定成同一故障域。运行中 `sslocal` 失败时 sock5gw
继续运行，管理 API 仍可访问，代理路径按 fail-closed 规则失败。

## 启用 sock5gw 前置代理

在 `/etc/sock5gw/config.json` 中加入：

```json
"front_proxy": {
  "enabled": true,
  "protocol": "socks5",
  "address": "127.0.0.1:11080",
  "username": "sock5gw",
  "password": "replace-with-local-socks-password",
  "fail_open": false
}
```

先确认本地监听和 SS 链路，再重启 sock5gw：

```sh
sudo systemctl status sslocal.service
ss -lnt '( sport = :11080 )'
curl --proxy-user sock5gw --socks5-hostname 127.0.0.1:11080 https://example.com/
sudo systemctl restart sock5gw.service
```

上述 `curl` 会交互式询问本地 SOCKS5 密码，避免把密码写入 shell 历史。sock5gw
配置中的用户名和密码必须与 `sslocal-auth.json` 一致。

启用后应再执行项目规划文档中的数据面、故障隔离和防绕过验收。单次 `curl`
成功只能说明 SS sidecar 可用，不能证明 SS 服务端能够访问租赁 SOCKS5，也不能
证明完整的两级 SOCKS5 链路正确。

## 停用与回滚

把 `front_proxy.enabled` 改为 `false`，保持 `fail_open` 为 `false`，重启
sock5gw 并确认原有直连租赁 SOCKS5 的行为恢复。确认无回滚依赖后再执行：

```sh
sudo systemctl disable --now sslocal.service
```

不要仅停止 sidecar 作为回滚手段；在前置配置仍启用时，这会按设计阻断所有
`proxy` 路径的新连接。
