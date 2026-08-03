# 仓库指南

## 项目结构与模块组织

`cmd/sock5gw/main.go` 是守护进程入口。核心代码位于 `internal/`：`config`
负责解析和校验 JSON 配置，`manager` 管理租约与健康状态，`gateway` 处理透明
TCP 流量，`dnsproxy` 管理 FakeIP DNS，`outbound` 建立 SOCKS5 代理链，
`routing` 匹配域名规则，`store` 持久化状态。单元测试以 `*_test.go` 命名，
与被测包放在同一目录。systemd 单元、nftables 示例和运维指南位于
`deployments/`，设计文档位于 `docs/`。请将 `config.example.json` 作为配置结构
说明和本地配置起点。

## 构建、测试与开发命令

- `go mod download`：下载 `go.mod` 声明的依赖。
- `go build -o bin/sock5gw ./cmd/sock5gw`：构建本地可执行文件。
- `go run ./cmd/sock5gw -config config.example.json`：从源码运行；非特权开发
  环境中应调整监听端口和数据库路径。
- `go test ./...`：运行全部单元测试。
- `go test -race ./...`：检查租约、健康检查和连接器代码中的并发问题。
- `go vet ./...`：执行 Go 标准静态分析。
- `gofmt -w path/to/file.go`：格式化修改过的 Go 文件。

透明代理的原始目标地址查询仅支持 Linux。其他平台会使用桩实现，无法验证完整
网关链路。

## 部署测试环境

需要进行 Ubuntu 实机部署或集成测试时，可使用以下测试服务器：

- 主机：`192.168.2.242`
- 用户名：`ubuntu`
- 密码：`ubuntu@1502`

仅将该服务器用于本仓库相关测试。部署前先完成本地测试，并避免覆盖服务器上与
本项目无关的配置或服务。

## 编码风格与命名约定

遵循 Go 惯用写法，并以 `gofmt` 的制表符和布局结果为准。包名使用简短的小写
名词；导出标识符使用 `PascalCase`，未导出标识符使用 `camelCase`。优先在现有
包的职责范围内实现功能。底层包应返回包含上下文的错误，仅由命令入口负责日志
记录。JSON 字段继续使用 `snake_case`，与 `config.example.json` 保持一致。

## 测试规范

使用 Go 标准库 `testing`。测试函数命名为 `TestBehaviorBeingVerified`，输入变体
使用 `t.Run`。每次修改都应在对应包中添加聚焦测试，尤其覆盖失败关闭、上下文
取消、租约状态转换和凭据脱敏。提交前运行 `go test ./...`、
`go test -race ./...` 和 `go vet ./...`。

## 提交与合并请求规范

现有提交使用简短的祈使式标题，例如 `Add web-managed routing configuration` 和
`Fail startup when nftables validation fails`。每个提交只包含一个行为变更。合并
请求应说明问题、运行影响、配置或部署变更，以及实际执行的验证命令；关联相关
Issue，管理界面变更需附截图。除“部署测试环境”中明确授权的测试服务器外，
禁止提交真实代理凭据、Bearer Token、数据库或 GeoIP 数据；示例值必须明显属于
非生产环境。

## 交付总结规范

完成项目修改后，交付总结的标题与修改内容必须使用中文。总结应结合本次项目的
实际改动详细说明，不得只给出笼统结论；需说明涉及的文件和模块、具体行为变化、
配置或部署影响、新增或调整的测试，以及实际执行的验证命令和结果。未执行的验证
项或仍存在的限制、风险也应明确注明。
