# Scheduler 与 Gateway API 层编译部署指南

本文档记录在目标服务器（如 90.91.183.35）上从源码编译并部署 AgentENV 集群调度层（Scheduler + Gateway API 层）的完整步骤，含实际踩坑与解决方案。

## 1. 部署架构

```
Client ──HTTP──> Gateway (:8081) ──gRPC──> Scheduler (:9091)
                        │                       │
                        │ LookupNode/Schedule   │ 静态节点列表 / 心跳
                        ▼                       ▼
                  aenv 节点 (:8000，Rust) ──── 心跳上报 ────► Scheduler
```

| 组件 | 语言 | 端口 | 说明 |
|---|---|---|---|
| aenv-gateway | Go | `0.0.0.0:8081`（HTTP）、`127.0.0.1:9102`（metrics） | API 层，认证 + 路由转发 |
| aenv-scheduler | Go | `127.0.0.1:9091`（gRPC）、`127.0.0.1:9101`（metrics） | 调度决策、节点注册、心跳、sandbox 绑定 |
| aenv | Rust | `127.0.0.1:8000` | 已有节点服务（`aenv.service`），需接入心跳 |

## 2. 环境要求

- openEuler 24.03 LTS-SP4（aarch64），内核 6.6+
- 已部署 aenv 节点（systemd 服务 `aenv.service`，配置在 `/var/lib/aenv/config/config.toml`）
- Go ≥ 1.25.0（`services/go.mod` 要求 `go 1.25.0`）
- 可用的 HTTP 代理（内网环境无法直连公网时）

## 3. 前置准备

### 3.1 确认端口占用，避免冲突

```bash
ss -tlnp | grep -E ':(9090|9091|8080|8081|9101|9102|8000)'
```

本机 9090 已被 iroh-relay 占用、8080 被 Ceph radosgw 占用，因此 Scheduler 用 **9091**、Gateway 用 **8081**。

### 3.2 安装 Go 1.25（走代理下载）

```bash
# 代理地址按实际环境填写
export http_proxy=http://<user>:<pass>@<proxy-host>:6688
export https_proxy=$http_proxy

cd /tmp
curl -fsSL -o go1.25.0.tgz https://mirrors.aliyun.com/golang/go1.25.0.linux-arm64.tar.gz \
  || curl -fsSL -o go1.25.0.tgz https://golang.google.cn/dl/go1.25.0.linux-arm64.tar.gz
rm -rf /usr/local/go
tar -C /usr/local -xzf go1.25.0.tgz
/usr/local/go/bin/go version   # go version go1.25.0 linux/arm64
```

> 注意：如果 `/usr/bin/go` 已有旧版本（如 1.24.6，低于 go.mod 要求），不要动它，
> 后续命令显式 `export PATH=/usr/local/go/bin:$PATH` 即可。

### 3.3 处理代理 TLS MITM 证书（Go 报 x509 时）

公司代理（华为 Web Secure Internet Gateway）会对 HTTPS 做 MITM，Go 默认不信任其根证书，
报错 `tls: failed to verify certificate: x509: certificate signed by unknown authority`。
（curl 能成功是因为 `/root/.curlrc` 里配了 `insecure`。）

从代理返回的证书链中提取网关根 CA 并装入系统信任：

```bash
# 取证书链中的第 2 张（中间 CA / 网关根）
timeout 15 openssl s_client -proxy <proxy-host>:6688 -connect goproxy.cn:443 -showcerts </dev/null 2>/dev/null \
  | awk '/BEGIN CERT/{n++} n==2{print} /END CERT/ && n==2{exit}' > /tmp/huawei-ca.pem

# 校验提取结果
openssl x509 -in /tmp/huawei-ca.pem -noout -subject -issuer
# subject=issuer=C=CN, ..., CN=Huawei Web Secure Internet Gateway CA V2

cp /tmp/huawei-ca.pem /etc/pki/ca-trust/source/anchors/huawei-web-gateway-ca.pem
update-ca-trust
```

## 4. 编译

在 AgentENV 源码树（服务器上 `/root/AgentENV`，或本地交叉编译后上传）：

```bash
cd /root/AgentENV/services

export PATH=/usr/local/go/bin:/usr/bin:/bin
export http_proxy=http://<user>:<pass>@<proxy-host>:6688
export https_proxy=$http_proxy
export HTTP_PROXY=$http_proxy HTTPS_PROXY=$http_proxy
export GOPROXY=https://goproxy.cn,direct

go build -o /usr/local/bin/aenv-scheduler ./scheduler/cmd
go build -o /usr/local/bin/aenv-gateway   ./gateway/cmd
```

产物：

```
/usr/local/bin/aenv-scheduler  (~61M)
/usr/local/bin/aenv-gateway    (~19M)
```

> 依赖下载偶发 `proxyconnect tcp: connection refused`（代理瞬时抖动），直接重试即可，
> Go 会复用已缓存的模块。

### 替代方案：本地交叉编译（服务器无网络时）

Go 交叉编译零成本，在任意有网机器上：

```bash
cd <repo>/services
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o aenv-scheduler ./scheduler/cmd
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o aenv-gateway   ./gateway/cmd
scp aenv-scheduler aenv-gateway root@<server>:/usr/local/bin/
```

## 5. 配置

### 5.1 服务配置 `/etc/aenv-gateway/config.json`

```json
{
  "log_level": "info",
  "log_format": "auto",
  "scheduler": {
    "grpc_listen_addr": "127.0.0.1:9091",
    "metrics_listen_addr": "127.0.0.1:9101",
    "strategy": "round_robin",
    "report_ttl": "30s",
    "binding_ttl": "30s",
    "redis_addr": "",
    "nodes": [
      { "id": "node-a", "endpoint": "http://127.0.0.1:8000" }
    ]
  },
  "gateway": {
    "http_listen_addr": "0.0.0.0:8081",
    "metrics_listen_addr": "127.0.0.1:9102",
    "scheduler_addr": "127.0.0.1:9091",
    "query_only_scheduler_addr": "",
    "request_timeout": "90s",
    "forward_response_size": 4194304,
    "sandbox_proxy_domains": []
  }
}
```

要点：

- **`nodes`**：静态节点列表（非 K8s 部署）。`id` 必须与节点侧
  `[node_identity].node_id` 一致，否则心跳会被 Scheduler 拒绝
  （`node is not in scheduler's configured node list`）。
  `endpoint` 指向节点 API 监听地址。
- **`redis_addr` 为空**：使用进程内 binding store（单机/演示够用）。
  多 Gateway 实例或要求重启后绑定保留时，填 Redis 地址
  （如 `127.0.0.1:6379`），Scheduler 会自动切换为 `RedisBindingStore`。
- **`strategy`**：`round_robin`（默认）或 `random`。
- 端口选择见 §3.1。

### 5.2 API Key `/etc/default/aenv-gateway`

Gateway 启动时从 `AENV_API_KEY` 环境变量或 `/run/secrets/api-key` 读取，
要求 32–256 个 URL-safe 字符（`a-zA-Z0-9._~-`）：

```bash
mkdir -p /etc/aenv-gateway
echo "AENV_API_KEY=<openssl rand -hex 24 的输出>" > /etc/default/aenv-gateway
chmod 600 /etc/default/aenv-gateway
```

## 6. systemd 服务

### `/etc/systemd/system/aenv-scheduler.service`

```ini
[Unit]
Description=AgentENV Scheduler
After=network.target

[Service]
User=root
Group=root
ExecStart=/usr/local/bin/aenv-scheduler -config /etc/aenv-gateway/config.json
Restart=on-failure
RestartSec=5
KillMode=process
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

### `/etc/systemd/system/aenv-gateway.service`

```ini
[Unit]
Description=AgentENV Gateway API
After=network.target aenv-scheduler.service
Wants=aenv-scheduler.service

[Service]
User=root
Group=root
EnvironmentFile=/etc/default/aenv-gateway
ExecStart=/usr/local/bin/aenv-gateway -config /etc/aenv-gateway/config.json
Restart=on-failure
RestartSec=5
KillMode=process
TimeoutStopSec=15

[Install]
WantedBy=multi-user.target
```

### 启动

```bash
systemctl daemon-reload
systemctl enable --now aenv-scheduler.service
sleep 2   # 等 scheduler 就绪
systemctl enable --now aenv-gateway.service
systemctl status aenv-scheduler aenv-gateway
```

## 7. 节点接入（开启心跳上报）

修改节点配置 `/var/lib/aenv/config/config.toml`：

```toml
[cluster]
# 指向 Scheduler 的 gRPC 地址（注意端口是 9091）
scheduler_endpoint = "http://127.0.0.1:9091"

[observability.scheduler_report]
enabled = true
interval_secs = 5
```

> 修改前先备份：`cp /var/lib/aenv/config/config.toml{,.bak-api-layer}`
> 注意 sed 批量替换 `enabled = false` 时要用行号限定范围，
> 避免误改 `[p2p].enabled` 等其他开关。

重启节点：

```bash
systemctl restart aenv.service
systemctl is-active aenv.service
```

心跳生效后，Scheduler 会周期性收到 `NodeSnapshot`（CPU/内存/磁盘/sandbox 计数），
并在心跳响应中下发集群 CPU 模板交集给节点（跨节点 snapshot 兼容性保障）。

## 8. 验证

```bash
API_KEY=$(grep AENV_API_KEY /etc/default/aenv-gateway | cut -d= -f2)

# 1) 健康检查（无认证，204 即正常）
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8081/health

# 2) 无 key 请求应 401
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8081/nodes

# 3) 带 key 查节点列表：应返回 node-a，status 为 "ready"
curl -s -H "X-API-Key: $API_KEY" http://127.0.0.1:8081/nodes | head -c 500

# 4) 业务请求走完整调度链路：Gateway -> Scheduler.Schedule -> node-a
curl -s -o /dev/null -w '%{http_code}\n' -H "X-API-Key: $API_KEY" http://127.0.0.1:8081/sandboxes
```

预期结果：

- `/nodes` 返回 `status: "ready"`、machineInfo（aarch64）、metrics（cpu/mem/disks）、
  `sandboxCount` 等心跳指标
- `/sandboxes` 返回 200（请求经 Scheduler 调度到 node-a 转发）

### 8.1 一键验证脚本

仓库提供 [scripts/tests/verify-gateway-scheduler.sh](../scripts/tests/verify-gateway-scheduler.sh)，
覆盖 Gateway 认证、双端 metrics、Scheduler gRPC 可达性、节点心跳、调度转发等 14 项检查：

```bash
# 在部署机上运行
API_KEY=$(grep AENV_API_KEY /etc/default/aenv-gateway | cut -d= -f2) \
  bash scripts/tests/verify-gateway-scheduler.sh

# 远程运行（本机指向服务器）
GATEWAY_ADDR=http://<server>:8081 \
GATEWAY_METRICS_ADDR=<server>:9102 \
SCHEDULER_METRICS_ADDR=<server>:9101 \
SCHEDULER_GRPC_ADDR=<server>:9091 \
API_KEY=<key> bash scripts/tests/verify-gateway-scheduler.sh

# 深度验证（创建 sandbox 并验证 LookupNode 绑定路由，最后自动清理）
DEEP=1 TEMPLATE_ID=<template-id> API_KEY=<key> bash scripts/tests/verify-gateway-scheduler.sh
```

可配置环境变量：`GATEWAY_ADDR`、`API_KEY`、`GATEWAY_METRICS_ADDR`、
`SCHEDULER_METRICS_ADDR`、`SCHEDULER_GRPC_ADDR`、`NODE_ID`、`HEARTBEAT_MAX_AGE_SECS`。

注意事项：

- 深度检查（`DEEP=1`）需要有效的 `TEMPLATE_ID` 且节点可创建沙箱；
  核心检查不依赖 jq（有 jq 时输出更详细的节点校验，无 jq 自动降级为 grep）
- `schedule_duration` 指标是 Prometheus HistogramVec，**首次 Schedule RPC 之后**才会出现在
  `/metrics` 输出里（lazy 注册），脚本对此自动降级为 SKIP
- 脚本退出码：0 = 全部通过，1 = 有失败项，2 = 环境缺依赖（curl / API_KEY）
- 服务器上已放置于 `/root/agentenv-verify/verify-gateway-scheduler.sh`

## 9. 常见问题（实际踩坑）

| 现象 | 原因 | 解决 |
|---|---|---|
| `curl: (7) Failed to connect ... 6688` | 环境里残留失效代理（`~/.bashrc` 的 `export http_proxy=...`） | 显式覆盖 `http_proxy`/`https_proxy`（大小写共 4 个变量），或 `env http_proxy=... curl` |
| `curl: (28) Resolving timed out` | 直连公网被防火墙拦截（DNS 也不通） | 必须走代理；`dnf` 源同样不可用 |
| `x509: certificate signed by unknown authority`（go build 拉依赖时） | 代理对 HTTPS 做 MITM，Go 不信任网关根证书 | §3.3：提取网关 CA 装入 `update-ca-trust` |
| `go.mod requires go >= 1.25.0` | 系统 `go` 是 1.24.x | 安装 go1.25.0 到 `/usr/local/go`，PATH 前置 |
| 编译时大量 `proxyconnect ... connection refused` | 代理瞬时抖动或旧代理变量未清干净 | 重试；确认 4 个代理环境变量都被覆盖 |
| 心跳被拒：`node is not in scheduler's configured node list` | 节点 `node_id` 与 scheduler `nodes[].id` 不一致 | 两边对齐（本部署均为 `node-a`） |
| Scheduler 起在 9090 失败 | iroh-relay 已占 9090 | 换 9091 |
| Gateway 起在 8080 失败 | Ceph radosgw 已占 8080 | 换 8081 |
| `/nodes` 返回 `[]` | 节点未开心跳上报（`scheduler_report.enabled=false` 或 `scheduler_endpoint` 未配） | §7 配置后重启 aenv，等一个心跳周期（5s） |

## 10. 日常运维

```bash
# 服务管理
systemctl status|restart aenv-scheduler aenv-gateway

# 日志
journalctl -u aenv-scheduler -f
journalctl -u aenv-gateway  -f

# metrics（Prometheus 格式）
curl -s http://127.0.0.1:9101/metrics   # scheduler
curl -s http://127.0.0.1:9102/metrics   # gateway
```

### 文件清单

| 路径 | 内容 |
|---|---|
| `/usr/local/bin/aenv-scheduler` | Scheduler 二进制 |
| `/usr/local/bin/aenv-gateway` | Gateway 二进制 |
| `/etc/aenv-gateway/config.json` | 两服务共用配置 |
| `/etc/default/aenv-gateway` | Gateway API key（600 权限） |
| `/etc/systemd/system/aenv-{scheduler,gateway}.service` | systemd 单元 |
| `/var/lib/aenv/config/config.toml` | 节点配置（含 scheduler_endpoint） |
| `/var/lib/aenv/config/config.toml.bak-api-layer` | 改动前备份 |

### 后续扩展

- **多节点**：`config.json` 的 `scheduler.nodes` 追加条目，或在 K8s 内用
  `discovery.mode = "kubernetes"`（EndpointSlice informer 自动发现）。
- **绑定持久化**：部署 Redis，`scheduler.redis_addr` 填地址，
  Gateway 多实例共享 sandbox→node 绑定，Scheduler 重启不丢。
- **资源感知调度**：`scheduler.node_resource_limit` 配置阈值
  （max_sandbox_count、max_cpu_used_percent、含 paused 的组合上限等），
  当前 round_robin/random 策略不消费心跳指标，过滤后才进入策略选择。
