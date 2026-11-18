# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。缓存容量有上限（默认 48MiB，可在
Docker Compose 中配置），超限先淘汰最久未下载的包，仍不足则返回 507。
代理由 **FastAPI + aiohttp** 实现，单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin`、
  24MiB 包 `pkg-24m.bin` 与 1MiB 包 `pkg-1m.bin`）

容量上限由 `docker-compose.yml` 中代理服务的 `CACHE_MAX_BYTES` 环境变量配置
（字节数，默认 48MiB = 50331648）。

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态与容量账目
curl http://localhost:8080/cache/status
curl http://localhost:8080/cache/quota
```

容量演示（默认 48MiB 上限）：下载 32MiB 包后再下载 24MiB 包，前者被自动
淘汰（`X-Cache` 变回 MISS）；用源站控制 API 生成超过 48MiB 的包
（`POST /control/content/pkg-big.bin?size=62914560`），下载立即得到
`507 Insufficient Storage`，不留残件，释放空间后重试即可成功。

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **容量上限**：任一时刻计入额度的总量 = 完整包 + 填充中的整包预留 +
  已清除但仍被旧读者占用的包，不超过 `CACHE_MAX_BYTES`。填充按源站
  `Content-Length` 在写第一个字节前预留整包空间；空间不足时先淘汰
  最久未下载且无人读取的完整包（淘汰绝不中断读者），仍不足则该代所有
  下载在发送正文前得到 `507 Insufficient Storage`，不产生残件，空间
  释放后重试即可。清除后的新代重新预留空间；重启保留的完整包在启动时
  计入额度。源站响应缺少 `Content-Length` 时无法预留，填充按失败处理。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越，以及容量上限（LRU 淘汰、507 与恢复、超限包并发单飞、
已清除代计费至最后读者、重启计入额度）。`tests/run_server.py` 是测试
专用启动器（本沙箱禁止进程间信号，用控制文件代替 SIGTERM 停服），
交付应用本身不含测试钩子。
