# 独立评审记录 —— 转录轮次原子发布

## Review Charter
- **评审对象**：一轮转录产物"暂存 → 校验 → 烧录 → 原子发布"，远程下载（sync/async）复用同一流程；移除本轮不再生成的旧受管产物但保留用户文件；中断恢复后只见完整旧轮或完整新轮；并发写入与下载断流安全；返回目录/产物名/HTTP 表面兼容。
- **评审方式**：由**全新上下文的独立只读评审 agent**（不接触实现过程）逐条核对 spec.md 的 AC-1～AC-12，独立读代码、独立跑测试，不信任 tasks.md 的自评证据。
- **评审边界**：只读，不修改任何文件、不运行任何自动修复（black/ruff --fix 等）；可运行 pytest / 静态检查的只读子命令。
- **结论路由**：
  - `pass`：全部 AC 达到阈值（rubric AC-11/AC-12 ≥ 4），无阻断缺陷 → 收尾。
  - `fail`：存在阻断/正确性缺陷或某条 rule AC 不成立 → 把 actionable findings 落成 tasks.md 的 pending issues 并修复，修复后复审。
  - `blocked`：评审因环境/材料缺失无法完成 → 补齐后重审。

## Materials
- 规格：`.trae/specs/atomic-round-publish/spec.md`
- 计划/证据：`.trae/specs/atomic-round-publish/tasks.md`
- 核心新模块：`src/transcribe_anything/publish.py`
- 集成点：`src/transcribe_anything/api.py`、`src/transcribe_anything/client.py`、`src/transcribe_anything/server_app.py`、`src/transcribe_anything/server_config.py`
- 测试：`tests/test_publish.py`、`tests/test_api_staging.py`、`tests/test_client_resilience.py`、`tests/test_server_app.py`
- 测试命令：`uv run pytest <files> -q`；全量：`uv run pytest tests/ --ignore=tests/test_insanely_fast_whisper_mlx.py --ignore=tests/test_mlx_timestamps.py -q`
- 静态检查（只读）：`uv run black --check <files>`、`uv run mypy <files>`、`uv run ruff check <files>`

## 评审重点（独立核对，不要只信证据）
1. **原子性/可恢复性正确性**：journal 落盘点、`os.replace` 顺序（先替换后删除）、四个崩溃时点（暂存期 / journal 后替换中 / 替换完未删 stale / journal 在暂存失）恢复后是否严格收敛到完整一轮；staging 位于最终目录内保证同文件系统。
2. **锁正确性**：RLock 可重入记账、跨进程 mkdir 锁、死 pid 回收与 tombstone 竞争、owner 文件缺失的年龄保护；同目录串行、异目录并发；有无死锁/全局锁粒度隐患。
3. **远程复用是否真同构**：sync/async 是否都走 publish 原语；断流异常边界是否覆盖 stream 上下文退出；`.part` 清理；Content-Length/空文件校验；集合 mismatch；stale 删除与用户文件保留是否与本地一致。
4. **兼容面零变化**：公开签名/返回绝对路径/产物名/daemon HTTP；隐藏名 404 是否仅安全收紧而不影响正式产物。
5. **测试是否真验证了主张**：压力测试是否断言"所有受管文件同一轮标识"而非仅"不报错"；断流测试是否同时断言旧轮字节不变 + 无内部残留 + 重试成功；有无伪绿（桩把行为替没了）。

## Findings

### 首轮独立评审（全新上下文、只读）
- **结论**：AC-1～AC-10 全部成立；rubric AC-11（复用度）/AC-12（侵入性）独立评分均 **5/5**（阈值 ≥4）。
- 评审 agent 除跑仓库套件外，另做了独立实测：真实 TCP socketserver 的 trunc/drop × sync/async 断流+重试、四个崩溃现场手工构造、journal 名单越界守卫、owner 年龄保护、死 pid 回收无残留、RLock 重入记账、用户 dot 条目保留、五个后端产出是否都落在 7 个受管名内——均通过，无伪绿。
- **F1（low，已修复，见下）**：同线程重入（同一事件循环两个 asyncio 任务下载到同一目录）时，后进入者的恢复会清扫外层存活暂存；并可能泄漏裸 `FileNotFoundError`。当前无生产调用嵌套、AC-9 给定的多线程场景正确，故不阻断。
- **F2（informational，不修复）**：不可解析 journal 会永久残留。该协议只经 `os.replace`+fsync 安装完整 journal，进程崩溃不可能产生撕裂文件，仅磁盘损坏/人为篡改可触发；不影响后续发布。
- **F3（informational，不修复）**：锁回收 tombstone（`.transcribe-round.lock.reaped-*`）不在 `.staging-*` 清扫前缀内，仅在 rmtree 失败时残留，不影响后续加锁。
- **F4（informational，不修复）**：journal+staging 存活但某 new 文件被外力从两处删失时前滚仍收尾——不可能由"进程死亡"崩溃模型产生（文件只经 os.replace 离开暂存），属 spec Non-Goals 明确排除的磁盘故障/跨主机语义。

### F1 修复与复审闭环（resolved）
- 修复：[publish.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/publish.py) 为 `_RoundGuard` 增加 `reentered` 属性；`staged_round` 与 `recover_rounds` 仅在外层持有者（`not guard.reentered`）执行 `_recover_locked`。RLock 线程绑定保证重入必为同线程嵌套；外层进入已在创建暂存前完成对账，内层跳过恢复且退出时只 rmtree 自己的 `stage.path`。跨线程/跨进程互斥、外层孤儿清扫、journal 四时点恢复、8×5 压测语义均不变。
- 异常契约：[client.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/client.py) `_publish_downloaded_round` 将 `iterdir` 集合比对一并纳入 try，发布期任何 `OSError` 统一包成 `RemoteTranscriberError`（FR-7 无死角）。
- 新增测试：
  - `test_publish.py::test_nested_same_thread_round_preserves_outer_staging`（外层仍清扫早先 orphan；嵌套 `recover_rounds` 为 no-op；内层放弃只清自己；外层可正常提交）。
  - `test_client_resilience.py::test_concurrent_async_downloads_same_directory`（用 `_FirstArtifactBarrier` **确定性**保证两个下载在任一 body 字节发出前都已处于各自 staging 上下文——不依赖 wall-clock sleep；真实发生锁重入 depth `[0,1]`）。
  - `test_client_resilience.py::test_publish_failure_is_surfaced_as_remote_error`（OSError→RemoteTranscriberError 的直接单测）。
- **反事实验证**：用仓库外临时插件强制 `reentered` 恒 False（模拟旧行为），两个并发/嵌套测试均如期失败（async 用例报外层 `.part` 被清扫的 `RemoteTranscriberError`），恢复修复后通过——确认是真回归测试而非伪绿。
- 复审（同一独立 agent，只读）结论：**resolved**，互斥/孤儿清扫/journal 恢复/压测无回退，无新增生产缺陷；并据此把异步测试从"健康 Response（depth [0,0]、旧行为也通过）"加固为 barrier 真交错版本。
- 静态检查：`black --check` 9 文件 unchanged；`mypy` 5 源文件 no issues；`ruff` 新模块/新测试零告警，client.py 等改动文件相对 HEAD 的规则码直方图完全一致（33 UP045 等均为预存在告警，本次新增的 4 处 `Optional[int]` 已改 `int | None`，零新增告警）。
- 测试：目标四模块 **85 passed**；全量（排除两个预存在 MLX 环境失败模块）**305 passed, 9 skipped**，零失败。

## Verdict
**pass**

- AC-1～AC-10 全部满足且证据经独立复核实测；AC-11 = 5/5、AC-12 = 5/5（均 ≥ 阈值 4）。
- 首轮唯一 low 发现 F1 已修复并经独立复审 resolved，配套三个回归测试（含确定性交错屏障与反事实失效验证）。
- F2/F3/F4 为仅外部损坏可触发、且被 spec Non-Goals 排除的 informational 项，记录在案、不阻断收尾。

