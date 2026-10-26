# 转录轮次原子发布（暂存-校验-烧录-发布）- 产品需求文档

## Overview
- **Summary**: 一轮转录的全部产物（`out.txt` / `out.srt` / `out.vtt` / `out.json` / openai-whisper 后端另产出 `out.tsv` / 可选 `speaker.json` / 可选烧录产物 `out.mp4`）先写入与最终目录同文件系统的隐藏暂存区，经完整性校验、字幕烧录完成后，以"完整一轮"为单位发布到最终 `output_dir`；远程下载（sync / async 客户端）复用同一套暂存-校验-发布-恢复机制。
- **Purpose**: 现状下产物被逐个移动/直接写入最终目录（`api.py` 逐个 `shutil.move`、烧录直接写 `out.mp4`、客户端逐文件流式落盘），中途失败或断流会在 `output_dir` 留下截断文件与新旧混杂的一轮；本轮不再产出的旧文件（如旧 `out.mp4`、旧 `speaker.json`）也会残留。需要保证任何中断恢复后，用户只能看到**完整的旧一轮**或**完整的新一轮**。
- **Target Users**: 本地 CLI / Python API 调用者、daemon（`--serve`）作业、`--remote` 远程下载客户端的使用者。

## Goals
- 单轮产物对用户不可见地暂存，校验通过且烧录完成后才整体发布。
- 发布以"完整集合"收敛：替换本轮受管产物、删除本轮不再生成的受管旧产物、绝不触碰用户文件。
- 崩溃/中断可恢复：基于提交日志（journal）前滚或清扫，恢复后只存在完整旧轮或完整新轮。
- 远程下载复用同一流程；下载断流不留半截文件。
- 并发写入同一目录安全（同进程线程互斥 + 同机跨进程锁）。
- 保持现有返回目录、产物文件名、daemon HTTP 表面完全兼容。

## Non-Goals
- 不改变 Whisper 各后端本身的输出格式与文件名约定。
- 不做跨主机分布式锁、不做多副本事务；仅保证同机（同文件系统）原子替换语义。
- 不改变 daemon 作业目录（`job_root/ta-job-*`）的生命周期与 TTL 策略。
- 不引入新的第三方运行时依赖（文件锁、journal 均用标准库实现）。
- 不做产物内容级语义校验（如字幕时间码合法性审计）；校验限于集合完整性与文件完整性。

## Background & Context
- 现状本地流程（[api.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/api.py)）：后端写入匿名 `TemporaryDirectory`，随后逐个文件 `os.remove` 目标 + `shutil.move` 到 `output_dir`（约 L412-L430）；`embed=True` 时 `_embed_subtitles` 直接把 `out.mp4` 写进 `output_dir`（L434-L446）。任一步中断 → 新旧混杂 / 截断 mp4。
- 远程流程（[client.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/client.py)）：sync 与 async 两个客户端都把每个 artifact 以 `dest.open("wb")` **直接写最终文件名**（L159-L166、L259-L266），断流即半截文件；且不清理旧轮中本轮清单之外的受管文件。
- daemon 作业（[server_config.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/server_config.py) `JobStore`、[server_app.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/server_app.py)）：worker 调用同一个 `api.transcribe(output_dir=job.artifact_dir)`；上传文件存于该目录下的 `_input/` 子目录；`list_artifacts` 只列顶层普通文件，zip 端点内联同类枚举。
- 受管产物名固定为：`out.txt`、`out.srt`、`out.vtt`、`out.json`、`out.tsv`（openai-whisper 后端）、`speaker.json`、`out.mp4`（由各后端写出点与 api 重命名逻辑确认）。
- `transcribe()` 返回 `os.path.abspath(output_dir)`；产物名 `out.*` 是 README 与测试固化的公开契约。

## Functional Requirements

- **FR-1 暂存区**：每轮在 `output_dir` 内创建同文件系统的隐藏暂存目录（`.` 前缀），后端产物与烧录产物全部先落暂存区；运行期间最终目录顶层不出现本轮任何新产物的半成品。
- **FR-2 重命名归一在暂存内完成**：现有 `chop_double_extension`、`speaker.json` 透传逻辑不变，只是目标改为暂存区。
- **FR-3 完整性校验（本地）**：发布前校验暂存集合——`out.srt` 必须存在且非空；每个受管文件必须是普通文件且大小 > 0；不存在受管名单之外的文件（归一逻辑只产出名单内名称）。校验失败 → 本轮失败、暂存清除、旧轮不动。
- **FR-4 烧录在暂存完成**：`embed=True` 时 ffmpeg 输出到暂存区的 `out.mp4`；ffmpeg 失败（`CalledProcessError`）或产物为空 → 不上发布，异常照旧抛出，最终目录（含旧 `out.mp4`）保持原状。
- **FR-5 原子发布**：校验通过后在锁内把暂存文件逐个 `os.replace` 到最终目录（单文件原子替换），随后删除本轮清单之外的受管旧文件；发布完成后删除提交日志与暂存区。
- **FR-6 提交日志与恢复**：任何对最终目录的修改前，先在暂存区写就 journal（新一轮文件清单 + 待删除旧受管文件清单 + round id），`os.replace` 原子落到 `output_dir` 内的固定日志路径并 fsync；恢复入口扫描：存在 pending journal 且暂存存活 → 按日志前滚完成发布；无 journal → 清扫所有遗留暂存目录（旧完整轮自然保留）。
- **FR-7 远程复用**：sync / async 客户端下载到同一结构的暂存区，按 daemon 清单校验（清单集合一致、每个文件非空、服务端提供 `Content-Length` 时字节数一致），随后调用与本地完全相同的发布/恢复函数；断流或校验失败 → 清除暂存、抛 `RemoteTranscriberError`、最终目录旧轮完整。
- **FR-8 并发安全**：按 `resolve()` 后的 output_dir 提供进程内互斥锁（线程），并以 `mkdir` 原子锁目录（内含 host/pid/时间戳，可回收死进程的陈旧锁）提供同机跨进程互斥；同目录发布串行化，不同目录互不阻塞。
- **FR-9 内部条目对外隐藏**：daemon 的 artifact 列表、zip 打包、单文件 GET 不得暴露 `.staging-*`、journal、锁目录等内部条目；对隐藏文件名的直接 GET 返回 404。
- **FR-10 失败清扫**：本轮任何阶段失败（后端异常、校验失败、烧录失败、下载断流）均清除本轮暂存目录；最终目录维持进入本轮前的完整状态。

## Non-Functional Requirements

- **NFR-1 兼容性**：`transcribe()` 函数签名、返回的绝对目录路径、产物文件名全部不变；daemon `/v1/jobs/*` 请求/响应形态不变；现有测试不回归。
- **NFR-2 零新依赖**：仅用标准库（`os`/`shutil`/`json`/`threading`/`uuid`/`fsync` 等）。
- **NFR-3 同文件系统**：暂存目录必须位于最终目录之内，保证 `os.replace` 原子且不跨设备；跨设备场景由"暂存在目标目录内"天然规避。
- **NFR-4 崩溃面最小**：journal 写入带目录 fsync；发布顺序为先替换新文件、后删除旧文件，使恢复总能沿 journal 单向前滚。
- **NFR-5 可测试性**：核心机制为不依赖模型/GPU/网络的纯文件操作，可用伪产物做快速单测；断流与并发通过可注入的流/线程模拟。
- **NFR-6 平台**：macOS / Linux 为主要验证目标；Windows 上锁与替换逻辑退化为"尽力而为"（不破坏既有平台支持）。

## Constraints
- **Technical**: Python 3 标准库；`os.replace` / `mkdir(EEXIST)` 原子原语；不动后端 iso-env 与 ffmpeg 调用形态（`shell=False`、参数列表不变）。
- **Business**: 不改变 CLI 参数、环境变量、HTTP API 与 README 中声明的产物契约。
- **Dependencies**: 无新增第三方依赖；测试沿用 pytest / httpx TestClient / starlette 既有设施。

## Assumptions
- "一轮"= 一次 `transcribe()` 调用或一次远程作业下载所对应的完整产物集合。
- 用户文件指 `output_dir` 中不属于受管名单（FR 背景列出的 7 个名称）、也不属于本机制内部 `.` 前缀条目的任何文件/目录（例如 daemon 的 `_input/`、用户自备笔记）。
- 同一 `output_dir` 的并发写只需要同机同文件系统保证；跨主机共享盘不在范围内。
- daemon 每个 job 使用全新的 artifact 目录，因此旧产物清理主要影响本地复用目录与远程复用目录；daemon 路径经由同一 api 自动获得原子性且行为不回归。

## Acceptance Criteria

### AC-1: 运行中新产物只存在于隐藏暂存区
- **Type**: `rule`
- **Given**: 一轮转录正在后端写出阶段
- **When**: 枚举最终 `output_dir` 顶层条目
- **Then**: 不出现本轮任何新的 `out.*` / `speaker.json` 半成品；可见的受管文件只可能是上一完整轮；本轮内容仅存在于 `.` 前缀暂存目录内
- **Pass Condition**: 后端执行期间对最终目录快照断言：无本轮部分新文件；暂存目录为 `.` 前缀且位于最终目录内
- **Evidence**: `tests/test_publish.py` 与 api 集成测试（注入会在写产物后、发布前检查目录的伪后端）

### AC-2: 校验失败阻止发布且旧轮完整
- **Type**: `rule`
- **Given**: 最终目录存在完整旧轮；暂存产物缺少 `out.srt` 或某受管文件为空
- **When**: 触发发布前校验
- **Then**: 抛出明确异常、本轮失败；最终目录文件与旧轮逐字节一致；暂存目录被清除
- **Pass Condition**: 缺 `out.srt` 与空文件两个用例均：发布未发生、旧轮内容不变、无暂存残留
- **Evidence**: `tests/test_publish.py`

### AC-3: 字幕烧录在暂存完成后才发布
- **Type**: `rule`
- **Given**: `embed=True` 且最终目录已有上一轮 `out.mp4`
- **When**: ffmpeg 成功 → 暂存 `out.mp4` 非空校验通过后随整轮发布；ffmpeg 失败（模拟 `CalledProcessError`）
- **Then**: 成功时最终 `out.mp4` 来自暂存且完整；失败时异常向上传播、最终目录旧 `out.mp4` 完好、暂存清除
- **Pass Condition**: 两条路径分别断言最终 mp4 内容/时间戳与旧轮保持
- **Evidence**: api 集成测试（mock 静态 ffmpeg 调用）

### AC-4: 发布后只含完整新一轮的受管集合
- **Type**: `rule`
- **Given**: 暂存校验通过的新一轮（如含 `speaker.json`、不含 `out.mp4`）
- **When**: 发布完成
- **Then**: 最终目录受管文件集合恰为新一轮集合；无 `.part`/临时名/journal/暂存残留；`transcribe()` 仍返回同一绝对路径，文件名仍为 `out.txt/out.srt/out.vtt/out.json[/speaker.json][/out.mp4]`
- **Pass Condition**: 集合精确相等 + 无内部条目残留 + 返回值断言
- **Evidence**: `tests/test_publish.py`、api 集成测试、现有 `test_client.py` / `test_server_app.py` 通过

### AC-5: 本轮不再生成的旧受管产物被移除
- **Type**: `rule`
- **Given**: 旧轮含 `out.mp4` 与 `speaker.json`，新一轮二者均不产出
- **When**: 新一轮发布完成
- **Then**: `out.mp4`、`speaker.json` 从最终目录消失，其余新轮产物就位
- **Pass Condition**: 前后目录清单断言
- **Evidence**: `tests/test_publish.py`

### AC-6: 用户文件与非受管目录始终保留
- **Type**: `rule`
- **Given**: 最终目录含用户文件（如 `notes.txt`）与子目录（如 `_input/`）
- **When**: 任意轮次发布（含删除旧受管产物的轮次）
- **Then**: 用户文件与子目录内容逐字节不变
- **Pass Condition**: 发布前后用户条目路径与内容哈希一致
- **Evidence**: `tests/test_publish.py`

### AC-7: 中断恢复后只见完整旧轮或完整新轮
- **Type**: `rule`
- **Given**: 在三个时点模拟进程死亡：(a) 暂存写入中、(b) journal 已落、发布替换进行到一半、(c) 新文件已替换但旧文件待删
- **When**: 下次进入时执行恢复
- **Then**: (a) 清扫遗留暂存，目录=完整旧轮；(b)(c) 凭 journal 与存活暂存前滚，目录=完整新轮且旧产物删除到位；任何情况下不存在截断文件或新旧混杂
- **Pass Condition**: 三时点用例恢复后，受管文件集合与内容分别完整匹配旧轮或新轮，且无暂存/journal 残留
- **Evidence**: `tests/test_publish.py`（手工构造各崩溃现场并调用恢复入口）

### AC-8: 远程下载断流不污染最终目录并重试成功
- **Type**: `rule`
- **Given**: 最终目录已有完整旧轮；sync 与 async 客户端分别下载新一轮
- **When**: 在某个 artifact 传输中段注入断流/异常（另一用例：声明字节数与实际不符）
- **Then**: 抛出 `RemoteTranscriberError`；最终目录无该文件的半截版本、旧轮完整；暂存目录清除；随后重试成功得到完整新轮
- **Pass Condition**: 断流后清单=旧轮且无内部残留；重试后清单=新轮；sync 与 async 各覆盖
- **Evidence**: `tests/test_client.py` 新增用例（可注入的中断流）

### AC-9: 并发写入同一目录安全
- **Type**: `rule`
- **Given**: 多线程同时对同一 `output_dir` 发布不同轮次（另组：多个不同目录并发）
- **When**: 全部线程结束
- **Then**: 同目录最终状态等于某一完整轮（不混杂、不缺文件、无暂存残留、用户文件保留）；不同目录组各自完整；过程中无异常逃逸
- **Pass Condition**: N 线程 × M 轮压力用例后逐目录断言集合完整性；另含跨进程锁获取/陈旧锁回收的单测
- **Evidence**: `tests/test_publish.py`

### AC-10: daemon 不暴露内部暂存条目且现有 HTTP 契约不回归
- **Type**: `rule`
- **Given**: 作业运行中/完成后 artifact 目录含 `.staging-*`、journal、`_input/`
- **When**: 调用 job 状态、单文件 GET、artifacts.zip
- **Then**: artifacts 清单与 zip 内只有正式产物名；对 `.staging-*`/journal 隐藏名的 GET 返回 404；现有 `test_server_app.py` 全部通过
- **Pass Condition**: 断言清单/zip 条目名集合与隐藏名 GET 404
- **Evidence**: `tests/test_server_app.py` 新增/既有用例

### AC-11: 共享机制单一实现，本地与远程一致复用
- **Type**: `rubric`
- **Dimension**: 机制内聚与复用度
- **Scale**: 1-5
- **Anchors**: 1 = 本地/远程各自复制一套暂存发布逻辑；3 = 抽出公共函数但两处仍有显著分叉；5 = 本地 api、sync 客户端、async 客户端、恢复路径全部收敛到同一模块的同一组原语，无重复实现
- **Pass Threshold**: >= 4
- **Evidence**: 代码结构审查 + 调用点清单（评审阶段核对）

### AC-12: 变更的侵入性与兼容性
- **Type**: `rubric`
- **Dimension**: 对既有代码/契约的最小侵入
- **Scale**: 1-5
- **Anchors**: 1 = 改动公开签名/HTTP 形态/产物路径或大面积重写；3 = 行为兼容但存在可避免的公开面变动；5 = 公开签名、返回路径、产物名、HTTP 表面零变化，改动集中在新模块与少量集成点
- **Pass Threshold**: >= 4
- **Evidence**: diff 审查 + 全量测试套件结果

## Open Questions
- 无（关键决策按 Assumptions 执行：校验只做集合/文件完整性；暂存位于最终目录内；同机同文件系统为原子性边界）。
