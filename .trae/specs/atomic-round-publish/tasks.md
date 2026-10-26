# 转录轮次原子发布（暂存-校验-发布）- 实现计划

## Task 1: 新建 publish.py —— 受管产物常量、目录锁与恢复清扫
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 新建 `src/transcribe_anything/publish.py`，定义受管产物名单 `MANAGED_ARTIFACTS = {"out.txt","out.srt","out.vtt","out.json","out.tsv","speaker.json","out.mp4"}`（实测 openai-whisper CLI 默认另产 `out.tsv`，已并入名单，spec 背景同步为 7 个名称）、`REQUIRED_ARTIFACTS = {"out.srt"}`、内部条目前缀/常量（暂存目录前缀 `.staging-`、journal 文件名 `.transcribe-round.json`、锁目录名 `.transcribe-round.lock`）。
  - 提供 `is_managed_artifact(name)`、`is_internal_name(name)`（"." 前缀即内部条目，统一覆盖 staging/journal/lock）、`list_managed_artifacts(d)`、`list_staging_dirs(d)` 枚举助手。用户条目的判定收敛为"非受管且非内部"，无需单独 `list_user_entries`。
  - 实现按 resolved 目录键控的进程内 `threading.RLock` 注册表（`_RoundGuard`，可重入），以及 `mkdir` 原子锁目录（owner.json 写 host/pid/创建时间；POSIX 用 `os.kill(pid,0)`、Windows 用 ctypes OpenProcess 判定持锁进程存活，死锁可回收——`_reap_lock_dir` 先 rename 成唯一 tombstone 再 rmtree，保证多个回收者只有一个胜出；无法判定时按存活处理不抢占；owner 文件缺失但锁目录未到 `STALE_LOCK_AGE_SECONDS=60s` 不回收，覆盖 mkdir 与写 owner 之间的窗口）。
  - 实现 `recover_rounds(output_dir)`：锁内 `_recover_locked`——pending journal 且暂存存活则 `_roll_forward` 前滚；journal 在但暂存已失说明安装已完成（暂存只在全部替换/删除后才移除），移除 journal 收尾；无 journal 时清扫全部遗留 `.staging-*`。
  - 提供 `staged_round()` 上下文 + `recover_rounds()` 显式入口；锁等待带超时（`LOCK_POLL_SECONDS=0.05` 轮询）并抛 `RoundLockTimeout`。
- **Acceptance Criteria Addressed**: AC-7, AC-9
- **Test Requirements**:
  - `rule` TR-1.1: 连续两次获取同目录锁，第二次在第一次释放后成功；不同目录可并发持有；证据：pytest 断言。
  - `rule` TR-1.2: 锁目录持有者进程不存在时（伪造 pid）锁可被回收；证据：单测构造陈旧锁目录后成功获取。
  - `rule` TR-1.3: 无 journal 时遗留的多个 `.staging-*` 被清扫且受管文件/用户文件不动；证据：恢复前后清单与内容哈希断言。
  - `rule` TR-1.4: 受管/内部/用户条目分类正确（含 `_input/`、`notes.txt`、`.staging-x`）；证据：参数化单测。
- **Completion Evidence**:
  - 模块：[publish.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/publish.py)（纯 stdlib，零新依赖，符合 NFR-2）。
  - TR-1.1 → `test_same_dir_lock_serializes_threads`、`test_distinct_dirs_do_not_block_each_other`、`test_lock_timeout_raises`。
  - TR-1.2 → `test_stale_lock_dir_with_dead_pid_is_reaped`；跨进程版见 `test_cross_process_lock_blocks_then_reaps`。
  - TR-1.3 → `test_recover_sweeps_orphan_staging_and_keeps_round`。
  - TR-1.4 → 参数化 `test_name_classification`（12 例，覆盖 7 受管名 + notes.txt + _input + 3 内部名）。

## Task 2: publish.py —— 暂存、校验、journal 提交与原子发布
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 实现 `staged_round(output_dir)` 上下文管理器：进入时 `mkdir(parents=True, exist_ok=True)` 最终目录、先 `_recover_locked`，再创建唯一暂存目录（uuid 后缀，位于最终目录内，同文件系统）；返回 `StagedRound(output_dir, path, round_id, committed, _guard)`。退出时：成功提交或存在 journal（崩溃可恢复）之外的情形一律 rmtree 暂存——`except BaseException` 分支仅在"未提交且 journal 不存在"时清扫，有 journal 则保留暂存供下轮前滚。
  - 归一搬运不放在 publish 模块：api 直接在 `stage.path` 内用既有 `chop_double_extension` 逻辑搬运（见 Task 3），publish 只暴露 `stage.path`。
  - 校验函数 `validate_staging(staging)`：`out.srt` 必须存在且 `stat().st_size > 0`；暂存内每个条目（journal 名除外）都必须是受管普通文件且非空；三类违例分别抛 `ArtifactValidationError`（`unexpected staging entry` / `artifact is empty` / `missing required artifacts`）。
  - 发布 `commit_round(staging)`：锁内算 stale=旧受管-新集合 → `_write_pending_journal`（journal 先写暂存并 fsync 文件+暂存目录，再 `os.replace` 到最终目录固定位置并 fsync 最终目录）→ 逐个 `os.replace` 新文件（先替换）→ 删除 stale（后删除）→ fsync 最终目录 → rmtree 暂存 → 最后移除 journal 并 fsync。"journal 不存在"即已提交标记（`stage.committed=True` 最后置位）。
  - 前滚恢复复用同一安装清单：`_roll_forward` 按 journal 的 new/stale 从存活暂存补齐替换与删除再收尾；暂存已失则只收敛 stale + 删 journal。
  - 失败清扫：journal 落盘后任一步异常现场保持可恢复（journal + 暂存），由下次进入前滚；journal 落盘前异常由上下文退出清扫暂存，旧轮不动。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-4, AC-5, AC-6, AC-7
- **Test Requirements**:
  - `rule` TR-2.1: 校验拒绝缺 `out.srt`、空字节文件、未知名文件；拒绝时最终目录（旧轮）逐字节不变、暂存清除；证据：三类用例断言。
  - `rule` TR-2.2: 正常提交后受管集合恰为新轮集合，无 journal/暂存/临时名残留；证据：目录快照断言。
  - `rule` TR-2.3: 旧轮 `out.mp4`/`speaker.json` 而新轮不含时，提交后二者被删；用户文件与子目录不变；证据：哈希比对。
  - `rule` TR-2.4: 手工构造崩溃现场（仅暂存 / journal+部分替换 / journal+替换完未删 stale），执行恢复后分别为完整旧轮或完整新轮；证据：三时点恢复断言。
  - `rule` TR-2.5: journal 写入与安装过程使用 `os.replace`，新文件不以临时名长期存在于最终目录；证据：代码审查 + 发布中途快照断言最终目录无 `.part` 名称。
- **Completion Evidence**:
  - TR-2.1 → 参数化 `test_validation_failures_leave_old_round_intact`（missing required / empty / unexpected 三例，均断言旧轮快照不变、暂存被清扫）。
  - TR-2.2 → `test_commit_replaces_removes_stale_and_preserves_user_entries`、`test_commit_overwrites_existing_managed_files`（提交后仅新轮受管集合，无 journal/staging 残留）。
  - TR-2.3 → 同上用例断言 out.mp4/speaker.json 删除；`test_openai_whisper_tsv_round_then_core_round_removes_tsv` 额外验证 tsv 轮次→核心轮次时 out.tsv 被移除。
  - AC-6 用户条目 → 同用例对 `notes.txt` 与 `_input/upload.bin` 做前后哈希比对（逐字节不变）。
  - TR-2.4 / AC-7（四个崩溃时点，覆盖且超出三时点要求）→ `test_recovers_pre_commit_crash_to_old_round`（a：暂存写入期，无 journal → 清扫，=旧轮）、`test_recovers_mid_install_rolls_forward_to_new_round`（b：journal 落盘+替换到一半 → 前滚=新轮）、`test_recovers_after_replaces_before_stale_delete`（c：新文件已替、stale 未删 → 前滚删 stale=新轮）、`test_recovers_journal_without_staging_closes_round`（journal 在暂存失 = 安装已完成，收敛删 journal）。
  - TR-2.5 → journal 经 `os.replace`（`_write_pending_journal`）、新文件经 `os.replace`（`commit_round`）；最终目录除固定 journal 名外不出现临时名，由 `test_new_products_are_invisible_until_commit` 发布前快照佐证。

## Task 3: api.py 集成 —— 后端写入暂存、烧录入暂存、整轮发布
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - 将 `transcribe()` 中后端输出目录从匿名 `TemporaryDirectory` 改为 `staged_round` 暂存目录（`tmpdir = str(stage.path)`，同文件系统）；`chop_double_extension` 归一与 `speaker.json` 透传的搬运目标改为暂存区内；临时 wav 仍在系统 temp（既有清理职责不变）。
  - 缺 `out.srt` 的保护改由发布前 `validate_staging` 承担（`assert srt_file is not None` 保留，空 srt 由校验拦截），缺 srt 即失败且旧轮不动。
  - `embed=True`：ffmpeg 输出改为暂存区 `out.mp4`；`CalledProcessError` 路径行为不变（警告并抛出），但最终目录旧 `out.mp4` 不被触碰，异常退出时暂存由上下文清扫。
  - 成功时 `commit_round(stage)`，返回值仍为 `os.path.abspath(output_dir)`（abspath 计算移到 with 块之外）；失败时最终目录维持旧轮完整。
  - 关键正确性修复：归一目标改到暂存后，`speaker.json` 的源路径可能等于目标路径，旧的"先 `os.remove(outfile)` 再 move"会删掉自己——加 `os.path.abspath(file) != os.path.abspath(outfile)` 守卫，自名文件原地保留。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-3, AC-4, AC-12
- **Test Requirements**:
  - `rule` TR-3.1: 用 monkeypatch 桩掉 `fetch_audio`/设备/后端（后端直接在给定 output_dir 写四份产物）跑通 `transcribe()`，最终目录恰含新轮四件套且返回绝对路径不变；证据：api 集成测试。
  - `rule` TR-3.2: 在后端写产物后、发布前钩子内断言最终目录无本轮新文件（AC-1）；证据：同测试内快照。
  - `rule` TR-3.3: mock ffmpeg 失败时，旧轮（含旧 out.mp4）完好且异常传播；成功时 out.mp4 经暂存发布；证据：两个 api 用例。
  - `rule` TR-3.4: 第二轮不产出 speaker.json/out.mp4 时旧文件消失；证据：api 两轮集成用例。
- **Completion Evidence**:
  - 改动：[api.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/api.py) diff（`+34/-12` 量级），`tempfile` 仍被 L122/L167 使用，无未用导入。
  - TR-3.1 → `test_transcribe_publishes_complete_round_and_keeps_return_path`（桩后端写 `clip.wav.{txt,srt,vtt,json}`，断言最终四件套 + 返回绝对路径不变）。
  - TR-3.2 / AC-1 → `test_new_products_are_invisible_until_commit`（提交前快照最终目录无本轮新文件，仅 `.staging-*` 内可见）。
  - AC-2 → `test_backend_failure_leaves_previous_round_intact`（后端抛错后旧轮逐字节不变、暂存清除）。
  - TR-3.3 / AC-3 → `test_embed_success_publishes_burned_mp4_from_staging`、`test_embed_failure_keeps_old_mp4_intact`（桩 ffmpeg 返回码 1：警告照常 `warnings.warn` 并抛出，旧 mp4 内容/时间戳不变）。
  - TR-3.4 → `test_second_round_without_optional_files_removes_them`。

## Task 4: client.py 集成 —— sync 与 async 下载复用暂存-校验-发布
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2
- **Description**:
  - sync `transcribe_remote` 与 `transcribe_remote_async`：输出目录解析逻辑保持不变；进入 `staged_round(out_path)`（含进入即恢复），逐 artifact 下载到暂存区，先写 `name + ".part"` 再 `os.replace` 到最终受管名，全部成功后 `_publish_downloaded_round`。
  - 下载校验：`_publish_downloaded_round` 比对暂存文件名集合与 job 清单必须完全相等（缺/多均报 mismatch），再调与本地完全相同的 `validate_staging` + `commit_round`；`_verify_downloaded` 拒绝空文件，并在响应带 `Content-Length` 时校验累计字节相等，否则 `RemoteTranscriberError`。
  - 断流边界：`except (httpx.HTTPError, OSError, RuntimeError)` 覆盖**整个 stream 上下文含退出时**（TestClient/ASGI 门户在流上下文退出时才抛服务端生成器里的 RuntimeError），`RemoteTranscriberError` 先行 re-raise；失败后 `.part` 由暂存上下文随目录一起 rmtree。
  - 消除双份落盘/发布代码：共享 `_expected_content_length`/`_verify_downloaded`/`_publish_downloaded_round`，sync/async 仅各留 `_download_artifact_sync`/`_download_artifact_async` 一个 httpx IO 外壳，主流程结构对称。
  - 任何失败：暂存清除、最终目录保持旧轮；对外异常类型仍为 `RemoteTranscriberError`。
- **Acceptance Criteria Addressed**: AC-8, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-4.1: 正常远程流程（sync/async）最终产物与返回路径同现状；证据：既有 `test_client.py` 全部通过。
  - `rule` TR-4.2: 在第二个 artifact 传输中段注入异常（可注入的中断流 transport），断言抛错、最终目录=旧轮、无 `.part`/暂存残留；证据：sync+async 各一用例。
  - `rule` TR-4.3: Content-Length 大于实际字节（截断响应）时拒绝发布、旧轮不动；证据：参数化用例。
  - `rule` TR-4.4: 断流后重试成功，最终目录=完整新轮；证据：同用例重试断言。
  - `rule` TR-4.5: 旧轮含 out.mp4 而新清单不含时，发布后旧 mp4 删除；证据：客户端用例。
- **Completion Evidence**:
  - 改动：[client.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/client.py)（新增共享助手 + 两个 IO 外壳；sync/async 主流程均改为 `with staged_round(...)` → 下载 → `_publish_downloaded_round`；删除两处旧 `out_path.mkdir` 与直接 `dest.open("wb")`）。
  - 自建 Starlette ASGI 小应用（job j1，4 个核心 artifact），两种故障模式：`raise`（生成器中途 RuntimeError）与 `trunc`（Content-Length 撒谎）。
  - TR-4.2/TR-4.4/AC-8 → 参数化 `test_sync_interrupted_download_preserves_old_round_and_retries[raise/trunc]` 与 async 同名两例：首次断流抛 `RemoteTranscriberError`、最终目录=旧轮、无 `.part`/`.staging-*` 残留，重试后=完整新轮。
  - TR-4.3 单元化 → `test_verify_downloaded_rejects_empty_and_truncated`（空文件 + 字节数不符）。
  - TR-4.5 → `test_sync_remote_removes_stale_managed_files`（旧轮 out.mp4 在新清单不含时被删，用户文件保留）。
  - TR-4.1 → 全量套件含既有 `tests/test_client.py`，302 passed 无回归。

## Task 5: daemon 表面隐藏内部条目
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 2
- **Description**:
  - `JobStore.list_artifacts` 枚举时用 `not is_internal_name(p.name)` 排除 "." 前缀条目（继续只列顶层普通文件；`_input/` 本来就是目录枚举不到）。
  - `server_app.get_artifacts_zip` 改为复用 `store.list_artifacts(job)`，zip 内不含内部文件；`get_artifact` 在既有 traversal 检查之后对 `is_internal_name(filename)` 返回 404。
- **Acceptance Criteria Addressed**: AC-10, AC-12
- **Test Requirements**:
  - `rule` TR-5.1: 在已完成作业目录人为放置 `.staging-x/` 与 journal 后，job 状态 artifacts 与 zip 条目均不含内部名；证据：server_app 测试。
  - `rule` TR-5.2: GET 隐藏名返回 404，GET 正式产物 200 不变；证据：路由用例。
  - `rule` TR-5.3: 既有 `test_server_app.py` 全绿；证据：pytest 输出。
- **Completion Evidence**:
  - 改动：[server_config.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/server_config.py)（`list_artifacts` 加内部名过滤）、[server_app.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/server_app.py)（GET 隐藏名 404；zip 改走 `store.list_artifacts`）。
  - TR-5.1/TR-5.2 → 新增 `test_internal_round_entries_never_exposed`（植入 `.staging-leftover/`、journal、lock 目录后，artifacts 清单与 zip 条目精确等于四件套；隐藏名 GET 404；正式产物 GET 200 不变）。
  - TR-5.3 → `tests/test_server_app.py` 40 例全绿。

## Task 6: 并发写入压力测试
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 2, Task 3
- **Description**:
  - 在 `tests/test_publish.py` 增加：N 个线程对同一 output_dir 并发提交不同轮次（每轮文件内容带轮次标识，部分轮含/不含 speaker.json、out.mp4），断言结束后目录等于某一完整轮（所有受管文件内容轮次一致、无缺失/残留/用户文件损坏）。
  - 多目录并发互不阻塞/串扰用例。
  - 跨进程锁：子进程方式（`subprocess` 跑小脚本持锁）验证同目录第二次获取阻塞/超时；持锁进程退出/被杀后锁可回收。
- **Acceptance Criteria Addressed**: AC-9
- **Test Requirements**:
  - `rule` TR-6.1: 同目录 8 线程 × 每线程 5 轮，结束后受管集合与内容一致且无内部残留；证据：压力用例通过。
  - `rule` TR-6.2: 4 个不同目录并发各自完整；证据：用例断言。
  - `rule` TR-6.3: 活进程持锁导致跨进程获取超时，持锁进程退出后可获取；证据：子进程用例。
- **Completion Evidence**:
  - TR-6.1 → `test_concurrent_rounds_same_directory`（8 线程 × 5 轮同目录；每轮含轮次标识、可选 speaker.json/out.mp4；结束后全部受管文件带同一 round 标记、集合完整、无内部残留、用户文件完好；过程无异常逃逸）。
  - TR-6.2 → `test_concurrent_rounds_distinct_directories`（4 目录并发各自收敛到完整一轮）。
  - TR-6.3 → `test_cross_process_lock_blocks_then_reaps`（子进程持锁时同目录第二次获取超时 `RoundLockTimeout`；子进程退出后锁可获取；另以伪造死 pid 验证被杀进程锁回收，见 TR-1.2）。

## Task 7: 全量验证与静态检查
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 3, Task 4, Task 5, Task 6
- **Description**:
  - 运行与改动相关的测试文件（publish/api/client/server_app）及全量可在本机执行的测试；运行仓库既有的静态检查（black/isort/ruff/mypy）。
  - 自查公开签名、返回路径、产物名、HTTP 表面零变化。
- **Acceptance Criteria Addressed**: AC-4, AC-10, AC-11, AC-12
- **Test Requirements**:
  - `rule` TR-7.1: 新增测试与受影响既有测试全部通过；证据：测试命令与输出摘要记录。
  - `rule` TR-7.2: lint 无新增告警；证据：lint 输出摘要。
  - `rubric` TR-7.3: 机制复用度（AC-11 维度）；scale 1-5；anchors 同 spec；threshold >= 4；证据：调用点清单（api、client sync/async、recover 均引用 publish 模块）。
  - `rubric` TR-7.4: 侵入性（AC-12 维度）；scale 1-5；anchors 同 spec；threshold >= 4；证据：diff 范围与签名/契约核对。
- **Completion Evidence**:
  - TR-7.1：目标四模块 `uv run pytest tests/test_publish.py tests/test_api_staging.py tests/test_client_resilience.py tests/test_server_app.py` → **82 passed**；全量 `uv run pytest tests/ --ignore=tests/test_insanely_fast_whisper_mlx.py --ignore=tests/test_mlx_timestamps.py` → **302 passed, 9 skipped**（与改动前基线一致；6 个 MLX 失败为预存在环境问题：iso-env 源码构建 tiktoken 0.3.3 需 Rust 编译器，与本次无关）。
  - TR-7.2：`black --check` 9 个改动文件全部 unchanged；`mypy` 5 个源文件 `Success: no issues found`；`ruff` 新文件 publish.py 与 3 个新测试文件、api.py 零告警，client/server_app/server_config/test_server_app 相对 HEAD 的规则码直方图完全一致（UP045/F401/I001/PIE810/ASYNC230 等均为预存在告警，本次新增的 4 处 `Optional[int]` 已改为 `int | None`，未新增任何告警）。
  - TR-7.3（自评 5/5）：本地 [api.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/api.py)、sync 与 async [client.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/client.py)、进入即恢复的 `staged_round`/显式 `recover_rounds` 全部 import 并复用同一个 [publish.py](file:///Users/kkcarrot/swe-project/transcribe-anything_fork2/src/transcribe_anything/publish.py) 的 `staged_round`/`validate_staging`/`commit_round`/`is_internal_name`；无重复实现（对应 anchor 5 描述）。
  - TR-7.4（自评 5/5）：`transcribe()`/`transcribe_remote()`/`transcribe_remote_async()` 签名与返回绝对路径不变；产物名仍为 out.txt/srt/vtt/json/tsv + 可选 speaker.json/out.mp4；daemon 路由与请求/响应形态不变（仅新增"隐藏名 404"这一安全收紧）；改动集中在 1 个新模块 + 4 处集成点（api +34、client +110、server_app +10、server_config +6 量级）与新增测试，无公开面变动。

## Review Follow-ups（独立评审后，详见 review.md）
- **F1（low，已修复并复审 resolved）**：同线程重入（同一事件循环两个 asyncio 下载到同一目录）会让内层进入恢复时清扫外层存活暂存。修复：`_RoundGuard.reentered` 门控——`staged_round`/`recover_rounds` 仅外层持有者执行 `_recover_locked`；并把 client 发布期 `OSError`（含 iterdir）统一包成 `RemoteTranscriberError`。
- 新增 3 个回归测试（目标模块由 82 增至 **85 passed**，全量 **305 passed, 9 skipped**）：`test_nested_same_thread_round_preserves_outer_staging`、`test_concurrent_async_downloads_same_directory`（`_FirstArtifactBarrier` 确定性真交错，非 sleep 计时）、`test_publish_failure_is_surfaced_as_remote_error`。两并发/嵌套测试经"强制 reentered=False"反事实插件验证旧行为必失败、修复后通过。
- F2/F3/F4（损坏 journal 残留 / 回收 tombstone 残留 / 外力删失暂存文件）为仅外部损坏可触发、被 spec Non-Goals 排除的 informational 项，记录于 review.md，不改动。
- **最终评审 Verdict：pass**（AC-11/AC-12 均 5/5）。
