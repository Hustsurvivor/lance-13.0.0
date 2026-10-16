# Lance Lineitem 元数据性能实验

## Lineitem 数据集实验

`write_lineitem.rs` 流式读取 `data/lineitem.tbl`，每批处理 25 条记录。每批生成 5 个数据文件，每个文件 5 条记录，并且只提交一个版本：第一批创建数据集，后续批次追加数据。末尾不足一批时仍然提交，不补齐也不丢弃记录；只有最后一个文件可能不足 5 条。

写入程序拒绝使用已经存在的目标目录。重复实验时，请通过 `--dataset` 指定新目录，避免重复导入。输入格式错误时会终止，并提供行号或批次及列的信息；此前已经提交的批次仍然可见，程序不会自动回滚或断点续写。

16 列使用标准的 `l_` 前缀列名：键和行号为 Int64，数量、价格、折扣和税率为 Decimal128(15,2)，日期为 Date32，其余字段为 Utf8。行末可以有或没有 `|`。文本解析与 Arrow RecordBatch 构造均在写入计时区间之外，每次只缓冲 25 条输入记录。每次提交后都会核对 Manifest 中新增文件数、物理行数和版本号。

`scan_lineitem.rs` 每轮使用新的 Session，测量 Dataset 打开和扫描规划。`--version` 指定历史版本，`--filter` 指定 SQL 过滤条件，标量索引处于禁用状态。默认只准备扫描计划，不读取实际数据。

添加 `--execute` 后，程序会在元数据采集结束后重新构建可执行数据流，并流式读取所有匹配批次。行数和实际扫描耗时输出到标准错误流，不计入元数据 JSONL。实验不会清空操作系统缓存。元数据总耗时仍然采用下文定义的埋点区间并集，而不是整个规划过程的全部耗时。

```powershell
$docker = 'C:\Users\lgn\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe'
& $docker compose restart lance-dev
& $docker compose exec -T -e LANCE_METADATA_METRICS=1 lance-dev cargo run -p lance-examples --example write_lineitem
& $docker compose exec -T -e LANCE_METADATA_METRICS=1 lance-dev cargo run -p lance-examples --example scan_lineitem -- --iterations 5 --execute
& $docker compose exec -T -e LANCE_METADATA_METRICS=1 lance-dev cargo run -p lance-examples --example scan_lineitem -- --version 1 --iterations 1 --execute --output /workspace/source/damlake_test/results/scan_lineitem_v1.jsonl
& $docker compose exec -T -e LANCE_METADATA_METRICS=1 lance-dev cargo run -p lance-examples --example scan_lineitem -- --filter 'l_quantity > 20' --iterations 1 --execute --output /workspace/source/damlake_test/results/scan_lineitem_filter.jsonl
& $docker compose exec -T lance-dev cargo test -p lance-examples --example write_lineitem
```

默认数据集路径为 `data/lineitem.lance`，默认结果文件为 `results/write_lineitem.jsonl` 和 `results/scan_lineitem.jsonl`。两个程序都支持 `--dataset` 和 `--output`，写入程序还支持 `--input`。重复运行时，JSONL 采用追加模式。

设置 `LANCE_METADATA_METRICS=0`，或不设置该变量，会关闭性能采集和结果输出，但不会关闭实际读写操作。正式性能实验请将 `cargo run` 替换为 `cargo run --profile release-with-debug`；调试构建结果只用于功能验证。

本次提供的输入有 100 条记录，实际导入生成 20 个文件、4 个版本，每个版本新增 25 条记录和 5 个文件。最新版本扫描得到 100 条记录，版本 1 得到 25 条，`l_quantity > 20` 得到 69 条，均与输入一致。最新版本的 5 次元数据耗时均值为 6822.0762 us，范围为 6197.401 至 8776.042 us。

以上结果来自调试构建和 Windows 源码挂载目录，操作系统缓存未受控，因此不能直接与此前临时文件系统中的测试数据比较。原始记录位于 `results` 下的 4 个 `*lineitem*.jsonl` 文件。`data/` 和 `results/` 已加入仓库根目录的 `.gitignore`，保留本地文件但不提交到 Git。两个写入测试覆盖每 5 个文件提交的边界、末尾不满文件、行数保持以及非法输入拒绝。

本实验对 Lance 13.0.0 的表级元数据增加埋点，不修改文件格式、提交顺序或现有公共方法签名。已验证的范围为本地存储、无索引、当前格式的追加写入和读取规划。`metadata_total_us` 测量明确标记的元数据区间并集，不代表 Dataset 操作的所有工作。保存的 lineitem 测试结果来自调试构建，是正确性证据，不是可直接用于论文的性能结论。

## 源码调用链

以下路径均相对于仓库根目录。可通过 `rg -n 'metadata_metrics::phase' rust` 定位所有埋点。

读取最新版本和历史版本：

```text
rust/lance/src/dataset/builder.rs  DatasetBuilder::load -> load_by_uri
  CommitHandler::resolve_latest_location / resolve_version_location
  rust/lance/src/dataset.rs  Dataset::get_manifest -> load_manifest
    对象读取器 -> 最后一个块 -> protobuf -> Manifest::try_from
    rust/lance-table/src/format/manifest.rs  Schema 和 Fragment 转换
    内联索引描述、事务元数据、Schema 字典
  Dataset::checkout_manifest  Session、引用、Fragment 状态
  rust/lance/src/dataset/scanner.rs  Scanner::new -> create_plan
    create_filter_plan / plan_metadata_scan_range / scan_fragments
```

`Dataset::checkout_by_ref` 提供另一条版本切换路径，其中也单独测量版本解析。命中元数据缓存时可能跳过 Manifest 加载；结果中缺少某个阶段，表示该代码未执行，而不是测得了零耗时。

追加写入：

```text
Dataset::write
  rust/lance/src/dataset/write/insert.rs  InsertBuilder
    validate_write
    实际数据写入                                  [不计入]
    build_transaction
  rust/lance/src/dataset/write.rs
    V2WriterAdapter::finish: 描述信息组装          [计入]
    finish_file / writer.finish                   [不计入]
    do_write_fragments_impl: 挂接描述信息          [计入]
  rust/lance/src/io/commit.rs
    write_transaction_file
    load_and_sort_new_transactions
    prepare_attempt 和冲突检查
      conflict_resolver.rs  check_txn
      lance-table/src/transaction/manifest_build.rs  build_manifest_prepared
    rust/lance/src/dataset.rs  write_manifest_file
      CommitHandler::commit
        lance-table/src/io/commit.rs  write_manifest_file_to_path
    record_successful_commit: 缓存更新            [计入]
    自动清理                                      [不计入]
```

提交处理器负责版本发布，具体可能使用重命名或条件提交，取决于存储实现。外层提交计时包含其元数据操作，但不包含此前的数据写入。事务发现阶段读取表级事务和 Manifest 对象，包括冲突历史。异步 rebase 没有整体计时，因为其他操作类型的重放或调整过程可能读取索引或数据文件。

## 指标与统计边界

每个阶段的值均为 `{ "total_us": number, "calls": integer }`。耗时为包含子阶段的墙钟时间，包括计时范围内的异步等待和调度。

| 指标 | 对应函数与统计边界 |
| --- | --- |
| `version_resolution` | `DatasetBuilder::load_by_uri`、`Dataset::checkout_by_ref`：解析最新或指定版本及其 Manifest 位置，不包含 Manifest 加载 |
| `manifest_read` | `Dataset::load_manifest`、lance-table 中的 `read_manifest`：打开对象、获取大小、读取最后一个块、提取 Manifest 偏移；在解码前结束 |
| `manifest_deserialization` | `load_manifest` 的小 Manifest 路径、lance-table 中的 `decode_manifest`：protobuf 解码及表级结构转换 |
| `manifest_read_decode_remainder` | `load_manifest` 的大 Manifest 路径：现有 `read_struct` 合并执行剩余 Manifest IO 和解码 |
| `manifest_structure_conversion` | `Manifest::try_from`：protobuf 到 Rust Manifest 的结构转换，嵌套于解码阶段 |
| `schema_metadata_processing` | `Manifest::try_from`：表级 Schema 转换，是结构转换的子阶段 |
| `fragment_metadata_processing` | `Manifest::try_from`：Fragment 信息驻留与偏移组织；以及 `checkout_manifest` 中 Dataset 引用、缓存和 Fragment 状态的组装 |
| `manifest_auxiliary_metadata` | `load_manifest`：内联索引描述、事务缓存、Manifest 中的 Schema 字典，不包含索引内部内容 |
| `manifest_index_descriptors` | lance-table 中的 `read_manifest_indexes`：Manifest 的索引描述段，不包含索引结构读取或搜索 |
| `scan_metadata_planning` | `Scanner::new`、`create_filter_plan` 的同步部分、`scan_fragments`：Schema 与投影、谓词规划、Fragment 扫描节点构建；不包含 `scalar_index_info` 的异步等待或扫描执行 |
| `scan_metadata_range_planning` | `plan_metadata_scan_range`：该路径执行时，根据表级行数和 limit/offset 规划扫描范围 |
| `write_version_validation` | `InsertBuilder::validate_write`：数据集存在性、写入模式和 Schema 兼容性校验 |
| `fragment_metadata_generation` | V2 写入器的描述映射和最终描述组装，以及将完成的文件描述与行数挂接到 Fragment；不包含文件完成、实际写入和进度回调 |
| `transaction_construction` | `InsertBuilder::build_transaction`：构造追加或覆盖操作及事务 |
| `transaction_serialize_write` | `write_transaction_file`：事务对象序列化和持久化 |
| `transaction_discovery` | `load_and_sort_new_transactions`：发现、读取、排序期间新增的事务及最新 Manifest；包含嵌套的 Manifest 计时 |
| `conflict_detection` | `conflict_resolver.rs` 中 `TransactionRebase` 的 `check_txn`：同步操作冲突检查 |
| `manifest_construction` | `build_manifest_prepared`：构造下一版本的 Manifest、Schema、Fragment 和版本元数据 |
| `manifest_serialize_write` | `write_manifest_file_to_path`：从对象写入器创建到 Manifest 序列化及写入器关闭；嵌套于提交阶段 |
| `commit_metadata_processing` | Dataset 的 `write_manifest_file`：元数据特性与版本检查、时间戳和最大 Fragment ID 更新、提交处理器发布；包含 Manifest 写入 |
| `commit_cache_update` | `record_successful_commit`：填充事务、Manifest 和索引描述缓存，在自动清理前结束 |

没有单独设置 `manifest_location_resolution`，因为提交处理器的版本解析本身已经返回位置。序列化与持久化保持为合并阶段：现有写入器交错执行两者，并可能缓冲 IO，强行拆开可能错误归属缓冲写入耗时，或者要求改变业务行为。大 Manifest 的剩余读取和解码也明确保持为合并阶段。

实验没有假设存在通用的 Fragment min/max 谓词裁剪阶段。lineitem 的无索引过滤条件只在采集范围内规划，不执行实际过滤；添加 `--execute` 后的数据扫描在采集结束后执行。

索引搜索、数据文件 Footer/Page/Column 解析、编码、RecordBatch 构造和实际扫描执行均不在这些元数据区间内。将结论推广到一般索引或向量查询、旧版读取器、自定义提交处理器、远程存储以及大量使用字典的 Schema 前，需要分别验证统计边界。

## 采集方式与总耗时

`rust/lance-core/src/metadata_metrics.rs` 提供 `collect`、`phase` 和 `inherit`。每次启用的 `collect` 都有独立的 Tokio 任务局部上下文、`Instant` 起点，以及由互斥锁保护的计时区间数组。现有 Dataset 公共 API 无需增加上下文参数，也没有引入混合请求的全局计数器或后台输出任务。

对于区间 `[start_i, end_i)`，`metadata_total_us` 等于这些区间并集的持续时间。采集器先排序，再只累计超出此前已覆盖终点的部分，从而消除嵌套和并发重叠带来的重复统计。

不要直接求和 `phases.*.total_us` 作为总耗时：重复调用、嵌套转换和重叠等待使其成为包含子阶段的累计工作耗时，而不是去重后的墙钟时间。`end_to_end_us` 覆盖传入 future 的整个执行过程，因此追加写入的端到端耗时包含数据 IO，但 `metadata_total_us` 不包含。两者都不是 CPU 时间。

RAII 计时器会记录正常的提前返回和错误返回。任务取消或 panic 无法向调用方返回 JSON 记录。独立请求必须分别调用 `collect`。新建的 Tokio 任务不会自动继承任务局部上下文，需要在采集范围内通过 `inherit` 包装 future，并在结束采集前等待所有子任务完成。已完成记录不包含脱离当前操作的后台工作。

嵌套启用的采集使用独立上下文；应避免在已启用的采集内部嵌套禁用采集，因为后者仍在外层上下文中正常执行。

库不会在每次 API 调用时读取环境变量或写文件。实验驱动只读取一次 `LANCE_METADATA_METRICS`，其中 `1` 为启用，`0` 或未设置为关闭；再将开关传给 `collect`，并在计时结束后导出 JSONL。

没有采集上下文时，阶段计时只尝试查找任务局部变量，不分配计时对象或读取时钟。启用后会有时钟、Arc、锁和内存分配开销；父阶段也包含子阶段的埋点开销。结果没有扣除这些开销，启用和禁用埋点的对照实验应分别报告。

## 开发环境与验证

在仓库根目录运行上面的 PowerShell 命令。Docker 环境编译挂载的源码，而不是预构建的 Python Lance wheel。首次启动使用 `docker compose up -d lance-dev`；新增源文件后重启服务，以刷新容器内的源码镜像目录。

目录中保留以下文件：

- `write_lineitem.rs`：写入程序及写入边界、尾批、非法输入测试。
- `scan_lineitem.rs`：最新版本、历史版本和带过滤条件的扫描规划测量。
- `lineitem_common.rs`：采集开关和 JSONL 导出。
- `collector_tests.rs`：lineitem 使用的元数据采集器的正确性测试，覆盖错误返回、嵌套区间去重、关闭采集、并发请求隔离、子任务上下文传递和重叠区间并集。
- `README.md`：实验说明。
- `task1.md`：原始实验任务与元数据统计要求，保留作为需求依据。
- `data/`、`results/`：本地输入、生成的数据集和结果，均由 Git 忽略。

```powershell
$docker = 'C:\Users\lgn\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe'
& $docker compose exec -T lance-dev cargo test -p lance-core --test metadata_metrics
& $docker compose exec -T lance-dev cargo test -p lance-examples --example write_lineitem
& $docker compose exec -T lance-dev cargo clippy -p lance-examples --example write_lineitem --example scan_lineitem -- -D warnings
& $docker compose exec -T lance-dev cargo fmt --all --check
```

已有功能验证：采集器 4 项测试、写入程序 2 项测试通过；lineitem 最新版本、历史版本和过滤结果的行数与输入一致；关闭扫描采集时不会创建 JSONL。完整工作区测试和优化构建的正式基准测试尚未执行。

## 输出格式与实验限制

结果包含数据集 URI、操作名称、成功状态、构建与缓存策略、各阶段累计耗时和调用次数，以及成功时的实际版本号和 Fragment 数。写入记录还包含提交序号、本次行数和文件数；扫描记录包含迭代编号、请求版本和过滤条件。无法获取的版本号与 Fragment 数不虚构填入。

lineitem 数据位于 Windows 源码挂载目录。读取使用新的 Dataset Session，但操作系统页缓存未受控，因此不是冷存储基准测试。JSONL 采用追加模式，不同实验配置应指定不同输出文件。

对比实验应记录容器资源、CPU 与存储、Rust 版本、构建配置、源码提交及差异、Fragment 数、Schema、缓存策略、存储后端和采集开关。与 Iceberg 及数据库原生方案对比时，需要采用一致的指标定义，并按操作类型和 Fragment 数比较分布，不要混合启动阶段与稳态结果，也不要混合成功与失败操作。

输入解析、RecordBatch 构造和结果导出不在被测 future 内；未埋点的衔接逻辑和部分扫描计划组装不在元数据区间并集中，但仍体现在端到端耗时里。

容器提示可选的 FP16/BF16/AMX 内核不可用；本实验的标量元数据测试不使用这些内核，但向量基线实验需要单独考虑该构建配置。
