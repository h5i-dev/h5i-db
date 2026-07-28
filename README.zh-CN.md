# h5i-db

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · **简体中文** · [日本語](README.ja.md)

**一个面向量化研究、为智能体而生的高速时序数据库。嵌入式，用 Rust 编写。**

- **贴合时序数据的性能：** 在 2000 万行的 OHLCV+VWAP 汇总上，比 DuckDB 和
  Polars 快 4.5 倍以上。
- **原生的时序 SQL：** ASOF join、可感知时区的 `time_bucket`、
  gapfill/resample、滑动窗口、`vwap`、`ewma`。
- **毫秒级 fork 一个数据库：** fork 共享数据而不是复制数据。智能体因此可以
  近乎零成本地大规模试错（fork、修改、评估、丢弃）。
- **每次写入都是一次原子的、带版本的提交：** 任何历史版本都能以 O(1) 读取，
  所以一次错误的导入（无论出自人还是智能体）只需一条 `restore` 即可撤销。
- **面向智能体写入的安全策略：** 可预览的变更、策略闸门、以失败即关闭的方式
  拦截破坏性操作的约束，以及记录“改了什么、为什么改”的审计轨迹。
- **时点（point-in-time）读取：** 固定一个决策时刻，交到 pandas 手上的数据帧
  就不可能包含此后的行。从构造上杜绝前视偏差。
- **嵌入式：** 一个目录，无需服务端，也没有守护进程。Apache-2.0。

📖 **[文档](https://db.h5i.dev/manual/)** · [手册](https://db.h5i.dev/manual/) · [Python API](https://db.h5i.dev/api/) ·
[实例集](https://github.com/h5i-dev/h5i-db-cookbook) · [智能体 skill](skills/h5i-db/SKILL.md)

---

## 快速上手

**命令行**

```bash
cargo install h5i-db-cli
```

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db context market.db                                           # 一次调用即可掌握全局
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z                             # 未来不可读
h5i-db ui market.db                                                # 审阅界面
```

**Python 库**

```bash
pip install h5i-db
```

```python
import pyarrow as pa
import h5i_db

db = h5i_db.Database("market.db", create=True)

db.create_table(
    "trades",
    pa.schema([("ts", pa.timestamp("us")), ("symbol", pa.string()), ("price", pa.float64())]),
    time_column="ts",
)
db.append("trades", pa.table({
    "ts": pa.array([1_700_000_000_000_000, 1_700_000_060_000_000], pa.timestamp("us")),
    "symbol": ["AAPL", "MSFT"], "price": [187.4, 411.2],
}))

df = db.sql("SELECT symbol, avg(price) AS px FROM trades GROUP BY symbol").to_pandas()
# df = db.table("trades").group_by("symbol").agg(px=col("price").mean()).to_pandas()
old = db.read("trades", version=1)                # 时间旅行：读取任意历史版本

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # 在变更落地之前先预览
plan.apply()
```

**智能体 skill**（Claude Code、Codex、Cursor 等）

```bash
npx skills add h5i-dev/h5i-db        # 从 skills/h5i-db/ 安装 h5i-db skill
```

---

## 为什么选它

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| 面向用户的版本管理 / 时间旅行 | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓（版本读取为 O(1)） |
| 支持 join / 窗口函数 / CTE 的 SQL | ✓ | 部分 | ✗ | ✗ | ✗ | ✓（DataFusion） |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴（在已排序存储上免排序） |
| 可预览的变更（plan/apply） | ✗ | ✗ | ✗ | ✗ | ✗ | ✓，且可由策略强制 |
| 并发写入 | MVCC | 不适用 | 不适用 | 不适用 | 不安全³ | CAS + 显式冲突 |
| 2000 万行窄时间范围扫描 | 45.5 ms | 28.1 ms | 23.9 ms | 22.8 ms | **4.2 ms**⁵ | 10.0 ms |
| 2000 万行 1 分钟 OHLCV+VWAP | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| 2000 万行按标的 ASOF join | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |

¹ `AT (VERSION …)` 语法确实存在，但原生存储会拒绝它。
² 有一个实验性的 `join_asof`，但慢约 1000 倍，在这个量级上不具备可用性。
³ 其文档假定每个标的只有一个写入方。
⁴ 原生的 `ASOF JOIN … MATCH_CONDITION` SQL 语法，以及一个 `asof_join(...)`
  表函数（SQL 与 Python 均可用）。
⁵ ArcticDB 依靠自有 LMDB 存储上的原生时间索引，在窄范围点查上胜出；h5i-db
  的清单裁剪位列第二，并且超过了所有通用引擎。

完整方法说明见 [benchmarks/RESULTS.md](benchmarks/RESULTS.md)。

---

## 为什么快

- **清单裁剪：** 每个版本的清单都记录了各分段的时间范围和各列的最小值/最大值。
  窄查询在打开任何一个文件之前，就已经把整段整段的数据排除掉了。
- **声明排序：** 分段按时间排序存储，查询层会把这一点告诉 DataFusion。于是
  OHLCV 汇总可以流式处理，而不必先对 2000 万行做排序（每个对照引擎都要付这笔
  代价），ASOF join 也无需排序。
- **不可变分段：** 由于分段永不改变，页脚元数据可以无条件缓存，热扫描因此减少
  约 40% 的开销。
- **版本感知的聚合状态：** OHLCV/VWAP 汇总会按不可变分段持久化可合并的中间
  状态；再次查询时只需毫秒级地合并这些状态，而不是重新计算，并且只扫描新追加
  的分段。
- **不做底层特技：** 通用扫描与聚合直接跑在标准 DataFusion 上，与最好的引擎
  持平；只有当时序数据的形态能让额外结构真正带来收益时，h5i-db 才引入它。

---

## 为什么适合智能体

- **可复现的输入：** 每次读取都会解析到一个具体版本，因此“这次运行到底看到了
哪些数据”是有答案的；针对该版本重跑是 O(1) 的操作，而不是一场考古。

- **时点提取：** 读取点可以在两个轴上固定：事件时间（`--decision-time`）与到达
时间（`--as-of`）。这样一来，你交给 pandas 的数据帧在源头就被限定住了，而源头
是唯一能让这种限定在进入 Python 之后依然成立的地方。`arrival-delta` 则事后度量
一个结果中有多少依赖了后来才到达的数据。

- **别让一个结果撑爆上下文窗口。** `H5I_DB_PROFILE=agent` 会给每次查询设上限，
其余部分写入 Parquet，并报告真实行数以及被保留下来的行存放在哪里。

- **一次调用就能掌握全局：** `h5i-db context <db>` 会返回每张表的 schema、大小、
时间范围和当前版本，运维策略的各项闸门，以及任何已经暂存的 plan。

- **可据以行动的错误信息：** stderr 的错误信封中带有 `next_actions`（可直接执行
的命令）、用于纠正拼写的 `did_you_mean`，以及 `retryable` 标记。

- **分支而不复制。** `fork` 会在所有表的固定视图之上开出一个可写工作区，并且
不复制任何数据，因此一次改动或一次实验只花费一个小文件，丢弃它和保留它一样
便宜。随后 `forks('trades')` 可以带着 `__fork` 列一次性读取所有分支上的这张表，
比较各分支的产出无需任何导出步骤。

- **犯错的代价很低。** 变更通过 `plan`/`apply` 预览，并且策略可以强制要求走这道
关；`--idempotency-key` 让重试的导入变成重放而不是重复追加；可选的 `data-policy`
会以失败即关闭的方式拒绝格式错误的行；提交先 fsync 再切换，并带有清单哈希链，
这些都通过在每一步杀死写入方来验证。

---

## 什么时候*不该*用 h5i-db

- **分布式的多 TB 数仓：** 本项目在设计上就是单机、嵌入式的。这类场景请选择
  ClickHouse、Snowflake 或湖仓架构。
- **OLTP 或高并发在线服务：** 同一时刻只有一个写入方，没有行级 MVCC，也没有
  交互式事务。请用 Postgres。
- **亚微秒级的 tick 采集：** 它面向的写入节奏是分钟线、日终数据和供应商文件，
  而不是采集层本身。那是 kdb+ 的地盘。
- **没有时间列的数据库：** 整个设计都以时间索引为前提；没有它，你会失去裁剪、
  ASOF join 和时点读取。

---

## 开发

```bash
cargo test --workspace          # 约 290 个测试，含崩溃安全性的故障注入
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
```

`crates/` 下的 workspace crate：`core`（带版本的存储内核）、`query`
（DataFusion 层）、`cli`（面向智能体的可执行文件）、`ui`（审阅界面）、
`python`（`pip install h5i-db`）、`bench`。

---

## 许可证

Apache-2.0，详见 [LICENSE](./LICENSE)。
