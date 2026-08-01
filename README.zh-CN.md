# h5i-db

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · **简体中文** · [日本語](README.ja.md)

**一个面向量化研究、为智能体而生的高速时序数据库与回测引擎。嵌入式，用 Rust 编写。**

- **贴合时序数据的性能：** 在 2000 万行的 OHLCV+VWAP 汇总上，比 DuckDB 和
  Polars 快 4.5 倍以上。
- **原生的时序 SQL：** ASOF join、可感知时区的 `time_bucket`、
  gapfill/resample、滑动窗口、`vwap`、`ewma`。
- **时点（point-in-time）读取：** 固定一个决策时刻，交到 pandas 手上的数据帧
  就不可能包含此后的行。从构造上杜绝前视偏差。
- **高效的事件驱动回测器：** 重放内核每秒处理 305 万个事件，在同一份盘口最优价
  工作负载上是 NautilusTrader 的 11.7 倍、LEAN 的 31 倍。
- **原生支持交易场所：** [Kalshi](#数据源)、[Polymarket](#数据源)、[Hyperliquid](#数据源)、[Binance](#数据源) 等。
- **专业的统计分析：** 与 `alphalens`、`empyrical` 对齐的因子与绩效指标，外加
  deflated Sharpe 和过拟合概率检测。
- **毫秒级 fork 一个数据库：** fork 共享数据而不是复制数据。智能体因此可以
  近乎零成本地大规模试错（fork、修改、评估、丢弃）。
- **每次写入都是一次原子的、带版本的提交：** 任何历史版本都能以 O(1) 读取，
  所以一次错误的导入（无论出自人还是智能体）只需一条 `restore` 即可撤销。
- **面向智能体写入的安全策略：** 可预览的变更、策略闸门、以失败即关闭的方式
  拦截破坏性操作的约束，以及记录“改了什么、为什么改”的审计轨迹。

📖 **[文档](https://db.h5i.dev/manual/)** · [回测](https://db.h5i.dev/manual/backtest/) · [量化](https://db.h5i.dev/manual/quant/) · [Python API](https://db.h5i.dev/api/) ·
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
h5i-db ui market.db                                                # 审阅与实验界面
```

**Python 库：DataFrame 与 SQL**

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

**Python 库：回测**（同一次安装即可，不需要服务端）

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # 固定的锚点
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # 数据支撑不了的主张一律拒绝
result = backtest.execute(db, config)            # 在 fork "bt-momentum-001" 中重放

result.summary()                  # 成交数、期末现金、实际模拟到了哪里
result.explain()                  # 订单为什么被拒、为什么没成交
result.fills                      # Arrow 表，也可以直接 SELECT * FROM bt_fills
result.tearsheet("run.html")
result.verify()                   # 用保存下来的配置重新执行并比对
```

一个参数网格会变成“每次试验一个 fork”，排名不需要任何导出步骤。给它明确的训练窗口
和留出窗口，每次试验就会跑完两个阶段，于是排行榜可以按样本外的结果来读：

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

**智能体 skill**（Claude Code、Codex、Cursor 等）

```bash
npx skills add h5i-dev/h5i-db        # 从 skills/h5i-db/ 安装 h5i-db skill
```

**跑起来看看**

```bash
python examples/agent_swarm_demo.py   # 三个智能体、十一次试验，然后打开界面
```

让一支小队在同一份固定的数据上跑：一次阈值扫描、一组执行成本的阶梯对比，以及一次
标记为需要人工签字的验证。

<p align="center">
  <img src="./docs/_static/backtest-ui.png" alt="演示界面视图" width="99%">
</p>

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
- **惰性重放：** 回测内核不会把一整段窗口展开，而是一条一条地取记录，所以无论一次
  运行重放的是一天还是一亿个事件，内存占用都是平的。
- **走索引的订单撮合：** 挂在盘口的订单按场所和价格建索引，一笔新成交只会唤醒它真正
  穿越的那些订单，而不是把所有未平仓订单重扫一遍。

---

## 为什么适合智能体

- **可复现的输入：** 每次读取都会解析到一个具体版本，因此“这次运行到底看到了
哪些数据”是有答案的；针对该版本重跑是 O(1) 的操作，而不是一场考古。
- **别让一个结果撑爆上下文窗口。** `H5I_DB_PROFILE=agent` 会给每次查询设上限，
其余部分写入 Parquet，并报告真实行数以及被保留下来的行存放在哪里。
- **可据以行动的错误信息：** stderr 的错误信封中带有 `next_actions`（可直接执行
的命令）、用于纠正拼写的 `did_you_mean`，以及 `retryable` 标记。
- **分支而不复制。** `fork` 会在所有表的固定视图之上开出一个可写工作区，并且
不复制任何数据，因此一次改动或一次实验只花费一个小文件，丢弃它和保留它一样
便宜。
- **权限控制。** 变更通过 `plan`/`apply` 预览，并且策略可以强制要求走这道关；
`--idempotency-key` 让重试的导入变成重放；可选的 `data-policy` 会以失败即关闭的
方式拒绝格式错误的行。
- **一次回测是一个分支。** 每次运行都在自己的 fork 里执行，并把订单、成交、持仓和
净值曲线当作普通表写在那里。于是两次运行可以用 `fork_diff` 在成交级别
对比，整轮扫描能用一条跨 fork 查询汇总，值得留下的那次 `promote`，其余的丢掉。
- **审阅界面做的是分配注意力，不是排名。** `h5i-db ui` 按“接下来需要人看什么”排序：
需要决策的、失败或有告警的、已完成但未读的、正在跑的、已读的。扫一眼列表并不算读过；
只有打开某次试验的详情，它才计为已读。

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
- **实盘交易：** 回测器从不真正下单。没有券商接口，没有组合优化器，也没有绘图 API；
  它的边界就是模拟与评估。

---

## 数据源

加载器读取你已经拿到的文件和响应，本身不抓取，因此凭证、重试和限流都留在你的
脚本里。

| 数据源 | 订单簿 | 成交 | K 线 | 其他 |
|---|---|---|---|---|
| Kalshi | ✓ | ✓ | ✓ | 结算 |
| Polymarket | ✓ | ✓ | 聚合 | 结算、完整份额的铸造与赎回 |
| Hyperliquid | ✓ | ✓ | ✓ | 资金费率、标记价与预言机价、杠杆上限 |
| Limitless | ✓ | ✓ | 聚合 | |
| Opinion | ✓ | ✓ | 聚合 | |
| Manifold | 不适用 | ✓ | 聚合 | 结算 |
| Binance | | ✓ | ✓ | 现货与合约批量数据 |
| 任意 OHLCV 导出 | | | ✓ | 券商 CSV、`yfinance`、Stooq |
| 任意成交数据 | | ✓ | 聚合 | |
| 公开序列 | | | | 利率或指数等参考价 |
| 公司行动 | | | | 拆股、分红、退市 |

「聚合」指 K 线由该数据源自身的成交汇总而来，而非直接下载，所以没有成交的区间
不会生成 K 线，缺口保持可见。「不适用」指该场所没有这个概念：Manifold 是自动
做市商，有成交但没有订单簿。细节见[场所指南](crates/h5i-db-venues/README.md)。

---

## 基准测试

完整的方法说明与结果见 [benchmarks](benchmarks)。

**数据库**

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

**回测**

| 引擎 | 测量边界 | 中位数 | 吞吐 |
|---|---|---:|---:|
| **h5i-db** | 已解码记录穿过重放内核 | **65.7 ms** | **305 万 事件/秒** |
| h5i-db `wide` | 同一内核，128 位定点 | 94 ms⁷ | 213 万 事件/秒⁷ |
| **h5i-db** | 同一内核，策略是每事件一次的 Python 回调 | 278 ms⁶ | 71.9 万 事件/秒⁶ |
| h5i-db `wide` | 同上，128 位定点 | 306 ms⁶ ⁷ | 65.3 万 事件/秒⁶ ⁷ |
| **h5i-db** | 含持久化的完整运行：扫描、解码、fork、重放、写回 | 280 ms | 71.3 万 事件/秒 |
| h5i-db `wide` | 同上，128 位定点 | 280 ms | 71.3 万 事件/秒 |
| NautilusTrader 1.230.0 | 内存中的对象穿过 `BacktestEngine.run()` | 767 ms | 26.1 万 事件/秒 |
| LEAN `11ba019f6` | 从首个 `Slice` 回调到 `OnEndOfAlgorithm`，数据来自磁盘 | 2033 ms | 9.84 万 事件/秒 |

先热身一次，之后每次都用全新进程测三遍取中位数；每个适配器都会验证自己确实看到了
20 万个事件、发出了 200 笔订单。各家测量的边界并不相同，就是表中那一列写的意思；
这份基准校验的是事件数和订单数，不是 PnL 的等价性。
⁶ 其他行重放期间不调 Python；这一行每个事件都跨进去，和 Nautilus 一样。回调对
回调来比，差距是 3.1 倍而不是 13 倍。此数字为推导值（原生内核加实测边界开销），
并非直接计时。
⁷ `--features wide`，默认关闭，详见
[精度与范围](https://db.h5i.dev/manual/backtest/)。为推导值，非直接计时；方法见
[RESULTS.md](benchmarks/backtest_compare/RESULTS.md)。

---

## 开发

```bash
cargo test --workspace          # 约 290 个测试，含崩溃安全性的故障注入
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # 对比 NautilusTrader 与 LEAN
```

`crates/` 下的 workspace crate：`core`（带版本的存储内核）、`query`
（DataFusion 层）、`backtest`（重放内核、场所模型、结算）、`venues`（Kalshi、
Polymarket、Hyperliquid 的加载器）、`cli`（面向智能体的可执行文件）、
`ui`（审阅界面）、`observability`、`python`（`pip install h5i-db`）、`bench`。

---

## 许可证

Apache-2.0，详见 [LICENSE](./LICENSE)。
