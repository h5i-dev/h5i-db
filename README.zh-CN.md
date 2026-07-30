# h5i-db

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · **简体中文** · [日本語](README.ja.md)

**一个面向量化研究、为智能体而生的高速时序数据库与回测引擎。嵌入式，用 Rust 编写。**

- (DB) **贴合时序数据的性能：** 在 2000 万行的 OHLCV+VWAP 汇总上，比 DuckDB 和
  Polars 快 4.5 倍以上。
- (DB) **原生的时序 SQL：** ASOF join、可感知时区的 `time_bucket`、
  gapfill/resample、滑动窗口、`vwap`、`ewma`。
- (DB) **时点（point-in-time）读取：** 固定一个决策时刻，交到 pandas 手上的数据帧
  就不可能包含此后的行。从构造上杜绝前视偏差。
- (BT) **高效的事件驱动回测器：** 重放内核每秒处理 305 万个事件，在同一份盘口最优价
  工作负载上是 NautilusTrader 的 11.7 倍、LEAN 的 31 倍。
- (BT) **原生支持主流市场：** Kalshi、Polymarket 和 Hyperliquid 的报文都会解码成
  同一套规范表，费用也按各场所真实的费率曲线和资金费计算，而不是一个通用的
  `名义金额 × 费率`。
- (BT) **还是那些统计量，外加它们值多少信任：** 因子与绩效数字与 `alphalens`、
  `empyrical` 一致；deflated Sharpe 和过拟合概率则说明一个结果里有多少只是“搜出来的”。
- (AI) **毫秒级 fork 一个数据库：** fork 共享数据而不是复制数据。智能体因此可以
  近乎零成本地大规模试错（fork、修改、评估、丢弃）。
- (AI) **每次写入都是一次原子的、带版本的提交：** 任何历史版本都能以 O(1) 读取，
  所以一次错误的导入（无论出自人还是智能体）只需一条 `restore` 即可撤销。
- (AI) **面向智能体写入的安全策略：** 可预览的变更、策略闸门、以失败即关闭的方式
  拦截破坏性操作的约束，以及记录“改了什么、为什么改”的审计轨迹。

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
h5i-db ui market.db                                                # 审阅与实验界面
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

**回测**（同一次安装即可，不需要服务端，也不需要另一条数据管线）

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

同一份带类型的契约也能从命令行使用，所以一个配置文件就是完整的复现配方：

```bash
python -m h5i_db.backtest inspect market.db config.json   # 重放保真度与预检结论
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
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

## 量化工作流

`h5i_db.quant` 把常规的研究循环直接跑在这个引擎上，而且每个结果都会记下它是从
哪个数据版本算出来的。

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")     # 固定的锚点

panel.ic()                  # 按日期的 rank IC，每个 horizon 一列
panel.quantile_returns()    # 每个分位桶的平均前瞻收益
quant.factor_report(panel, path="factor.html")
```

因子统计与 `alphalens-reloaded` 一致，组合统计与 `empyrical-reloaded` 一致，所以
数字本身还是你熟悉的那些；新的地方在于它们可以追溯来源。报告开头就写明版本 SHA
和当次使用的锚点，没有锚定的运行会如实说明，而 `quant.verify()` 拒绝为无法复现的
结果背书。

有三件事来自存储层，而不是来自统计：

- **`event_time_cutoff=`** 把每次读取限制在某个决策时刻能够知道的范围内，于是需要
  用到之后价格的前瞻收益会被丢掉，而不是被算出来。
- **`quant.sweep()`** 跑参数网格时每次试验一个 fork，试验之间无法互相污染，同时所有
  试验又能在一次跨 fork 查询里比较。
- **`quant.restatement_impact()`** 在两个数据版本上重算同一件事，报告供应商的数据
  修订到底动了什么。

选择偏差在这里有一等的统计量，而不是一句脚注，因为搜出来的数字，价值低于一次就得到
的同一个数字：

- **`quant.deflated_sharpe(returns, trials=N)`** 按找到它所用的搜索规模来折减夏普
  比率，`minimum_track_record_length()` 则给出这个比率要有意义所需的记录长度。
- **`quant.probability_of_backtest_overfitting(matrix)`** 运行组合对称交叉验证：
  PBO 接近 0.5 意味着样本内的赢家并不携带信息。
- **`quant.purged_kfold()`**、**`combinatorial_purged()`** 和 **`walk_forward()`**
  按 horizon 与 embargo 切分，于是依赖后面十根 K 线的标签不会渗进自己的训练折。
  horizon 从不猜测：省略它就意味着标签是瞬时的。
- **`quant.fit_impact()`** 从真实成交去标定滑点模型，而不是假定一个成本常数。

### 回测

`h5i-db-backtest` 是一个事件驱动的回测器，它的数据面就是数据库本身。一次运行在
fork 内部执行，并把 `bt_orders`、`bt_fills`、`bt_positions`、`bt_equity` 写到那里，
所以结果能用与行情数据相同的 SQL 读取，两次运行也能用 `fork_diff` 在成交级别对比。

```python
fork = db.fork("bt-momentum-001")
quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")
```

它同时也很快，因为重放是直接从存储层读取已解码的记录，而不是每个事件都跨一次语言
边界。在同一份工作负载上（20 万条盘口最优价更新、200 笔市价单、一个标的，每个适配器
都会验证自己确实看到了全部事件）：

| 引擎 | 测量边界 | 中位数 | 吞吐 |
|---|---|---:|---:|
| **h5i-db** | 已解码记录穿过重放内核 | **65.7 ms** | **305 万 事件/秒** |
| **h5i-db** | 含持久化的完整运行：扫描、解码、fork、重放、写回 | 331 ms | 60.5 万 事件/秒 |
| NautilusTrader 1.230.0 | 内存中的对象穿过 `BacktestEngine.run()` | 767 ms | 26.1 万 事件/秒 |
| LEAN `11ba019f6` | 从首个 `Slice` 回调到 `OnEndOfAlgorithm`，数据来自磁盘 | 2033 ms | 9.84 万 事件/秒 |

含持久化的那条边界干的活明显比另外两者更多，即便如此仍是 NautilusTrader 内存引擎的
2.3 倍、LEAN 实测回调吞吐的 6.1 倍。这是一份窄口径的事件驱动工作负载的结果，不是对
回测系统的排名：各家的边界画法不同，而这份基准校验的是事件数和订单数，不是 PnL 的
等价性。方法、原始样本，以及每条边界为什么画在那里，都在
[benchmarks/backtest_compare/RESULTS.md](benchmarks/backtest_compare/RESULTS.md)。

模拟本身覆盖到的东西：

- **一次运行是（数据锚点、策略、配置）的纯函数。** 不看墙上时钟，没有未设种子的
  随机数，也不会不排序就遍历哈希表。`result.verify()` 会重跑一次保存下来的运行，
  并报告它是否复现。
- **前视是用结构关掉的，不是靠约定。** 记录带有 `ts_event` 和 `ts_init`，并按
  `ts_init` 顺序重放，所以迟到的数据就是迟到；策略没有任何通往市场结算结果的路径。
- **结算受可观测性约束。** 对一个为期半年的市场只重放三天，运行结束时仍持有仓位，
  并说明原因，而不是记上一笔那三天里谁都拿不到的盈利。两个数字都会保留：按市价的
  盈亏和结算后的盈亏，两者之差作为显式调整项报告，而不是悄悄并进去。
- **公司行动向前应用，从不回溯改写。** 没有人以拆股调整后的价格成交过，所以拆股、
  分红、退市都在生效的那一刻作为事件到来，并作用于持仓、挂在盘口的限价单和估值。
  调整因子本身就是时点数据；尚未公告的行动根本不在流里。代码到标的的解析建立在
  半开区间上，含义不唯一的查询会被拒绝，并列出候选。
- **账户是多币种的，** 涵盖保证金、强制平仓、永续资金费、订单修改、自成交防范，
  以及下单前的风险限额。
- **预检会拒绝数据支撑不了的主张。** `backtest.inspect()` 会报告一个重放保真度；
  用周期性快照去要求队列位置级别的成交是一个错误，而不是一个看着像样的数字。
- **策略有三种写法：** signals 或 commands 表（把策略当数据放进去，循环里既没有回调
  代码也没有语言边界）、Python 的 `EventStrategy` 回调，以及 Rust 原生的 `Strategy`
  trait。
- **场所覆盖：** 预测市场是第一个场所，并以 N 结果市场作为一般情形；Kalshi、
  Polymarket 和 Hyperliquid 的加载器都产出同一套规范表。`KalshiFees` 实现的是真实的
  二次费率曲线、厘分（centicent）取整，以及按订单的部分成交取整累加器，而不是
  `名义金额 × 费率`。

详见[量化](https://db.h5i.dev/manual/quant/)与[回测](https://db.h5i.dev/manual/backtest/)
手册页。

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

- **一次试验由内容来标识，而不是由名字。** 锚定好的声明式 `BacktestConfig` 会对重放
的全部输入算出一个 `trial_digest`，其中不含 run id 和描述性的元数据。再提交一遍语义
相同的试验，会带着 `cached=True` 返回已记录的结果，而不是重新 fork、重新重放；查找
与创建这一对动作在本地各个智能体进程之间是串行化的，所以重试循环既不会多花一次运行，
也不会把同一个分数算两遍。

- **审阅界面做的是分配注意力，不是排名。** `h5i-db ui` 按“接下来需要人看什么”排序：
需要决策的、失败或有告警的、已完成但未读的、正在跑的、已读的。扫一眼列表并不算读过；
只有打开某次试验的详情，它才计为已读。排行榜是单独一个标签页，因为“目前哪个最好”
和“我还有哪个没看”是两个不同的问题。

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
- **实盘交易：** 回测器从不真正下单。没有券商接口，没有组合优化器，也没有绘图 API；
  它的边界就是模拟与评估。

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
