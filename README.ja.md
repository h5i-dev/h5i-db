# h5i-db

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · **日本語**

**クオンツ研究のための、速くてエージェント前提の時系列データベースとバックテスト
エンジン。組み込み型、Rust製。**

- (DB) **時系列の形に速い:** 2000万行のOHLCV+VWAP集計で、DuckDBとPolarsより4.5倍以上速い。
- (DB) **時系列SQLがそのまま使える:** ASOF join、タイムゾーンを解釈する `time_bucket`、
  gapfill/resample、移動窓、`vwap`、`ewma`。
- (DB) **時点を固定した読み取り:** 判断時刻を固定すれば、pandasまで届くデータフレームに
  それ以降の行は入りようがない。先読みバイアスは構造的に起こらない。
- (BT) **速いイベント駆動バックテスタ:** リプレイカーネルは毎秒305万イベントを処理し、
  同一ワークロードでNautilusTraderの11.7倍、LEANの31倍。
- (BT) **主要な市場をそのまま扱える:** Kalshi、Polymarket、Hyperliquidのペイロードは
  一組の正規化テーブルへデコードされる。手数料も、汎用の `想定元本 × 料率` ではなく
  各会場の実際の曲線とファンディングで計算する。
- (BT) **いつもの統計と、それをどこまで信じてよいか:** ファクターとパフォーマンスの
  数字は `alphalens` と `empyrical` に一致する。そこにdeflated Sharpeと過剰適合確率が、
  その数字のうちどれだけが探索の産物だったかを添える。
- (AI) **データベースをミリ秒でforkできる:** forkはデータをコピーせず共有する。
  分岐して、変えて、評価して、捨てる。この試行錯誤をエージェントがほぼ無料で
  何度でも回せる。
- (AI) **書き込みはすべて原子的でバージョン付きのコミット:** 過去のどのバージョンも
  O(1)で読める。取り込みをしくじっても、それが人の操作でもエージェントの操作でも、
  `restore` 一回で元に戻る。
- (AI) **エージェントの書き込みを縛る安全策:** 変更を事前に確認できるプレビュー、
  ポリシーによるゲート、破壊的な操作を止めるフェイルクローズの制約、そして
  何がなぜ変わったかを残す監査証跡。

📖 **[ドキュメント](https://db.h5i.dev/manual/)** · [マニュアル](https://db.h5i.dev/manual/) · [Python API](https://db.h5i.dev/api/) ·
[クックブック](https://github.com/h5i-dev/h5i-db-cookbook) · [エージェント向けskill](skills/h5i-db/SKILL.md)

---

## クイックスタート

**CLI**

```bash
cargo install h5i-db-cli
```

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db context market.db                                           # 一回の呼び出しで全体を把握する
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z                             # 未来は読めない
h5i-db ui market.db                                                # レビューと実験の画面
```

**Pythonライブラリ**

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
old = db.read("trades", version=1)                # タイムトラベル: 過去のどのバージョンでも読める

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # 変更が確定する前に中身を確認する
plan.apply()
```

**バックテスト**（同じインストールのまま。サーバーも別のデータパイプラインもいらない）

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # 固定するピン
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # データが支えられない主張は断る
result = backtest.execute(db, config)            # fork "bt-momentum-001" の中で走る

result.summary()                  # 約定件数、最終残高、どこまで実際に再現できたか
result.explain()                  # 注文が拒否された理由、約定しなかった理由
result.fills                      # Arrowで受け取る。SELECT * FROM bt_fills でもよい
result.tearsheet("run.html")
result.verify()                   # 保存された設定で再実行して突き合わせる
```

パラメータグリッドは1試行ごとに1つのforkになり、エクスポートを挟まずに順位が出る。
学習期間とホールドアウト期間を明示すれば各試行が両方の期間を走るので、
リーダーボードをアウトオブサンプルで読める:

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

同じ型付きの契約がシェルからも使える。だから設定ファイル一つが再現手順そのものになる:

```bash
python -m h5i_db.backtest inspect market.db config.json   # 再現精度と事前チェックの結果
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
```

**エージェント向けskill**（Claude Code、Codex、Cursorなど）

```bash
npx skills add h5i-dev/h5i-db        # skills/h5i-db/ からh5i-dbのskillを入れる
```

---

## なぜこれを使うのか

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| 利用者から見えるバージョン管理・タイムトラベル | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓（バージョン読み取りはO(1)） |
| join・窓関数・CTEを備えたSQL | ✓ | 一部 | ✗ | ✗ | ✗ | ✓（DataFusion） |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴（ソート済みストレージ上でソート不要） |
| 変更のプレビュー（plan/apply） | ✗ | ✗ | ✗ | ✗ | ✗ | ✓、ポリシーで強制もできる |
| 書き込みの同時実行 | MVCC | 該当なし | 該当なし | 該当なし | 安全でない³ | CAS＋明示的な衝突 |
| 2000万行・狭い時間範囲のスキャン | 45.5 ms | 28.1 ms | 23.9 ms | 22.8 ms | **4.2 ms**⁵ | 10.0 ms |
| 2000万行・1分足のOHLCV+VWAP | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| 2000万行・銘柄別のASOF join | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |

¹ `AT (VERSION …)` という構文はあるが、ネイティブのストレージが受け付けない。
² 実験的な `join_asof` はあるものの約1000倍遅く、この規模では使いものにならない。
³ 銘柄ごとに書き手は一つ、という前提が文書化されている。
⁴ ネイティブの `ASOF JOIN … MATCH_CONDITION` 構文と、`asof_join(...)` テーブル関数。
  SQLからもPythonからも呼べる。
⁵ 狭い範囲のピンポイントな読み取りは、ArcticDBが自前のLMDBストア上のネイティブな
  時間インデックスで勝つ。h5i-dbのマニフェスト枝刈りはそれに次ぐ2位で、汎用エンジンは
  すべて上回っている。

計測方法は [benchmarks/RESULTS.md](benchmarks/RESULTS.md) に全部書いてある。

---

## なぜ速いのか

- **マニフェストによる枝刈り:** バージョンごとのマニフェストが、セグメント単位の
  時間範囲と列ごとの最小値・最大値を持っている。範囲の狭いクエリは、ファイルを
  一つも開かないうちにセグメントごと丸ごと捨てる。
- **ソート順を宣言する:** セグメントは時間順に並べて保存し、クエリ層がそのことを
  DataFusionに伝える。おかげでOHLCV集計は2000万行を先にソートせずストリーム処理で
  済み（比較対象はどれもこのソート代を払っている）、ASOF joinもソートがいらない。
- **セグメントは不変:** フッタのメタデータを無条件にキャッシュできる。セグメントが
  決して書き換わらないから安全で、これでウォームスキャンが約40%縮む。
- **バージョンを意識した集計状態:** OHLCV/VWAPの集計は、不変セグメントごとに
  マージ可能な途中状態を保存する。次に同じ問い合わせが来たら再計算せず、状態を
  ミリ秒でマージし、新しく追加されたセグメントだけを読む。
- **低レイヤの離れ業には頼らない:** 一般的なスキャンや集計は素のDataFusionの上で
  動き、それで最速級のエンジンと肩を並べる。時系列という形のおかげで構造が効く
  ところにだけ、h5i-dbは構造を足している。

---

## クオンツのワークフロー

`h5i_db.quant` は、いつもの研究ループをこのエンジンの上で回す。どの結果にも、
それがどのデータバージョンから計算されたかが記録される。

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")     # 固定するピン

panel.ic()                  # 日付ごとのランクIC。horizonごとに1列
panel.quantile_returns()    # バケットごとの平均フォワードリターン
quant.factor_report(panel, path="factor.html")
```

ファクター統計は `alphalens-reloaded` と、ポートフォリオ統計は `empyrical-reloaded`
と一致する。つまり数字自体は見慣れたものだ。新しいのは、その数字の出どころを
たどれることだ。レポートは冒頭にバージョンSHAと使ったピンを出し、ピンなしで走った
場合はそう書く。そして `quant.verify()` は、再現できない結果に証明書を出さない。

統計ではなくストレージ層から出てくる性質が3つある:

- **`event_time_cutoff=`** は、すべての読み取りをある判断時刻に知り得た範囲へ絞る。
  後の価格が必要になるフォワードリターンは、計算されるのではなく落とされる。
- **`quant.sweep()`** はパラメータグリッドを1試行1forkで走らせる。試行が互いを
  汚染できず、しかも全試行を1回のfork横断クエリで比較できる。
- **`quant.restatement_impact()`** は同じ計算を2つのデータバージョンで走らせ、
  ベンダーの数値訂正が何を動かしたかを報告する。

探索して見つけた数字は、一発で出た同じ数字より価値が低い。だから選択バイアスは
脚注ではなく一級の統計として扱う:

- **`quant.deflated_sharpe(returns, trials=N)`** は、その数字を見つけるまでに試した
  回数でSharpeレシオを割り引く。`minimum_track_record_length()` は、その比率が
  意味を持つまでに必要な記録の長さを返す。
- **`quant.probability_of_backtest_overfitting(matrix)`** は組み合わせ対称
  クロスバリデーションを回す。PBOが0.5に近ければ、インサンプルの勝者には
  情報がなかったということだ。
- **`quant.purged_kfold()`**、**`combinatorial_purged()`**、**`walk_forward()`** は
  horizonとembargoを見て分割する。10本先に依存するラベルが自分の学習foldへ
  漏れ込まない。horizonは推測しない。省略すればラベルは瞬時、という意味になる。
- **`quant.fit_impact()`** は、コストを定数で仮定するのではなく、実際の約定から
  スリッページモデルを推定する。

### バックテスト

`h5i-db-backtest` はイベント駆動のバックテスタで、そのデータ面はデータベース
そのものだ。実行はforkの中で走り、`bt_orders`、`bt_fills`、`bt_positions`、
`bt_equity` をそこへ書く。だから結果は市場データと同じSQLで読めて、2つの実行は
`fork_diff` で約定単位で差分が取れる。

```python
fork = db.fork("bt-momentum-001")
quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")
```

しかも速い。リプレイはイベントごとに言語境界を越えるのではなく、ストレージ層から
デコード済みのレコードを直接読むからだ。共通のワークロード（板の最良気配20万件、
成行注文200件、銘柄1つ。どのアダプタも全件を見たことを検証する）での結果:

| エンジン | 計測した境界 | 中央値 | スループット |
|---|---|---:|---:|
| **h5i-db** | デコード済みレコードがリプレイカーネルを通る区間 | **65.7 ms** | **305万 events/s** |
| **h5i-db** | 永続化を含む実行全体: スキャン、デコード、fork、リプレイ、書き込み | 331 ms | 60.5万 events/s |
| NautilusTrader 1.230.0 | メモリ上のオブジェクトが `BacktestEngine.run()` を通る区間 | 767 ms | 26.1万 events/s |
| LEAN `11ba019f6` | 最初の `Slice` コールバックから `OnEndOfAlgorithm` まで、ディスク供給 | 2,033 ms | 9.84万 events/s |

永続化を含む境界は他の2つより明らかに多くの仕事をしているが、それでも
NautilusTraderのインメモリエンジンの2.3倍、LEANの計測区間の6.1倍だ。これは
イベント駆動の狭い1ワークロードの結果で、バックテストシステムの順位付けではない。
境界の引き方はそれぞれ違い、このベンチマークが検証するのはイベント数と注文数で、
PnLの一致ではない。計測方法、生のサンプル、各境界をそこに引いた理由は
[benchmarks/backtest_compare/RESULTS.md](benchmarks/backtest_compare/RESULTS.md)
にある。

シミュレーション自体が面倒を見る範囲:

- **1回の実行は（データのピン、戦略、設定）の純粋関数。** 実時間を見ないし、
  シードなしの乱数も、ソートせずにハッシュマップを回すこともない。
  `result.verify()` は保存された実行を再実行し、再現したかどうかを報告する。
- **先読みは慣習ではなく構造で閉じてある。** レコードは `ts_event` と `ts_init` を
  持ち、`ts_init` 順にリプレイされる。だから遅れて届いたデータは遅れて届く。
  戦略から市場の決済結果へ至る経路は存在しない。
- **決済は観測可能性で門を閉じる。** 6か月ものの市場を3日ぶんだけリプレイした実行は
  ポジションを持ったまま終わり、その理由を書く。その3日を取引していた誰も
  受け取れない利益を計上したりはしない。数字は両方残る。時価評価の損益と決済後の
  損益、そしてその差は暗黙に混ぜず明示的な調整として報告される。
- **コーポレートアクションは前向きに適用し、遡って書き換えない。** 分割調整後の
  価格で取引した人は一人もいない。だから分割・配当・上場廃止は効力が生じる瞬間の
  イベントとして届き、ポジション、板に残る指値、評価価格に作用する。調整係数は
  時点データであり、まだ発表されていないアクションはそもそもストリームにない。
  ティッカーは半開区間で銘柄に解決され、どちらの企業か決まらない照会は候補を
  挙げて拒否される。
- **口座は多通貨で**、証拠金、強制決済、パーペチュアルのファンディング、
  注文訂正、セルフトレード防止、発注前のリスク上限まで扱う。
- **事前チェックは、データが支えられない主張を断る。** `backtest.inspect()` は
  再現精度を報告し、定期スナップショットからキュー位置の約定を求めるのは
  それらしい数字ではなくエラーになる。
- **戦略の書き方は3通り。** signalsまたはcommandsのテーブル（戦略をデータとして
  置く。コールバックのコードもループ内の言語境界もない）、Pythonの
  `EventStrategy` コールバック、そしてRustネイティブの `Strategy` トレイト。
- **対応会場:** 予測市場が最初の会場で、N択の市場を一般形として扱う。Kalshi、
  Polymarket、Hyperliquidのローダーはどれも同じ正規化テーブルを吐く。`KalshiFees`
  は `想定元本 × 料率` ではなく、実際の二次曲線の手数料、センチセント丸め、
  注文ごとの部分約定の丸め繰り越しを実装している。

詳しくは [クオンツ](https://db.h5i.dev/manual/quant/) と
[バックテスト](https://db.h5i.dev/manual/backtest/) のマニュアルへ。

---

## なぜエージェント向きなのか

- **入力を再現できる。** 読み取りは必ず特定のバージョンに解決される。だから
「この実行はどのデータを見たのか」に答えがあるし、そのバージョンに対して
実行し直すのはO(1)の操作で、発掘作業にはならない。

- **時点を固定して取り出せる。** 読み取り点はイベント時刻（`--decision-time`）と
到着時刻（`--as-of`）の2軸で固定できる。こうするとpandasに渡すデータフレームは
データ源の側で区切られる。区切りがPythonまで生き延びる場所は、そこしかない。
後から効いてくるのが `arrival-delta` で、ある結果のうちどれだけが遅れて到着した
データに依存していたかを測る。

- **結果でコンテキストウィンドウを潰さない。** `H5I_DB_PROFILE=agent` は
クエリごとに上限を設け、あふれた分はParquetに書き出す。そのうえで本当の行数と、
書き出した行の置き場所を報告する。

- **一回の呼び出しで状況をつかめる。** `h5i-db context <db>` を叩けば、各テーブルの
スキーマ、サイズ、時間範囲、最新バージョンに加えて、運用ポリシーのゲートと、
すでに用意されているplanが返ってくる。

- **手を打てるエラーが返る。** stderrに出るエラー封筒には、そのまま実行できる
コマンド（`next_actions`）、打ち間違いに対する `did_you_mean`、再試行してよいかを
示す `retryable` が入っている。

- **コピーせずに分岐する。** `fork` は全テーブルの固定されたビューの上に、
書き込める作業領域を開く。データは一切複製しないので、修正一つでも実験一つでも
コストは小さなファイル一個ぶんで、捨てるのは取っておくのと同じくらい安い。
そのあと `forks('trades')` を使えば、`__fork` 列つきで全ブランチのそのテーブルを
一度に読める。どのブランチが何を出したかを比べるのに、エクスポートの手間はいらない。

- **試行を識別するのは名前ではなく中身。** ピンが効いた宣言的な `BacktestConfig` は、
リプレイの入力すべてから `trial_digest` を作る。run idや説明用のメタデータは含めない。
意味的に同じ試行をもう一度投げると、forkして走らせ直すのではなく記録済みの結果が
`cached=True` で返る。照合と作成はローカルのエージェントプロセスをまたいで直列化
されるので、リトライループが実行を二重に消費したりスコアを二重に数えたりしない。

- **レビュー画面がやるのは順位付けではなく注意の割り振り。** `h5i-db ui` は、次に人が
見るべき順に試行を並べる。判断が必要、失敗または警告あり、完了して未読、実行中、既読。
一覧を眺めただけでは既読にならない。詳細を開いたときだけ既読が付く。リーダーボードは
別タブだ。「今のところ最良はどれか」と「自分がまだ見ていないのはどれか」は別の問いだから。

- **失敗しても安い。** 変更は `plan`/`apply` でプレビューでき、ポリシーでその手順を
必須にもできる。`--idempotency-key` を付ければ、取り込みを再試行しても二重に
追加されず再生になる。任意で有効にする `data-policy` は、壊れた行をフェイルクローズで
弾く。コミットは差し替えの前にfsyncし、マニフェストのハッシュチェーンを持つ。
これらは書き手を各段階で強制終了させて検証してある。

---

## こういう用途には*向かない*

- **分散した数テラバイト級のウェアハウス:** 単一ノードの組み込み型として設計して
  いる。ClickHouseやSnowflake、あるいはレイクハウスを使うべき場面。
- **OLTPや高い同時実行のサービング:** 書き手は同時に一つだけで、行レベルのMVCCも
  対話的なトランザクションもない。Postgresの出番。
- **マイクロ秒未満のtick収集:** 想定している書き込みの粒度は分足、日次、ベンダー
  ファイルであって、収集層そのものではない。そこはkdb+の領域。
- **時間列を持たないデータ:** 設計全体が時間インデックスを前提にしている。それが
  ないと枝刈りもASOF joinも時点読み取りも失われる。
- **実弾のトレード:** バックテスタは本物の注文を一度も出さない。ブローカー接続も
  ポートフォリオ最適化も作図APIもない。担当範囲はシミュレーションと評価まで。

---

## 開発

```bash
cargo test --workspace          # クラッシュ安全性の障害注入を含む約290テスト
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # NautilusTrader・LEANとの比較
```

`crates/` 以下のワークスペースcrate: `core`（バージョン管理付きストレージの中核）、
`query`（DataFusion層）、`backtest`（リプレイカーネル、会場モデル、決済）、
`venues`（Kalshi・Polymarket・Hyperliquidのローダー）、`cli`（エージェントが触る
バイナリ）、`ui`（レビュー画面）、`observability`、`python`
（`pip install h5i-db`）、`bench`。

---

## ライセンス

Apache-2.0。[LICENSE](./LICENSE) を参照。
