# h5i-db

[English](README.md) · [Español](README.es.md) · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · **日本語**

**クオンツ研究のための、速くてエージェント前提の時系列データベース。組み込み型、Rust製。**

- **時系列の形に速い:** 2000万行のOHLCV+VWAP集計で、DuckDBとPolarsより4.5倍以上速い。
- **時系列SQLがそのまま使える:** ASOF join、タイムゾーンを解釈する `time_bucket`、
  gapfill/resample、移動窓、`vwap`、`ewma`。
- **データベースをミリ秒でforkできる:** forkはデータをコピーせず共有する。
  分岐して、変えて、評価して、捨てる。この試行錯誤をエージェントがほぼ無料で
  何度でも回せる。
- **書き込みはすべて原子的でバージョン付きのコミット:** 過去のどのバージョンも
  O(1)で読める。取り込みをしくじっても、それが人の操作でもエージェントの操作でも、
  `restore` 一回で元に戻る。
- **エージェントの書き込みを縛る安全策:** 変更を事前に確認できるプレビュー、
  ポリシーによるゲート、破壊的な操作を止めるフェイルクローズの制約、そして
  何がなぜ変わったかを残す監査証跡。
- **時点を固定した読み取り:** 判断時刻を固定すれば、pandasまで届くデータフレームに
  それ以降の行は入りようがない。先読みバイアスは構造的に起こらない。
- **組み込み型:** ディレクトリが一つあればよく、サーバーもデーモンもいらない。Apache-2.0。

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
h5i-db ui market.db                                                # レビュー画面
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

---

## 開発

```bash
cargo test --workspace          # クラッシュ安全性の障害注入を含む約290テスト
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
```

`crates/` 以下のワークスペースcrate: `core`（バージョン管理付きストレージの中核）、
`query`（DataFusion層）、`cli`（エージェントが触るバイナリ）、`ui`（レビュー画面）、
`python`（`pip install h5i-db`）、`bench`。

---

## ライセンス

Apache-2.0。[LICENSE](./LICENSE) を参照。
