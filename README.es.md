# h5i-db

[English](README.md) · **Español** · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**Una base de datos de series temporales y un motor de backtesting rápidos y
nativos para agentes, pensados para la investigación cuantitativa. Embebidos y
escritos en Rust.**

- **Rápida en la forma de las series temporales:** más de 4,5× más rápida que
  DuckDB y Polars en agregaciones OHLCV+VWAP sobre 20 M de filas.
- **SQL nativo de series temporales:** ASOF join, `time_bucket` con zonas
  horarias, gapfill/resample, ventanas móviles, `vwap`, `ewma`.
- **Lecturas point-in-time:** fija un instante de decisión y el marco de datos
  que llega a pandas no podrá contener filas posteriores a él. Sin sesgo de
  anticipación, por construcción.
- **Backtester orientado a eventos y eficiente:** 3,05 M de eventos/s a través
  del núcleo de replay, 11,7× NautilusTrader y 31× LEAN en una carga compartida
  sobre el tope del libro.
- **Soporte nativo de los mercados más usados:** los payloads de Kalshi,
  Polymarket e Hyperliquid se decodifican en un único conjunto canónico de tablas,
  cada uno con la curva de comisiones y el funding reales del venue.
- **Análisis estadístico profesional:** métricas de factores y de rendimiento
  con paridad `alphalens` y `empyrical`, además de Sharpe deflactado y detección de
  la probabilidad de sobreajuste.
- **Bifurca una base de datos en milisegundos:** los forks comparten los datos
  en lugar de copiarlos. Un agente puede recorrer ciclos amplios de ensayo y
  error (bifurcar, mutar, evaluar, descartar) a un coste casi nulo.
- **Cada escritura es un commit atómico y versionado:** cualquier versión
  pasada se lee en O(1), así que una ingesta defectuosa (humana o de un agente)
  se deshace con un solo `restore`.
- **Políticas de seguridad para las escrituras de agentes:** mutaciones
  previsualizables, controles por política, restricciones que fallan en cerrado
  y bloquean las operaciones destructivas, y un registro de auditoría de qué
  cambió y por qué.

📖 **[Documentación](https://db.h5i.dev/manual/)** · [Backtesting](https://db.h5i.dev/manual/backtest/) · [Cuantitativa](https://db.h5i.dev/manual/quant/) · [API de Python](https://db.h5i.dev/api/) ·
[Recetario](https://github.com/h5i-dev/h5i-db-cookbook) · [Skill para agentes](skills/h5i-db/SKILL.md)

---

## Inicio rápido

**CLI**

```bash
cargo install h5i-db-cli
```

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db context market.db                                           # ubícate en una sola llamada
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z                             # el futuro es ilegible
h5i-db ui market.db                                                # revisión y experimentos
```

**Biblioteca de Python para DataFrames y SQL**

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
old = db.read("trades", version=1)                # viaje en el tiempo: lee cualquier versión pasada

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # previsualiza la mutación antes de aplicarla
plan.apply()
```

**Biblioteca de Python para backtesting** (la misma instalación, sin servidor)

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # el anclaje
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # rechaza lo que los datos no sostienen
result = backtest.execute(db, config)            # corre en el fork "bt-momentum-001"

result.summary()                  # ejecuciones, caja final, hasta dónde simuló de verdad
result.explain()                  # por qué se rechazaron órdenes o nunca se ejecutaron
result.fills                      # en Arrow, o consúltalo: SELECT * FROM bt_fills
result.tearsheet("run.html")
result.verify()                   # reejecuta la configuración guardada y compara
```

Una malla de parámetros se convierte en un fork por prueba, y el ganador se
ordena sin ningún paso de exportación. Dale ventanas explícitas de entrenamiento
y validación y cada prueba correrá ambas fases, así la tabla de clasificación se
lee fuera de muestra:

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

**Skill para agentes** (Claude Code, Codex, Cursor, …)

```bash
npx skills add h5i-dev/h5i-db        # instala la skill de h5i-db desde skills/h5i-db/
```

**Verlo funcionar**

```bash
python examples/agent_swarm_demo.py   # tres agentes, once pruebas, luego la interfaz
```

Lanza una flota sobre un único conjunto de datos fijado: un barrido de umbrales,
una escalera de costes de ejecución y una validación marcada para que la firme
una persona.

<p align="center">
  <img src="./docs/_static/backtest-ui.png" alt="vista de la interfaz de la demo" width="99%">
</p>

---

## Por qué es rápida

- **Poda por manifiesto:** el manifiesto de cada versión guarda los rangos
  temporales por segmento y los mínimos y máximos de cada columna. Una consulta
  estrecha descarta segmentos enteros antes de abrir un solo archivo.
- **Orden declarado:** los segmentos se almacenan ordenados por tiempo y la capa
  de consulta se lo comunica a DataFusion. Las agregaciones OHLCV se procesan en
  flujo en vez de ordenar antes 20 M de filas (algo que sí paga toda la
  competencia), y el ASOF join no necesita ordenar.
- **Segmentos inmutables:** los metadatos del pie de página se cachean sin
  condiciones (algo válido porque los segmentos nunca cambian), lo que recorta
  cerca del 40 % de los escaneos en caliente.
- **Estados de agregación conscientes de la versión:** las agregaciones
  OHLCV/VWAP persisten estados combinables por segmento inmutable; volver a
  consultar los combina en milisegundos en lugar de recalcular, y solo escanea
  los segmentos recién añadidos.
- **Replay perezoso:** el núcleo de backtest tira de los registros de uno en uno
  en lugar de materializar una ventana, así que la memoria se mantiene plana tanto
  si una ejecución reproduce un día como cien millones de eventos.
- **Emparejamiento de órdenes indexado:** las órdenes en el libro se indexan por
  mercado y precio, así que un nuevo print solo despierta las que realmente cruza,
  en vez de repasar todas las abiertas.

---

## Por qué para agentes

- **Entradas reproducibles:** cada lectura se resuelve a una versión, de modo
que "qué datos vio esta ejecución" tiene respuesta, y repetirla contra esa
versión es O(1) en lugar de un trabajo de arqueología.
- **Que un resultado no arrase la ventana de contexto.** `H5I_DB_PROFILE=agent`
limita cada consulta y vuelca el resto a Parquet, informando del número real de
filas y de dónde quedaron las que se retuvieron.
- **Errores sobre los que se puede actuar:** el sobre de stderr lleva
`next_actions` (comandos ejecutables), `did_you_mean` para las erratas y un
indicador `retryable`.
- **Bifurcar sin copiar.** `fork` abre un espacio de trabajo escribible sobre una
vista fijada de todas las tablas y no duplica ningún dato, así que una edición o
un experimento cuestan un archivo pequeño y descartarlos sale tan barato como
conservarlos.
- **Control de privilegios.** Las mutaciones se previsualizan con `plan`/`apply`
y la política puede exigir ese paso; `--idempotency-key` hace que una ingesta
reintentada se repita; una `data-policy` opcional rechaza en cerrado las filas
mal formadas.
- **Una ejecución de backtest es una rama.** Cada ejecución corre dentro de su
propio fork y escribe allí sus órdenes, ejecuciones, posiciones y curva
de patrimonio como tablas normales. Así, dos ejecuciones se comparan al nivel de
ejecución con `fork_diff`, un barrido entero se agrega en una sola consulta entre
forks, la que merece la pena se `promote` y el resto se descarta.
- **La superficie de revisión reparte atención en vez de clasificar.** `h5i-db ui`
ordena las pruebas por lo que necesita a una persona a continuación: decisión
requerida, luego fallidas o con avisos, luego terminadas y no vistas, luego en
ejecución, luego vistas. Recorrer una lista no marca el trabajo como revisado; una
prueba cuenta como vista solo cuando se abre su detalle.

---

## Cuándo *no* usar h5i-db

- **Almacenes distribuidos de varios terabytes:** por diseño es de un solo nodo
  y embebida. Para eso están ClickHouse, Snowflake o un lakehouse.
- **OLTP o servicio con alta concurrencia:** un escritor cada vez, sin MVCC a
  nivel de fila ni transacciones interactivas. Usa Postgres.
- **Captura de ticks por debajo del microsegundo:** la cadencia de escritura
  para la que está pensada son barras de un minuto, cierres de jornada y
  archivos de proveedores, no la capa de captura en sí. Ese es el terreno de
  kdb+.
- **Bases de datos sin columna temporal:** todo el diseño presupone un índice
  temporal; sin él pierdes la poda, el ASOF join y las lecturas point-in-time.
- **Operar en real:** el backtester nunca enruta una orden de verdad. No hay
  adaptadores de bróker, ni optimizador de cartera, ni API de gráficos; el límite
  es la simulación y la evaluación.

---

## Benchmark

Metodología y resultados completos en [benchmarks](benchmarks).

**Base de datos**

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| Versionado / viaje en el tiempo de cara al usuario | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (lecturas de versión en O(1)) |
| SQL con joins/ventanas/CTE | ✓ | parcial | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| ASOF join | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴ (sin ordenación sobre almacenamiento ordenado) |
| Mutaciones previsualizables (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, exigible por política |
| Escritores concurrentes | MVCC | n/d | n/d | n/d | inseguro³ | CAS + conflicto explícito |
| Escaneo de rango temporal estrecho, 20 M filas | 45,5 ms | 28,1 ms | 23,9 ms | 22,8 ms | **4,2 ms**⁵ | 10,0 ms |
| OHLCV+VWAP de 1 min, 20 M filas | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| ASOF join por símbolo, 20 M filas | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |


¹ La sintaxis `AT (VERSION …)` existe, pero el almacenamiento nativo la rechaza.
² Existe un `join_asof` experimental, pero es unas 1000× más lento: inviable a esta escala.
³ Asume, y así lo documenta, un único escritor por símbolo.
⁴ Sintaxis SQL nativa `ASOF JOIN … MATCH_CONDITION` y una función de tabla
  `asof_join(...)` (en SQL y en Python).
⁵ El índice temporal nativo de ArcticDB gana en lecturas puntuales estrechas
  desde su propio almacén LMDB; la poda por manifiesto de h5i-db queda segunda
  y supera a todos los motores generalistas.

**Backtesting**

| motor | frontera medida | mediana | rendimiento |
|---|---|---:|---:|
| **h5i-db** | registros decodificados por el núcleo de replay | **65,7 ms** | **3,05 M eventos/s** |
| **h5i-db** | ejecución persistida completa: escaneo, decodificación, fork, replay, escritura | 331 ms | 605 k eventos/s |
| NautilusTrader 1.230.0 | objetos en memoria por `BacktestEngine.run()` | 767 ms | 261 k eventos/s |
| LEAN `11ba019f6` | del primer callback `Slice` a `OnEndOfAlgorithm`, desde disco | 2033 ms | 98,4 k eventos/s |

Medianas de tres ejecuciones en procesos nuevos tras un calentamiento, y cada
adaptador verifica que vio los 200 k eventos y envió las 200 órdenes. Las fronteras
medidas difieren, como dice la columna: el benchmark comprueba recuentos de eventos
y de órdenes, no equivalencia de PnL, y Nautilus invoca un callback de estrategia en
Python por cada cotización mientras los otros dos ejecutan código nativo.

---

## Desarrollo

```bash
cargo test --workspace          # ~290 pruebas, incl. inyección de fallos de seguridad ante caídas
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # frente a NautilusTrader y LEAN
```

Crates del workspace en `crates/`: `core` (núcleo de almacenamiento versionado),
`query` (capa DataFusion), `backtest` (núcleo de replay, modelos de venue,
liquidación), `venues` (cargadores de Kalshi, Polymarket e Hyperliquid), `cli` (el
binario de cara al agente), `ui` (superficie de revisión), `observability`,
`python` (`pip install h5i-db`), `bench`.

---

## Licencia

Apache-2.0. Véase [LICENSE](./LICENSE).
