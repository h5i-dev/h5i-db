# h5i-db

[English](README.md) · **Español** · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**Una base de datos de series temporales y un motor de backtesting rápidos y
nativos para agentes, pensados para la investigación cuantitativa. Embebidos y
escritos en Rust.**

- (DB) **Rápida en la forma de las series temporales:** más de 4,5× más rápida que
  DuckDB y Polars en agregaciones OHLCV+VWAP sobre 20 M de filas.
- (DB) **SQL nativo de series temporales:** ASOF join, `time_bucket` con zonas
  horarias, gapfill/resample, ventanas móviles, `vwap`, `ewma`.
- (DB) **Lecturas point-in-time:** fija un instante de decisión y el marco de datos
  que llega a pandas no podrá contener filas posteriores a él. Sin sesgo de
  anticipación, por construcción.
- (BT) **Backtester orientado a eventos y eficiente:** 3,05 M de eventos/s a través
  del núcleo de replay, 11,7× NautilusTrader y 31× LEAN en una carga compartida
  sobre el tope del libro.
- (BT) **Soporte nativo de los mercados más usados:** los payloads de Kalshi,
  Polymarket e Hyperliquid se decodifican en un único conjunto canónico de tablas,
  cada uno con la curva de comisiones y el funding reales del venue en lugar de un
  `nocional × tasa` genérico.
- (BT) **Las estadísticas de siempre, y cuánto fiarse de ellas:** las cifras de
  factores y de rendimiento coinciden con `alphalens` y `empyrical`; el Sharpe
  deflactado y la probabilidad de sobreajuste dicen cuánto de un resultado fue
  solo la búsqueda que lo encontró.
- (AI) **Bifurca una base de datos en milisegundos:** los forks comparten los datos
  en lugar de copiarlos. Un agente puede recorrer ciclos amplios de ensayo y
  error (bifurcar, mutar, evaluar, descartar) a un coste casi nulo.
- (AI) **Cada escritura es un commit atómico y versionado:** cualquier versión
  pasada se lee en O(1), así que una ingesta defectuosa (humana o de un agente)
  se deshace con un solo `restore`.
- (AI) **Políticas de seguridad para las escrituras de agentes:** mutaciones
  previsualizables, controles por política, restricciones que fallan en cerrado
  y bloquean las operaciones destructivas, y un registro de auditoría de qué
  cambió y por qué.

📖 **[Documentación](https://db.h5i.dev/manual/)** · [Manual](https://db.h5i.dev/manual/) · [API de Python](https://db.h5i.dev/api/) ·
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

**Biblioteca de Python**

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

**Backtesting** (la misma instalación, sin servidor ni una tubería de datos aparte)

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

El mismo contrato tipado funciona desde el shell, así que un archivo de
configuración es la receta completa de reproducción:

```bash
python -m h5i_db.backtest inspect market.db config.json   # fidelidad y hallazgos previos
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
```

**Skill para agentes** (Claude Code, Codex, Cursor, …)

```bash
npx skills add h5i-dev/h5i-db        # instala la skill de h5i-db desde skills/h5i-db/
```

---

## Por qué

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

Metodología completa en [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

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
- **Sin heroicidades a bajo nivel:** los escaneos y agregaciones genéricos
  corren sobre DataFusion estándar y empatan con los mejores motores; h5i-db
  solo añade estructura allí donde la forma de las series temporales hace que
  esa estructura rinda.

---

## Flujos de trabajo cuantitativos

`h5i_db.quant` ejecuta el ciclo de investigación habitual contra el motor, y cada
resultado registra la versión de los datos a partir de la que se calculó.

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")     # el anclaje

panel.ic()                  # IC de rango por fecha, una columna por horizonte
panel.quantile_returns()    # retorno futuro medio por cubeta
quant.factor_report(panel, path="factor.html")
```

Las estadísticas de factores coinciden con `alphalens-reloaded` y las de cartera
con `empyrical-reloaded`, así que los números son los que ya conoces; lo nuevo es
que son atribuibles. Un informe empieza por el SHA de la versión y el anclaje bajo
el que corrió, una ejecución sin anclar lo dice, y `quant.verify()` se niega a
certificar un resultado que no se puede reproducir.

Tres cosas se derivan de la capa de almacenamiento, no de la estadística:

- **`event_time_cutoff=`** restringe cada lectura a lo que era conocible en un
  instante de decisión, así que un retorno futuro que necesitaría un precio
  posterior se descarta en lugar de calcularse.
- **`quant.sweep()`** recorre una malla de parámetros con un fork por prueba, así
  que las pruebas no se contaminan entre sí y todas se comparan en una única
  consulta entre forks.
- **`quant.restatement_impact()`** repite un cálculo en dos versiones de los datos
  e informa de qué movió la revisión de un proveedor.

El sesgo de selección recibe estadísticas de primera clase y no una nota al pie,
porque un número hallado buscando vale menos que el mismo número hallado de una vez:

- **`quant.deflated_sharpe(returns, trials=N)`** descuenta un ratio de Sharpe por
  el tamaño de la búsqueda que lo encontró, y `minimum_track_record_length()` dice
  cuánto historial hace falta para que el ratio signifique algo.
- **`quant.probability_of_backtest_overfitting(matrix)`** ejecuta validación
  cruzada combinatoriamente simétrica: un PBO cercano a 0,5 significa que el
  ganador dentro de muestra no llevaba información.
- **`quant.purged_kfold()`**, **`combinatorial_purged()`** y **`walk_forward()`**
  parten según horizontes y embargo, así que una etiqueta que depende de las diez
  barras siguientes no puede filtrarse a su propio pliegue de entrenamiento. Los
  horizontes nunca se adivinan: omitirlos significa que las etiquetas son
  instantáneas.
- **`quant.fit_impact()`** calibra un modelo de slippage a partir de ejecuciones
  reales en lugar de suponer una constante de coste.

### Backtesting

`h5i-db-backtest` es un backtester orientado a eventos cuyo plano de datos es la
base de datos. Una ejecución corre dentro de un fork y escribe allí `bt_orders`,
`bt_fills`, `bt_positions` y `bt_equity`, así que los resultados se consultan con
el mismo SQL que los datos de mercado y dos ejecuciones se comparan al nivel de
ejecución con `fork_diff`.

```python
fork = db.fork("bt-momentum-001")
quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")
```

También es rápido, porque el replay lee registros ya decodificados directamente
de la capa de almacenamiento en vez de cruzar una frontera de lenguaje por evento.
Sobre una carga compartida (200 k actualizaciones del tope del libro, 200 órdenes
de mercado, un instrumento, y cada adaptador verificando que las vio todas):

| motor | frontera medida | mediana | rendimiento |
|---|---|---:|---:|
| **h5i-db** | registros decodificados por el núcleo de replay | **65,7 ms** | **3,05 M eventos/s** |
| **h5i-db** | ejecución persistida completa: escaneo, decodificación, fork, replay, escritura | 331 ms | 605 k eventos/s |
| NautilusTrader 1.230.0 | objetos en memoria por `BacktestEngine.run()` | 767 ms | 261 k eventos/s |
| LEAN `11ba019f6` | del primer callback `Slice` a `OnEndOfAlgorithm`, desde disco | 2033 ms | 98,4 k eventos/s |

Incluso la frontera persistida, que hace estrictamente más trabajo que las otras
dos, es 2,3× el motor en memoria de NautilusTrader y 6,1× el rendimiento medido
de los callbacks de LEAN. Esto es una carga estrecha orientada a eventos, no una
clasificación de sistemas de backtesting: las fronteras difieren y el benchmark
comprueba recuentos de eventos y de órdenes, no equivalencia de PnL. Metodología,
muestras en bruto y por qué cada frontera se trazó donde se trazó:
[benchmarks/backtest_compare/RESULTS.md](benchmarks/backtest_compare/RESULTS.md).

Qué cubre la simulación en sí:

- **Una ejecución es una función pura de (anclaje de datos, estrategia,
  configuración).** Sin reloj de pared, sin aleatoriedad sin semilla, sin iterar
  un mapa hash sin ordenar antes. `result.verify()` reejecuta una ejecución
  guardada e informa de si se reprodujo.
- **La anticipación se cierra por estructura, no por convención.** Los registros
  llevan `ts_event` y `ts_init` y se reproducen en orden de `ts_init`, así que los
  datos tardíos llegan tarde; una estrategia no tiene ninguna ruta hacia la
  resolución de un mercado.
- **La liquidación depende de la observabilidad.** Un replay de tres días sobre un
  mercado de seis meses termina con la posición abierta y dice por qué, en lugar
  de anotar un beneficio que nadie que operase esa ventana pudo cobrar. Los dos
  números sobreviven: el PnL a mercado y el liquidado, con la diferencia
  reportada como un ajuste explícito.
- **Las operaciones societarias se aplican hacia delante, nunca hacia atrás.**
  Nadie operó nunca al precio ajustado por split, así que splits, dividendos y
  exclusiones de cotización llegan como eventos en el instante en que surten
  efecto y actúan sobre posiciones, límites en el libro y valoraciones. Los
  factores de ajuste son datos point-in-time; una operación aún no anunciada
  simplemente no está en el flujo. Un ticker se resuelve a un instrumento sobre
  intervalos semiabiertos, y una consulta ambigua se rechaza nombrando los
  candidatos.
- **Las cuentas son multidivisa,** con margen, liquidación forzosa, funding de
  perpetuos, modificación de órdenes, prevención de auto-cruce y límites de riesgo
  previos al envío.
- **La comprobación previa rechaza lo que los datos no pueden sostener.**
  `backtest.inspect()` informa de una fidelidad de replay, y pedir ejecuciones por
  posición en cola a partir de instantáneas periódicas es un error, no un número
  con buena pinta.
- **Las estrategias vienen en tres formas:** tablas de señales o de comandos (la
  estrategia como dato, sin código de callback ni frontera de lenguaje en el
  bucle), callbacks `EventStrategy` de Python y el trait nativo `Strategy` de Rust.
- **Cobertura de mercados:** los mercados de predicción son el primer venue, con
  los mercados de N resultados como caso general, mediante cargadores de Kalshi,
  Polymarket e Hyperliquid que producen todos las mismas tablas canónicas.
  `KalshiFees` implementa la curva de comisiones cuadrática real, el redondeo a
  centicentavos y el acumulador de redondeo por ejecución parcial, no
  `nocional × tasa`.

Consulta las páginas del manual sobre [cuantitativa](https://db.h5i.dev/manual/quant/)
y [backtesting](https://db.h5i.dev/manual/backtest/).

---

## Por qué para agentes

- **Entradas reproducibles:** cada lectura se resuelve a una versión, de modo
que "qué datos vio esta ejecución" tiene respuesta, y repetirla contra esa
versión es O(1) en lugar de un trabajo de arqueología.

- **Extracciones point-in-time:** el punto de lectura se puede fijar en dos
ejes: tiempo del evento (`--decision-time`) y llegada (`--as-of`). El marco de
datos que entregas a pandas queda entonces acotado en el origen, que es el único
sitio donde una cota sobrevive al viaje hacia Python. `arrival-delta` mide, a
posteriori, cuánto de un resultado dependía de datos que llegaron después.

- **Que un resultado no arrase la ventana de contexto.** `H5I_DB_PROFILE=agent`
limita cada consulta y vuelca el resto a Parquet, informando del número real de
filas y de dónde quedaron las que se retuvieron.

- **Una sola llamada para orientarse:** `h5i-db context <db>` devuelve el esquema,
el tamaño, el rango temporal y la versión actual de cada tabla, los controles de
la política de operaciones y cualquier plan ya preparado.

- **Errores sobre los que se puede actuar:** el sobre de stderr lleva
`next_actions` (comandos ejecutables), `did_you_mean` para las erratas y un
indicador `retryable`.

- **Bifurcar sin copiar.** `fork` abre un espacio de trabajo escribible sobre una
vista fijada de todas las tablas y no duplica ningún dato, así que una edición o
un experimento cuestan un archivo pequeño y descartarlos sale tan barato como
conservarlos. Después, `forks('trades')` lee esa tabla en todas las ramas a la
vez con una columna `__fork`, de modo que comparar lo que produjo cada una no
necesita ningún paso de exportación.

- **Una prueba se identifica por su contenido, no por su nombre.** Un
`BacktestConfig` anclado y declarativo se resume en un `trial_digest` sobre todas
las entradas del replay, ignorando el id de la ejecución y los metadatos
descriptivos. Reenviar la misma prueba semántica devuelve el resultado registrado
con `cached=True` en lugar de bifurcar y reproducir otra vez, y la búsqueda más la
creación se serializan entre procesos de agentes locales, así que un bucle de
reintentos no puede gastar una segunda ejecución ni contar dos veces una puntuación.

- **La superficie de revisión reparte atención en vez de clasificar.** `h5i-db ui`
ordena las pruebas por lo que necesita a una persona a continuación: decisión
requerida, luego fallidas o con avisos, luego terminadas y no vistas, luego en
ejecución, luego vistas. Recorrer una lista no marca el trabajo como revisado; una
prueba cuenta como vista solo cuando se abre su detalle. La tabla de clasificación
es una pestaña aparte, porque "cuál es la mejor hasta ahora" y "cuál no he mirado"
son preguntas distintas.

- **Equivocarse sale barato.** Las mutaciones se previsualizan con `plan`/`apply`
y la política puede exigir ese paso; `--idempotency-key` hace que una ingesta
reintentada se repita en lugar de duplicar filas; una `data-policy` opcional
rechaza en cerrado las filas mal formadas; los commits hacen fsync antes del
intercambio y encadenan hashes de manifiesto, algo que se comprueba matando al
escritor en cada paso.

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
