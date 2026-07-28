# h5i-db

[English](README.md) · **Español** · [Français](README.fr.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**Una base de datos de series temporales rápida y nativa para agentes,
pensada para la investigación cuantitativa. Embebida y escrita en Rust.**

- **Rápida en la forma de las series temporales:** más de 4,5× más rápida que
  DuckDB y Polars en agregaciones OHLCV+VWAP sobre 20 M de filas.
- **SQL nativo de series temporales:** ASOF join, `time_bucket` con zonas
  horarias, gapfill/resample, ventanas móviles, `vwap`, `ewma`.
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
- **Lecturas point-in-time:** fija un instante de decisión y el marco de datos
  que llega a pandas no podrá contener filas posteriores a él. Sin sesgo de
  anticipación, por construcción.
- **Embebida:** un directorio, sin servidor ni demonio. Apache-2.0.

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
h5i-db ui market.db                                                # superficie de revisión
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

---

## Desarrollo

```bash
cargo test --workspace          # ~290 pruebas, incl. inyección de fallos de seguridad ante caídas
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
```

Crates del workspace en `crates/`: `core` (núcleo de almacenamiento versionado),
`query` (capa DataFusion), `cli` (el binario de cara al agente), `ui` (superficie
de revisión), `python` (`pip install h5i-db`), `bench`.

---

## Licencia

Apache-2.0. Véase [LICENSE](./LICENSE).
