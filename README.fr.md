# h5i-db

[English](README.md) · [Español](README.es.md) · **Français** · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**Une base de données de séries temporelles rapide et pensée pour les agents,
au service de la recherche quantitative. Embarquée, écrite en Rust.**

- **Rapide sur la forme des séries temporelles :** plus de 4,5× plus rapide que
  DuckDB et Polars sur des agrégations OHLCV+VWAP portant sur 20 M de lignes.
- **SQL natif pour les séries temporelles :** jointure ASOF, `time_bucket`
  sensible aux fuseaux horaires, gapfill/resample, fenêtres glissantes, `vwap`,
  `ewma`.
- **Forkez une base en quelques millisecondes :** les forks partagent les
  données au lieu de les copier. Un agent peut enchaîner de larges boucles
  d'essai et d'erreur (forker, muter, évaluer, jeter) pour un coût quasi nul.
- **Chaque écriture est un commit atomique et versionné :** n'importe quelle
  version passée se lit en O(1), donc une ingestion ratée (humaine ou
  automatique) s'annule d'un seul `restore`.
- **Des politiques de sécurité pour les écritures d'agents :** mutations
  prévisualisables, garde-fous par politique, contraintes qui échouent en
  position fermée et bloquent les opérations destructrices, et une piste d'audit
  indiquant ce qui a changé et pourquoi.
- **Lectures point-in-time :** fixez un instant de décision et la trame qui
  parvient à pandas ne pourra contenir aucune ligne postérieure. Aucun biais
  d'anticipation, par construction.
- **Embarquée :** un répertoire, sans serveur ni démon. Apache-2.0.

📖 **[Documentation](https://db.h5i.dev/manual/)** · [Manuel](https://db.h5i.dev/manual/) · [API Python](https://db.h5i.dev/api/) ·
[Livre de recettes](https://github.com/h5i-dev/h5i-db-cookbook) · [Skill pour agents](skills/h5i-db/SKILL.md)

---

## Démarrage rapide

**CLI**

```bash
cargo install h5i-db-cli
```

```bash
h5i-db init market.db
h5i-db create-table market.db trades --like ticks.parquet --time-column ts
h5i-db ingest market.db trades ticks.parquet --idempotency-key load-1
h5i-db context market.db                                           # se repérer en un seul appel
h5i-db query market.db "SELECT symbol, vwap(price,size) FROM trades GROUP BY symbol"
h5i-db query market.db "SELECT count(*) FROM trades" \
  --decision-time 2026-07-01T00:00:00Z                             # le futur est illisible
h5i-db ui market.db                                                # surface de revue
```

**Bibliothèque Python**

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
old = db.read("trades", version=1)                # voyage dans le temps : lire n'importe quelle version passée

plan = db.plan_delete_range("trades", 1_700_0_000_000)
print(plan.summary)                               # prévisualiser la mutation avant qu'elle n'atterrisse
plan.apply()
```

**Skill pour agents** (Claude Code, Codex, Cursor, …)

```bash
npx skills add h5i-dev/h5i-db        # installe la skill h5i-db depuis skills/h5i-db/
```

---

## Pourquoi

| | DuckDB | Polars | pandas | PyArrow | ArcticDB | **h5i-db** |
|---|---|---|---|---|---|---|
| Versionnement / voyage dans le temps exposés à l'utilisateur | ✗¹ | ✗ | ✗ | ✗ | ✓ | ✓ (lecture d'une version en O(1)) |
| SQL avec jointures/fenêtres/CTE | ✓ | partiel | ✗ | ✗ | ✗ | ✓ (DataFusion) |
| Jointure ASOF | ✓ | ✓ | ✓ | ✗² | ✗ | ✓⁴ (sans tri, sur un stockage trié) |
| Mutations prévisualisables (plan/apply) | ✗ | ✗ | ✗ | ✗ | ✗ | ✓, imposable par politique |
| Écrivains concurrents | MVCC | s.o. | s.o. | s.o. | non sûr³ | CAS + conflit explicite |
| Balayage d'une plage temporelle étroite, 20 M lignes | 45,5 ms | 28,1 ms | 23,9 ms | 22,8 ms | **4,2 ms**⁵ | 10,0 ms |
| OHLCV+VWAP à la minute, 20 M lignes | 7237 ms | 7309 ms | 5115 ms | 7121 ms | 3504 ms | **1558 ms** |
| Jointure ASOF par symbole, 20 M lignes | 11566 ms | **1485 ms** | 6624 ms | ✗² | 7008 ms | 1548 ms |

¹ La syntaxe `AT (VERSION …)` existe, mais le stockage natif la rejette.
² Un `join_asof` expérimental existe, mais il est environ 1000× plus lent :
  inutilisable à cette échelle.
³ Repose sur une hypothèse documentée d'un seul écrivain par symbole.
⁴ Syntaxe SQL native `ASOF JOIN … MATCH_CONDITION` et fonction de table
  `asof_join(...)` (en SQL comme en Python).
⁵ L'index temporel natif d'ArcticDB l'emporte sur les lectures ponctuelles
  étroites depuis son propre magasin LMDB ; l'élagage par manifeste de h5i-db
  arrive second et devance tous les moteurs généralistes.

Méthodologie complète dans [benchmarks/RESULTS.md](benchmarks/RESULTS.md).

---

## Pourquoi c'est rapide

- **Élagage par manifeste :** le manifeste de chaque version porte les plages
  temporelles par segment ainsi que les minimums et maximums de chaque colonne.
  Une requête étroite écarte des segments entiers avant même d'ouvrir un
  fichier.
- **Ordre de tri déclaré :** les segments sont stockés triés par le temps et la
  couche de requête l'indique à DataFusion. Les agrégations OHLCV se traitent en
  flux au lieu de trier d'abord 20 M de lignes (ce que paie chaque référence),
  et la jointure ASOF se passe de tri.
- **Segments immuables :** les métadonnées de pied de fichier sont mises en
  cache sans condition, ce qui est correct puisque les segments ne changent
  jamais, et retire environ 40 % du temps des balayages à chaud.
- **États d'agrégation conscients de la version :** les agrégations OHLCV/VWAP
  persistent des états fusionnables par segment immuable ; une nouvelle requête
  les fusionne en quelques millisecondes au lieu de tout recalculer, et ne
  balaie que les segments récemment ajoutés.
- **Aucune prouesse bas niveau :** les balayages et agrégations génériques
  s'exécutent sur un DataFusion standard et rivalisent avec les meilleurs
  moteurs ; h5i-db n'ajoute de la structure que là où la forme des séries
  temporelles rend cette structure payante.

---

## Pourquoi pour les agents

- **Des entrées reproductibles :** chaque lecture se résout en une version, si
bien que « quelles données cette exécution a-t-elle vues » a une réponse, et
rejouer contre cette version relève du O(1) plutôt que de l'archéologie.

- **Extractions point-in-time :** le point de lecture se fixe sur deux axes : le
temps de l'événement (`--decision-time`) et l'arrivée (`--as-of`). La trame que
vous passez à pandas est alors bornée à la source, le seul endroit où une borne
survit au passage vers Python. `arrival-delta` mesure après coup la part d'un
résultat qui dépendait de données arrivées plus tard.

- **Qu'un résultat ne détruise pas la fenêtre de contexte.**
`H5I_DB_PROFILE=agent` plafonne chaque requête et déverse le reste en Parquet,
en indiquant le vrai nombre de lignes et l'endroit où se trouvent celles qui ont
été retenues.

- **Un seul appel pour se repérer :** `h5i-db context <db>` renvoie le schéma, la
taille, la plage temporelle et la version courante de chaque table, les
garde-fous de la politique d'exploitation, ainsi que tout plan déjà préparé.

- **Des erreurs sur lesquelles on peut agir :** l'enveloppe envoyée sur stderr
porte `next_actions` (des commandes exécutables), `did_you_mean` pour les fautes
de frappe, et un indicateur `retryable`.

- **Bifurquer sans copier.** `fork` ouvre un espace de travail inscriptible
au-dessus d'une vue figée de chaque table sans dupliquer la moindre donnée : une
modification ou une expérience coûte un petit fichier et se jette aussi
facilement qu'elle se garde. `forks('trades')` lit ensuite cette table sur
toutes les branches à la fois, avec une colonne `__fork`, de sorte que comparer
ce que chacune a produit ne demande aucune étape d'export.

- **L'erreur coûte peu.** Les mutations se prévisualisent via `plan`/`apply` et
la politique peut imposer ce passage ; `--idempotency-key` fait qu'une ingestion
relancée rejoue au lieu d'ajouter deux fois ; une `data-policy` optionnelle
rejette en position fermée les lignes mal formées ; les commits font un fsync
avant l'échange et chaînent des empreintes de manifeste, ce qui est vérifié en
tuant l'écrivain à chaque étape.

---

## Quand *ne pas* utiliser h5i-db

- **Entrepôts distribués de plusieurs téraoctets :** mononœud et embarquée par
  conception. Tournez-vous vers ClickHouse, Snowflake ou un lakehouse.
- **OLTP ou service à forte concurrence :** un seul écrivain à la fois, pas de
  MVCC au niveau de la ligne, pas de transactions interactives. Prenez Postgres.
- **Capture de ticks sous la microseconde :** la cadence d'écriture visée, ce
  sont les barres à la minute, les clôtures de journée et les fichiers de
  fournisseurs, pas la couche de capture elle-même. C'est le domaine de kdb+.
- **Bases sans colonne temporelle :** toute la conception suppose un index
  temporel ; sans lui, vous perdez l'élagage, la jointure ASOF et les lectures
  point-in-time.

---

## Développement

```bash
cargo test --workspace          # ~290 tests, dont l'injection de fautes pour la sûreté au crash
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
```

Crates du workspace, sous `crates/` : `core` (noyau de stockage versionné),
`query` (couche DataFusion), `cli` (le binaire exposé aux agents), `ui` (surface
de revue), `python` (`pip install h5i-db`), `bench`.

---

## Licence

Apache-2.0. Voir [LICENSE](./LICENSE).
