# h5i-db

[English](README.md) · [Español](README.es.md) · **Français** · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

**Une base de données de séries temporelles et un moteur de backtesting rapides
et pensés pour les agents, au service de la recherche quantitative. Embarqués,
écrits en Rust.**

- **Rapide sur la forme des séries temporelles :** plus de 4,5× plus rapide que
  DuckDB et Polars sur des agrégations OHLCV+VWAP portant sur 20 M de lignes.
- **SQL natif pour les séries temporelles :** jointure ASOF, `time_bucket`
  sensible aux fuseaux horaires, gapfill/resample, fenêtres glissantes, `vwap`,
  `ewma`.
- **Lectures point-in-time :** fixez un instant de décision et la trame qui
  parvient à pandas ne pourra contenir aucune ligne postérieure. Aucun biais
  d'anticipation, par construction.
- **Un backtester événementiel efficace :** 3,05 M d'événements/s à travers le
  noyau de rejeu, soit 11,7× NautilusTrader et 31× LEAN sur une charge partagée
  portant sur le haut du carnet.
- **Prise en charge native des places :** [Kalshi](#sources-de-données), [Polymarket](#sources-de-données), [Hyperliquid](#sources-de-données), [Binance](#sources-de-données) et plus.
- **Analyse statistique professionnelle :** métriques de facteurs et de
  performance à parité `alphalens` et `empyrical`, plus le Sharpe dégonflé et la
  détection de la probabilité de surapprentissage.
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

📖 **[Documentation](https://db.h5i.dev/manual/)** · [Backtesting](https://db.h5i.dev/manual/backtest/) · [Quantitatif](https://db.h5i.dev/manual/quant/) · [API Python](https://db.h5i.dev/api/) ·
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
h5i-db ui market.db                                                # revue et expériences
```

**Bibliothèque Python pour DataFrames et SQL**

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

**Bibliothèque Python pour le backtesting** (la même installation, sans serveur)

```python
from h5i_db import backtest

config = backtest.BacktestConfig(
    run_id="momentum-001",
    data=backtest.DataConfig(signals="signals", snapshot="2024-q1"),   # l'ancrage
    portfolio=backtest.PortfolioConfig(starting_cash=100_000.0),
    execution=backtest.ExecutionConfig(fee_kind="kalshi", fee_rate=0.07),
    risk=backtest.RiskConfig(max_order_quantity=500.0),
)

backtest.inspect(db, config).raise_for_errors()  # refuse ce que les données ne soutiennent pas
result = backtest.execute(db, config)            # rejoue dans le fork « bt-momentum-001 »

result.summary()                  # exécutions, trésorerie finale, jusqu'où il a vraiment simulé
result.explain()                  # pourquoi des ordres ont été rejetés ou jamais exécutés
result.fills                      # en Arrow, ou interrogez-le : SELECT * FROM bt_fills
result.tearsheet("run.html")
result.verify()                   # rejoue la configuration enregistrée et compare
```

Une grille de paramètres devient un fork par essai, et le gagnant se classe sans
aucune étape d'export. Donnez-lui des fenêtres d'entraînement et de validation
explicites et chaque essai jouera les deux phases, si bien que le classement se
lit hors échantillon :

```python
board = backtest.study(
    db, study_id="fees", base=config,
    parameters={"execution.fee_rate": [0.0, 0.02, 0.07]},
    validation=backtest.ValidationWindows(
        train=("2024-01-01", "2024-04-01"), holdout=("2024-04-01", "2024-07-01")
    ),
).leaderboard("holdout_final_cash")
```

**Skill pour agents** (Claude Code, Codex, Cursor, …)

```bash
npx skills add h5i-dev/h5i-db        # installe la skill h5i-db depuis skills/h5i-db/
```

**Le voir tourner**

```bash
python examples/agent_swarm_demo.py   # trois agents, onze essais, puis l'interface
```

Lance une flotte sur un seul jeu de données figé : un balayage de seuils, une
échelle de coûts d'exécution, et une validation marquée pour signature humaine.

<p align="center">
  <img src="./docs/_static/backtest-ui.png" alt="vue de l'interface de la démo" width="99%">
</p>

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
- **Rejeu paresseux :** le noyau de backtest tire les enregistrements un par un
  au lieu de matérialiser une fenêtre, si bien que la mémoire reste plate que la
  course rejoue une journée ou cent millions d'événements.
- **Appariement d'ordres indexé :** les ordres en carnet sont indexés par marché et
  par prix, si bien qu'un nouveau print ne réveille que ceux qu'il croise vraiment,
  au lieu de repasser sur tous les ordres ouverts.

---

## Pourquoi pour les agents

- **Des entrées reproductibles :** chaque lecture se résout en une version, si
bien que « quelles données cette exécution a-t-elle vues » a une réponse, et
rejouer contre cette version relève du O(1) plutôt que de l'archéologie.
- **Qu'un résultat ne détruise pas la fenêtre de contexte.**
`H5I_DB_PROFILE=agent` plafonne chaque requête et déverse le reste en Parquet,
en indiquant le vrai nombre de lignes et l'endroit où se trouvent celles qui ont
été retenues.
- **Des erreurs sur lesquelles on peut agir :** l'enveloppe envoyée sur stderr
porte `next_actions` (des commandes exécutables), `did_you_mean` pour les fautes
de frappe, et un indicateur `retryable`.
- **Bifurquer sans copier.** `fork` ouvre un espace de travail inscriptible
au-dessus d'une vue figée de chaque table sans dupliquer la moindre donnée : une
modification ou une expérience coûte un petit fichier et se jette aussi
facilement qu'elle se garde.
- **Contrôle des privilèges.** Les mutations se prévisualisent via `plan`/`apply`
et la politique peut imposer ce passage ; `--idempotency-key` fait qu'une ingestion
relancée rejoue ; une `data-policy` optionnelle rejette en position fermée les
lignes mal formées.
- **Une exécution de backtest est une branche.** Chaque exécution se déroule dans
son propre fork et y écrit ses ordres, exécutions, positions et
courbe de capital comme des tables ordinaires. Deux exécutions se comparent donc au
niveau de l'exécution avec `fork_diff`, un balayage entier s'agrège en une seule
requête inter-forks, celle qui en vaut la peine est `promote`, et le reste est jeté.
- **La surface de revue répartit l'attention plutôt qu'elle ne classe.**
`h5i-db ui` trie les essais selon ce qui réclame un humain ensuite : décision
requise, puis en échec ou avec avertissement, puis terminés et non vus, puis en
cours, puis vus. Parcourir une liste ne marque rien comme revu ; un essai ne compte
comme vu que lorsque son détail est ouvert.

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
- **Le trading réel :** le backtester ne route jamais un ordre véritable. Pas
  d'adaptateurs de courtier, pas d'optimiseur de portefeuille, pas d'API de tracé ;
  la frontière, c'est la simulation et l'évaluation.

---

## Sources de données

Les chargeurs lisent des fichiers et des réponses que vous avez déjà. Rien ici ne
télécharge : les identifiants, les reprises et les limites de débit restent dans
votre script.

| Source | Carnet d'ordres | Transactions | Barres | Aussi |
|---|---|---|---|---|
| Kalshi | ✓ | ✓ | ✓ | règlement |
| Polymarket | ✓ | ✓ | dérivées | règlement, émission et rachat d'un ensemble complet |
| Hyperliquid | ✓ | ✓ | ✓ | financement, prix de marque et d'oracle, plafonds de levier |
| Limitless | ✓ | ✓ | dérivées | |
| Opinion | ✓ | ✓ | dérivées | |
| Manifold | s.o. | ✓ | dérivées | règlement |
| Binance | | ✓ | ✓ | exports en masse spot et futures |
| Tout export OHLCV | | | ✓ | un CSV de courtier, `yfinance`, Stooq |
| Tout export de transactions | | ✓ | dérivées | |
| Séries publiées | | | | prix de référence, pour un taux ou un indice |
| Opérations sur titres | | | | divisions, dividendes, retraits de cote |

`dérivées` signifie que les barres sont agrégées à partir des transactions de la
source elle-même plutôt que téléchargées : les trous restent visibles sous forme
de barres absentes. `s.o.` signifie que la place n'a pas cette notion : Manifold
est un teneur de marché automatisé, il a donc des transactions mais pas de
carnet. Voir le [guide des places](crates/h5i-db-venues/README.md) pour le détail.

---

## Benchmark

Méthodologie et résultats complets dans [benchmarks](benchmarks).

**Base de données**

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

**Backtesting**

| moteur | frontière mesurée | médiane | débit |
|---|---|---:|---:|
| **h5i-db** | enregistrements décodés à travers le noyau de rejeu | **65,7 ms** | **3,05 M évén./s** |
| h5i-db `wide` | même noyau, virgule fixe 128 bits | 94 ms⁷ | 2,13 M évén./s⁷ |
| **h5i-db** | même noyau, stratégie en callback Python par événement | 278 ms⁶ | 719 k évén./s⁶ |
| h5i-db `wide` | idem, virgule fixe 128 bits | 306 ms⁶ ⁷ | 653 k évén./s⁶ ⁷ |
| **h5i-db** | exécution persistée complète : balayage, décodage, fork, rejeu, écriture | 280 ms | 713 k évén./s |
| h5i-db `wide` | idem, virgule fixe 128 bits | 280 ms | 713 k évén./s |
| NautilusTrader 1.230.0 | objets en mémoire à travers `BacktestEngine.run()` | 767 ms | 261 k évén./s |
| LEAN `11ba019f6` | du premier callback `Slice` à `OnEndOfAlgorithm`, alimenté par disque | 2033 ms | 98,4 k évén./s |

Médianes de trois exécutions en processus neufs après un échauffement ; chaque
adaptateur vérifie qu'il a vu les 200 k événements et soumis les 200 ordres. Les
frontières mesurées diffèrent, comme l'indique la colonne, et le benchmark vérifie
des comptages plutôt qu'une équivalence de PnL.
⁶ Les autres lignes n'appellent jamais Python ; celle-ci y passe à chaque
événement, comme Nautilus. Callback contre callback, l'écart est de 3,1× et
non de 13×. Chiffre dérivé (noyau natif plus coût de frontière mesuré), non
chronométré directement.
⁷ `--features wide`, désactivé par défaut ; voir
[Précision et plage](https://db.h5i.dev/manual/backtest/). Chiffre dérivé, non
chronométré directement ; méthode dans
[RESULTS.md](benchmarks/backtest_compare/RESULTS.md).

---

## Développement

```bash
cargo test --workspace          # ~290 tests, dont l'injection de fautes pour la sûreté au crash
cargo run -p h5i-db-bench --profile bench-fast -- --trades 1000000
cargo run -p h5i-db-bench --profile bench-fast --bin h5i-db-fork-bench
python3 benchmarks/backtest_compare/run.py \
  --output benchmarks/backtest_compare/results.json   # face à NautilusTrader et LEAN
```

Crates du workspace, sous `crates/` : `core` (noyau de stockage versionné),
`query` (couche DataFusion), `backtest` (noyau de rejeu, modèles de venue,
règlement), `venues` (chargeurs Kalshi, Polymarket, Hyperliquid), `cli` (le binaire
exposé aux agents), `ui` (surface de revue), `observability`, `python`
(`pip install h5i-db`), `bench`.

---

## Licence

Apache-2.0. Voir [LICENSE](./LICENSE).
