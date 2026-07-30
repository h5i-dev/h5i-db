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
- **Un backtester événementiel sur le même stockage :** 3,05 M d'événements/s à
  travers le noyau de rejeu, soit 11,7× NautilusTrader et 31× LEAN sur une charge
  partagée portant sur le haut du carnet. Une exécution se déroule dans un fork et
  y réécrit ses ordres, exécutions, positions et courbe de capital sous forme de
  tables interrogeables ordinaires.
- **Des statistiques qui annoncent leur propre fiabilité :** évaluation de
  facteurs à parité `alphalens` et statistiques de performance à parité
  `empyrical`, plus le Sharpe dégonflé, la probabilité de surapprentissage du
  backtest et la validation croisée purgée et combinatoire.
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
h5i-db ui market.db                                                # revue et expériences
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

**Backtesting** (la même installation, sans serveur ni pipeline de données à part)

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

Le même contrat typé s'utilise depuis le shell : un fichier de configuration
constitue donc à lui seul la recette de reproduction.

```bash
python -m h5i_db.backtest inspect market.db config.json   # fidélité et constats préalables
python -m h5i_db.backtest run     market.db config.json
python -m h5i_db.backtest report  market.db momentum-001 --output run.html
python -m h5i_db.backtest verify  market.db momentum-001
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

## Flux de travail quantitatifs

`h5i_db.quant` exécute la boucle de recherche habituelle contre le moteur, et
chaque résultat enregistre la version des données à partir de laquelle il a été
calculé.

```python
from h5i_db import quant

panel = quant.build_panel(db, "signals", "prices",
                          periods=(1, 5, 10), quantiles=5,
                          snapshot="2024-q1")     # l'ancrage

panel.ic()                  # IC de rang par date, une colonne par horizon
panel.quantile_returns()    # rendement futur moyen par quantile
quant.factor_report(panel, path="factor.html")
```

Les statistiques de facteurs correspondent à `alphalens-reloaded` et celles de
portefeuille à `empyrical-reloaded` : les chiffres sont donc ceux auxquels vous
faites déjà confiance. Ce qui est nouveau, c'est qu'ils sont attribuables. Un
rapport s'ouvre sur le SHA de la version et l'ancrage sous lequel il a tourné, une
exécution non ancrée le dit, et `quant.verify()` refuse de certifier un résultat
qu'on ne peut pas reproduire.

Trois choses découlent de la couche de stockage plutôt que de la statistique :

- **`event_time_cutoff=`** restreint chaque lecture à ce qui était connaissable à
  un instant de décision : un rendement futur qui exigerait un prix ultérieur est
  écarté plutôt que calculé.
- **`quant.sweep()`** parcourt une grille de paramètres avec un fork par essai :
  les essais ne peuvent pas se contaminer, et tous se comparent dans une seule
  requête inter-forks.
- **`quant.restatement_impact()`** rejoue un même calcul sur deux versions des
  données et rapporte ce que la révision d'un fournisseur a déplacé.

Le biais de sélection reçoit de vraies statistiques et non une note de bas de page,
parce qu'un chiffre trouvé en cherchant vaut moins que le même chiffre trouvé du
premier coup :

- **`quant.deflated_sharpe(returns, trials=N)`** dégonfle un ratio de Sharpe selon
  l'ampleur de la recherche qui l'a produit, et `minimum_track_record_length()` dit
  quelle longueur d'historique il faut avant que ce ratio veuille dire quelque chose.
- **`quant.probability_of_backtest_overfitting(matrix)`** exécute une validation
  croisée combinatoirement symétrique : un PBO proche de 0,5 signifie que le
  gagnant en échantillon ne portait aucune information.
- **`quant.purged_kfold()`**, **`combinatorial_purged()`** et **`walk_forward()`**
  découpent selon les horizons et l'embargo : une étiquette qui dépend des dix
  barres suivantes ne peut pas fuiter dans son propre pli d'entraînement. Les
  horizons ne sont jamais devinés ; les omettre signifie que les étiquettes sont
  instantanées.
- **`quant.fit_impact()`** calibre un modèle de slippage à partir d'exécutions
  réelles au lieu de supposer une constante de coût.

### Backtesting

`h5i-db-backtest` est un backtester événementiel dont le plan de données est la
base elle-même. Une exécution se déroule dans un fork et y écrit `bt_orders`,
`bt_fills`, `bt_positions` et `bt_equity` : les résultats s'interrogent donc avec
le même SQL que les données de marché, et deux exécutions se comparent au niveau
de l'exécution avec `fork_diff`.

```python
fork = db.fork("bt-momentum-001")
quant.tearsheet(quant.from_levels(fork, "bt_equity"), path="run.html")
```

C'est aussi rapide, parce que le rejeu lit des enregistrements déjà décodés
directement dans la couche de stockage au lieu de franchir une frontière de langage
à chaque événement. Sur une charge partagée (200 k mises à jour du haut du carnet,
200 ordres au marché, un instrument, chaque adaptateur vérifiant qu'il les a tous vus) :

| moteur | frontière mesurée | médiane | débit |
|---|---|---:|---:|
| **h5i-db** | enregistrements décodés à travers le noyau de rejeu | **65,7 ms** | **3,05 M évén./s** |
| **h5i-db** | exécution persistée complète : balayage, décodage, fork, rejeu, écriture | 331 ms | 605 k évén./s |
| NautilusTrader 1.230.0 | objets en mémoire à travers `BacktestEngine.run()` | 767 ms | 261 k évén./s |
| LEAN `11ba019f6` | du premier callback `Slice` à `OnEndOfAlgorithm`, alimenté par disque | 2033 ms | 98,4 k évén./s |

Même la frontière persistée, qui fait strictement plus de travail que les deux
autres, vaut 2,3× le moteur en mémoire de NautilusTrader et 6,1× le débit mesuré
des callbacks de LEAN. Il s'agit d'une seule charge événementielle étroite, pas
d'un classement des systèmes de backtesting : les frontières diffèrent et le
benchmark vérifie des comptages d'événements et d'ordres, pas une équivalence de
PnL. Méthodologie, échantillons bruts et raisons du tracé de chaque frontière :
[benchmarks/backtest_compare/RESULTS.md](benchmarks/backtest_compare/RESULTS.md).

Ce que la simulation couvre elle-même :

- **Une exécution est une fonction pure de (ancrage des données, stratégie,
  configuration).** Pas d'horloge murale, pas d'aléa non initialisé, aucune
  itération sur une table de hachage sans tri préalable. `result.verify()` rejoue
  une exécution enregistrée et rapporte si elle s'est reproduite.
- **L'anticipation est fermée par construction, pas par convention.** Les
  enregistrements portent `ts_event` et `ts_init` et sont rejoués dans l'ordre de
  `ts_init` : les données tardives arrivent donc en retard, et une stratégie n'a
  aucun chemin vers la résolution d'un marché.
- **Le règlement est conditionné à l'observabilité.** Un rejeu de trois jours sur
  un marché de six mois se termine position ouverte et dit pourquoi, au lieu
  d'inscrire un profit que personne opérant cette fenêtre n'aurait pu encaisser.
  Les deux chiffres survivent : le PnL au marché et le PnL réglé, la différence
  étant rapportée comme un ajustement explicite.
- **Les opérations sur titres s'appliquent vers l'avant, jamais vers l'arrière.**
  Personne n'a jamais traité au prix ajusté du split : divisions, dividendes et
  radiations arrivent donc comme des événements à l'instant où ils prennent effet
  et agissent sur les positions, les limites en carnet et les valorisations. Les
  facteurs d'ajustement sont des données point-in-time ; une opération non encore
  annoncée n'est simplement pas dans le flux. Un ticker se résout en instrument sur
  des intervalles semi-ouverts, et une recherche ambiguë est refusée en nommant les
  candidats.
- **Les comptes sont multidevises,** avec marge, liquidation, funding des
  perpétuels, modification d'ordres, prévention d'auto-appariement et limites de
  risque avant envoi.
- **Le contrôle préalable refuse ce que les données ne peuvent pas soutenir.**
  `backtest.inspect()` rapporte une fidélité de rejeu, et réclamer des exécutions
  par position en file à partir d'instantanés périodiques est une erreur, pas un
  chiffre d'apparence plausible.
- **Les stratégies prennent trois formes :** tables de signaux ou de commandes (la
  stratégie comme donnée, sans code de callback ni frontière de langage dans la
  boucle), callbacks `EventStrategy` en Python, et le trait `Strategy` natif en Rust.
- **Couverture des lieux d'exécution :** les marchés de prédiction sont le premier
  venue, avec les marchés à N résultats comme cas général, via des chargeurs
  Kalshi, Polymarket et Hyperliquid qui produisent tous les mêmes tables
  canoniques. `KalshiFees` implémente la vraie courbe de frais quadratique,
  l'arrondi au centicent et l'accumulateur d'arrondi par exécution partielle, pas
  `notionnel × taux`.

Voir les pages du manuel sur le [quantitatif](https://db.h5i.dev/manual/quant/) et
le [backtesting](https://db.h5i.dev/manual/backtest/).

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

- **Un essai s'identifie par son contenu, pas par son nom.** Un `BacktestConfig`
ancré et déclaratif se résume en un `trial_digest` calculé sur toutes les entrées
du rejeu, en ignorant l'identifiant d'exécution et les métadonnées descriptives.
Resoumettre le même essai sémantique renvoie le résultat enregistré avec
`cached=True` au lieu de forker et de rejouer, et la recherche puis la création
sont sérialisées entre les processus d'agents locaux : une boucle de reprise ne
peut donc ni dépenser une seconde exécution ni compter deux fois un score.

- **La surface de revue répartit l'attention plutôt qu'elle ne classe.**
`h5i-db ui` trie les essais selon ce qui réclame un humain ensuite : décision
requise, puis en échec ou avec avertissement, puis terminés et non vus, puis en
cours, puis vus. Parcourir une liste ne marque rien comme revu ; un essai ne compte
comme vu que lorsque son détail est ouvert. Le classement est un onglet distinct,
parce que « lequel est le meilleur jusqu'ici » et « lequel n'ai-je pas regardé »
sont deux questions différentes.

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
- **Le trading réel :** le backtester ne route jamais un ordre véritable. Pas
  d'adaptateurs de courtier, pas d'optimiseur de portefeuille, pas d'API de tracé ;
  la frontière, c'est la simulation et l'évaluation.

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
