# h5i-db from Python

```python
import h5i_db
db = h5i_db.Database("market.db")            # read_only=True for analysis-only
df = db.sql("SELECT * FROM h5i('trades', 42)").to_pandas()
plan = db.plan_delete_range("trades", t0, t1); plan.summary; plan.apply()
```
