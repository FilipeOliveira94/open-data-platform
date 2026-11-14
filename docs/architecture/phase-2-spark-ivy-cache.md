# Spark lakehouse jar / Ivy cache (P2-08)

Each lakehouse Spark script still uses `--packages` with the same pins as P2-01 / P2-02 / P2-03 / P2-04. The `spark` service and the Airflow services mount the same **host directory** so Ivy artifacts survive `docker compose run --rm`. Since P3-03 all of them run the shared runtime image (`docker/runtime`).

| | |
| --- | --- |
| Host path | `.local/spark-ivy/` (gitignored under `.local/`) |
| Container path | `/tmp/.ivy2` (`spark.jars.ivy`) |
| Permissions | Host dirs are created world-writable so the container user can write the bind mount. The runtime image user is `uid=50000`. The earlier Spark image user was `uid=185`, so jars it wrote stay owned by 185 and readable |
| Pins | Iceberg `1.12.0`, Hadoop AWS `3.3.4`, AWS SDK bundle `1.12.262`, PostgreSQL JDBC `42.7.14` |

## Commands

```bash
make spark-ivy-warm   # first download (or after clearing the cache)
make live-bronze      # subsequent runs reuse the host cache
```

`make spark-ivy-warm` runs `/opt/odp/spark/warm_ivy.sh` (`SELECT 1` after resolving packages). Live Bronze/Silver and the probe targets create `.local/spark-ivy` before `compose run`.

## Result (2026-11-06)

1. Empty `.local/spark-ivy`, then `make spark-ivy-warm`: Ivy downloaded 6 artifacts (~30s jar fetch; cache size ~634 MiB).
2. Second `make spark-ivy-warm`: `0 artifacts copied, 6 already retrieved (0kB/6ms)`; resolve ~235 ms.
3. `run_bronze.sh` for `demo_storefront/clickstream` on a fresh `compose run --rm`: same cache hit (`artifacts dl 8ms`), Spark count `100`.

Pins unchanged. Chronological note: [status log](../status-log.md).
