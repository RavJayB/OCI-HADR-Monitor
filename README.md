# OCI HA/DR Monitor

A small Flask app that measures what an application **actually experiences** during an
Oracle Autonomous Data Guard switchover: how long it was down, whether it lost data, and
whether the switchover happened at all.

Every number it reports comes from a real connection to a real database. Nothing is simulated.

## What it proves

1. **That the switchover really occurred** - that the app is now talking to a physically
   different database, not just that the console said so.
2. **The real RTO**, measured from the application's point of view.
3. **That no committed data was lost.**

## Why `DATABASE_ROLE` can't tell you a switchover happened

This is the central gotcha, and the reason this tool exists.

Your connect string (`mydb_tp`) does not point at a machine. It points at *whichever
database currently holds the primary role*. You can never connect to the standby. So
`SELECT database_role FROM v$database` answers `PRIMARY` **before and after** a switchover
- it cannot do anything else. The OCI console shows the same thing for the same reason.

Data Guard requires the primary and standby to have **different `db_unique_name` values**.
That is the signal this app uses:

```sql
SELECT SYS_CONTEXT('USERENV','DB_UNIQUE_NAME'),
       SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM dual;
```

When that value changes across an outage, the workload moved. In our run:

```
SWITCHOVER CONFIRMED - workload moved ebf1pod/ebf1pod2 -> emi1pod/emi1pod2
```

A reconnect returns you to the *same* `db_unique_name`. Only a switchover changes it.

## How it works

Every ~3 seconds the app opens a **brand-new** connection, asks the database who it is,
inserts and commits a row into `demo_transactions`, reads the count back, and disconnects.
While healthy that's a boring loop; when the database goes away the loop starts failing and
the app records when it started, how many attempts failed, and when it recovered. All timing
is server-side and uses a monotonic clock, so it survives browser refreshes.

Two deliberate design choices, both of which matter:

**A fresh connection per probe.** A pooled connection would quietly survive the failover and
hide it from us. We want to feel every disconnect.

**The driver's retry logic is disabled.** The wallet ships with `retry_count=20,
retry_delay=3` in `tnsnames.ora`, so a single `connect()` blocks for ~60s of silent retries
before it tells your application anything is wrong. That behaviour exists to *hide* outages
from applications - the opposite of what a monitor needs. With retries left on, a switchover
shows up as **zero downtime**. `_tuned_dsn()` rewrites the descriptor to `retry_count=0` and
adds a 3s transport timeout, so failure surfaces in ~1s per attempt.

## Setup

You need Docker, an Autonomous Database with Autonomous Data Guard enabled, and its wallet.

```bash
git clone https://github.com/RavJayB/OCI-HADR-Monitor.git
cd OCI-HADR-Monitor
```

**1. Add your wallet.** In the OCI console: *Database connection → Download wallet*. Unzip it
into `wallet/` (so `wallet/tnsnames.ora`, `wallet/cwallet.sso`, … sit at that level).
`wallet/` is gitignored - your credentials stay local.

**2. Add your credentials.**

```bash
cp .env.example .env   # then edit .env
```

`DB_DSN` must be an alias from your own `wallet/tnsnames.ora` (e.g. `mydb_tp`).

**3. Create the table the monitor writes to:**

```sql
CREATE TABLE demo_transactions (
  id         NUMBER GENERATED ALWAYS AS IDENTITY,
  tx_data    VARCHAR2(100),
  created_at TIMESTAMP DEFAULT SYSTIMESTAMP,
  status     VARCHAR2(20)
);
```

**4. Build and run:**

```bash
docker build -t hadr-monitor .
docker run -d --name hadr --env-file .env -p 5050:5000 hadr-monitor
```

Open <http://localhost:5050>. It refuses to start if `DB_PASSWORD` or `WALLET_PASSWORD` are
missing - credentials are never baked into the image.

## Running the demo

Let it settle for a minute so you have a clean baseline, note the instance name under the
ROLE tile, then trigger **Switchover** in the OCI console and watch. Do **not** restart the
container during the switchover - it has to be running *through* the event to measure it.

Expect: the dashboard goes red, counts failed probes for ~60–90s, comes back green, and logs
`SWITCHOVER CONFIRMED`. The instance name will have changed. `DATABASE_ROLE` will still say
`PRIMARY` - that's correct, see above.

Stop the writes with `docker stop hadr`.

## Independent proof, without the dashboard

The app writes a row every ~3s, so an outage leaves a **hole** in the table. Oracle records
it whether you trust the dashboard or not:

```sql
SELECT id, created_at, gap_seconds FROM (
  SELECT id, created_at,
         ROUND((CAST(created_at AS DATE)
              - CAST(LAG(created_at) OVER (ORDER BY id) AS DATE)) * 86400) AS gap_seconds
  FROM demo_transactions
)
WHERE gap_seconds > 30
ORDER BY id;
```

Each large gap is a window where nothing could be committed. Ours showed 60s and 72s for two
switchovers, matching both the dashboard and the console's `Role changed on` timestamps.

## Reading the numbers honestly

**Downtime is reported as two bounds** (e.g. `72.7s observed, 73.5s worst case`). The app is
blind between probes, so it cannot know the exact instant the database died - only that it
lies between the last successful probe and the first failed one. A single exact figure would
be false precision.

**Probe cadence is ~3s, not the configured 1s.** Each fresh TLS connect to ADB costs ~1s.
The dashboard displays its measured cadence rather than the configured one. That cadence is
the resolution limit: outages shorter than one probe gap are invisible.

**"Zero data loss" means the row count never went backwards**, which is consistent with a
planned switchover being zero-RPO by design. It is not the same as individually verifying
that the final commits before the drop survived.

**Gaps in `id` are normal.** `ID` is an identity column with `CACHE_SIZE 20`; each new
connection grabs 20 values and discards the unused ones, and we open a connection per probe.
`COUNT(*) = COUNT(DISTINCT id)` - no rows are missing.

**`SLA_TARGET_S` defaults to 120s**, which matches Oracle's published target for a **local**
Autonomous Data Guard standby: RTO 2 minutes, RPO 0 seconds, for both switchover and failover
([About Standby Databases](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/autonomous-data-guard-about.html)).
A **cross-region** standby is a different story - RTO under 10 minutes, and manual failover can
lose up to a minute of data. Raise `SLA_TARGET_S` if your peer is cross-region.

## Cost

The probe writes ~28k rows/day and keeps a database busy. Stop the container when you're done,
and terminate the ADB peers in OCI if you don't need them - they consume credits.

Clean up the monitor's rows with:

```sql
DELETE FROM demo_transactions WHERE tx_data LIKE 'monitor_%';
COMMIT;
```

Note this does **not** reset the identity counter; new rows continue from the last id.

## Reference

- [Oracle Autonomous Database (serverless) documentation](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/) - provisioning, wallets, Database Actions
- [Autonomous Data Guard](https://docs.oracle.com/en-us/iaas/autonomous-database-serverless/doc/autonomous-data-guard-about.html) - enabling a peer, switchover vs failover, the RTO/RPO targets
- [python-oracledb driver documentation](https://python-oracledb.readthedocs.io/) - thin-mode connections, mTLS wallets, and the `retry_count` / `transport_connect_timeout` descriptor parameters this app overrides
- [Oracle LiveLabs](https://livelabs.oracle.com/) - free hands-on workshops on Oracle's own tenancy, including real 2-node RAC and Data Guard labs. No credit card, no quota
- [OCI Always Free tier](https://www.oracle.com/cloud/free/) - 2 Autonomous Databases free forever. Note that **Autonomous Data Guard is not available on the free tier** - the peer needs a paid (or trial-credit) tenancy

## Getting help

- Check the Autonomous Data Guard docs above before assuming the app is wrong - most surprises (`DATABASE_ROLE` never changing, the driver's silent 60s retry) are Oracle behaving as designed
- If the container starts but every probe fails, it's almost always the wallet: confirm `wallet/` contains the unzipped files, that `DB_DSN` matches an alias in your own `wallet/tnsnames.ora`, and that `WALLET_PASSWORD` is the one you set at download time
- If a switchover shows `Same instance - connectivity blip`, the switchover did not happen. Check **Work requests** in the OCI console; a standby that is still provisioning will refuse one
- Never commit your wallet or `.env`. Both are gitignored - keep it that way
- For questions or collaboration, contact me through my portfolio: [ravindubandara.dev](https://ravindubandara.dev)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Keep credentials out of commits - wallets and `.env` files must stay local (see [.gitignore](.gitignore))
4. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
