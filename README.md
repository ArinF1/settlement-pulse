# Settlement Pulse

A small, reproducible post-trade data product for exploring settlement exceptions. It ingests instruction and event CSVs, validates them, builds an as-of snapshot in SQLite, and produces a self-contained HTML report. All data is synthetic. No SEB systems, client data, or proprietary models are used.

**Why this project:** Investor Services teams need to turn operational data into trustworthy reporting. A useful report starts with traceable definitions, rejected-record visibility, repeatable runs, and a clear explanation of what an exception means. This project demonstrates those practices with Python, SQL, and a report that a non-engineer can read.

## Run it

Python 3.11+; no packages or credentials required.

```bash
python settlement_pulse.py demo --out output
```

Open `output/report.html`. The command also writes `output/settlement.db`, `output/rejected_rows.csv`, and the generated source CSVs in `output/input/`.

Run on your own CSVs:

```bash
python settlement_pulse.py run --instructions instructions.csv --events events.csv --as-of 2026-06-30 --out output
```

Run checks:

```bash
python -m unittest discover -s tests -v
```

## Input contract

`instructions.csv`: `instruction_id,client,market,currency,amount,trade_date,intended_date`

`events.csv`: `event_id,instruction_id,event_ts,status`

Dates use ISO `YYYY-MM-DD`; event timestamps use ISO UTC ending in `Z`. `amount` is a positive decimal in the instruction currency. Status is `SENT`, `FAILED`, or `SETTLED`. IDs must be unique in their respective files. Rows violating the contract, events referencing unknown instructions, events before trade, events with ambiguous same-time ordering, and events after a terminal `SETTLED` event are quarantined in `rejected_rows.csv`. They are not silently counted as valid data.

## Metric definitions

The snapshot includes instructions traded by the as-of date and uses the latest valid event before midnight UTC following that date. An instruction is **settled on time** when its first `SETTLED` event occurred on or before the intended date. A **currently open exception** is an unsettled instruction whose intended date is before the as-of date, whether its latest event is `FAILED`, `SENT`, or missing. Thus an explicit failure that was later settled is no longer an open exception, but still counts as late if it settled after the intended date. Exposure is shown by currency; unlike amounts in different currencies, counts can be combined. The report is an operational illustration and does not implement CSDR reporting definitions or penalties.

The SQLite database has `instructions`, `events`, and `rejected_rows` tables plus `snapshot` and `market_metrics` views. Query it directly, for example:

```sql
SELECT market, COUNT(*) AS open_exceptions
FROM snapshot WHERE is_open_exception = 1
GROUP BY market ORDER BY open_exceptions DESC;
```

## Design choices and limits

- Deterministic synthetic data makes the example reproducible and safe to publish. The generator includes late settlement, unresolved failure, post-cutoff resolution, and three deliberately bad records.
- Ingestion is idempotent: each run rebuilds the database from source files in a temporary directory and replaces the output only after successful processing.
- The report displays rejected-row counts and links to the full rejection CSV. Currency amounts are never added across currencies.
- This is a portfolio prototype, not a production settlement engine. It does not model partial settlement, market calendars, corporate actions, FX conversion, or actual regulatory submission schemas.

## Why these metrics

ESMA describes settlement-fail and efficiency reporting as part of CSDR and explicitly notes data quality, consistency, and completeness in its supervisory work. This project borrows the *problem framing* only, using invented operational definitions and synthetic data. See [ESMA's CSDR reporting overview](https://www.esma.europa.eu/data-reporting/csdr-reporting) and [Article 7](https://www.esma.europa.eu/publications-and-data/interactive-single-rulebook/csdr/article-7-measures-address-settlement-0).

## License

MIT. See [LICENSE](LICENSE).
