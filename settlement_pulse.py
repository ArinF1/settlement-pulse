"""Reproducible, synthetic settlement exception pipeline. Python standard library only."""

from __future__ import annotations

import argparse
import csv
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path
import os
import random
import sqlite3
import tempfile


INSTRUCTION_FIELDS = ["instruction_id", "client", "market", "currency", "amount", "trade_date", "intended_date"]
EVENT_FIELDS = ["event_id", "instruction_id", "event_ts", "status"]
STATUSES = {"SENT", "FAILED", "SETTLED"}


def read_csv(path: Path, fields: list[str]):
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or set(reader.fieldnames) != set(fields):
            raise ValueError(f"{path}: expected columns {', '.join(fields)}")
        for line, row in enumerate(reader, 2):
            yield line, row


def clean_instruction(row: dict[str, str]) -> tuple:
    identifier = (row.get("instruction_id") or "").strip()
    client = (row.get("client") or "").strip()
    market = (row.get("market") or "").strip().upper()
    currency = (row.get("currency") or "").strip().upper()
    if not identifier or not client or not market or len(currency) != 3 or not currency.isalpha():
        raise ValueError("missing ID/client/market or invalid currency")
    try:
        amount = Decimal(row.get("amount") or "")
        trade = date.fromisoformat(row.get("trade_date") or "")
        intended = date.fromisoformat(row.get("intended_date") or "")
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("invalid amount or date") from exc
    if not amount.is_finite() or amount <= 0 or amount > 1_000_000_000_000 or amount.as_tuple().exponent < -2:
        raise ValueError("amount must be positive, at most 1 trillion, with at most two decimal places")
    if intended < trade:
        raise ValueError("intended date precedes trade date")
    return identifier, client, market, currency, int(amount * 100), trade.isoformat(), intended.isoformat()


def clean_event(row: dict[str, str]) -> tuple:
    identifier = (row.get("event_id") or "").strip()
    instruction = (row.get("instruction_id") or "").strip()
    status = (row.get("status") or "").strip().upper()
    if not identifier or not instruction or status not in STATUSES:
        raise ValueError("missing ID/instruction or invalid status")
    raw = (row.get("event_ts") or "").strip()
    if "T" not in raw or not raw.endswith("Z"):
        raise ValueError("event_ts must be UTC and end in Z")
    try:
        timestamp = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("invalid event timestamp") from exc
    return identifier, instruction, timestamp.isoformat().replace("+00:00", "Z"), status


def build_database(instructions: Path, events: Path, as_of: date, db_path: Path) -> int:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript("""
            CREATE TABLE instructions (
                instruction_id TEXT PRIMARY KEY, client TEXT NOT NULL, market TEXT NOT NULL,
                currency TEXT NOT NULL, amount_cents INTEGER NOT NULL,
                trade_date TEXT NOT NULL, intended_date TEXT NOT NULL
            );
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY, instruction_id TEXT NOT NULL REFERENCES instructions,
                event_ts TEXT NOT NULL, status TEXT NOT NULL
            );
            CREATE INDEX events_by_instruction_time ON events(instruction_id,event_ts);
            CREATE TABLE rejected_rows (
                source TEXT NOT NULL, line_number INTEGER NOT NULL,
                record_id TEXT, reason TEXT NOT NULL
            );
            CREATE TABLE snapshot (
                instruction_id TEXT PRIMARY KEY, client TEXT, market TEXT, currency TEXT,
                amount_cents INTEGER, intended_date TEXT, latest_status TEXT,
                settled_date TEXT, is_on_time INTEGER, is_open_exception INTEGER
            );
            CREATE VIEW market_metrics AS
                SELECT market, COUNT(*) AS instructions,
                       SUM(is_on_time) AS on_time,
                       SUM(is_open_exception) AS open_exceptions
                FROM snapshot GROUP BY market;
        """)
        def reject(source: str, line: int, identifier: str, reason: str):
            connection.execute("INSERT INTO rejected_rows VALUES (?,?,?,?)", (source, line, identifier, reason))

        for line, row in read_csv(instructions, INSTRUCTION_FIELDS):
            try:
                value = clean_instruction(row)
                connection.execute("INSERT INTO instructions VALUES (?,?,?,?,?,?,?)", value)
            except sqlite3.IntegrityError:
                reject("instructions", line, row.get("instruction_id", ""), "duplicate instruction_id")
            except ValueError as exc:
                reject("instructions", line, row.get("instruction_id", ""), str(exc))

        candidates = []
        seen_ids = set()
        for line, row in read_csv(events, EVENT_FIELDS):
            try:
                value = clean_event(row)
                if value[0] in seen_ids:
                    raise ValueError("duplicate event_id")
                seen_ids.add(value[0])
                instruction = connection.execute("SELECT trade_date FROM instructions WHERE instruction_id=?", (value[1],)).fetchone()
                if instruction is None:
                    raise ValueError("unknown instruction_id")
                if value[2][:10] < instruction[0]:
                    raise ValueError("event precedes trade date")
                candidates.append((value[2], value[0], line, value))
            except ValueError as exc:
                reject("events", line, row.get("event_id", ""), str(exc))

        settled_ids = set()
        for _, _, line, value in sorted(candidates):
            if value[1] in settled_ids:
                reject("events", line, value[0], "event follows terminal SETTLED")
                continue
            if connection.execute("SELECT 1 FROM events WHERE instruction_id=? AND event_ts=?", (value[1], value[2])).fetchone():
                reject("events", line, value[0], "ambiguous events at same timestamp")
                continue
            connection.execute("INSERT INTO events VALUES (?,?,?,?)", value)
            if value[3] == "SETTLED":
                settled_ids.add(value[1])

        cutoff = (as_of + timedelta(days=1)).isoformat() + "T00:00:00Z"
        connection.execute("""
            INSERT INTO snapshot
            SELECT i.instruction_id, i.client, i.market, i.currency, i.amount_cents,
                   i.intended_date,
                   (SELECT e.status FROM events e WHERE e.instruction_id=i.instruction_id
                    AND e.event_ts<? ORDER BY e.event_ts DESC, e.event_id DESC LIMIT 1),
                   (SELECT substr(e.event_ts,1,10) FROM events e WHERE e.instruction_id=i.instruction_id
                    AND e.status='SETTLED' AND e.event_ts<? ORDER BY e.event_ts LIMIT 1),
                   CASE WHEN EXISTS (
                       SELECT 1 FROM events e WHERE e.instruction_id=i.instruction_id
                       AND e.status='SETTLED' AND e.event_ts<? AND substr(e.event_ts,1,10)<=i.intended_date
                   ) THEN 1 ELSE 0 END,
                   CASE WHEN i.intended_date<? AND NOT EXISTS (
                       SELECT 1 FROM events e WHERE e.instruction_id=i.instruction_id
                       AND e.status='SETTLED' AND e.event_ts<?
                   ) THEN 1 ELSE 0 END
            FROM instructions i WHERE i.trade_date<=?
        """, (cutoff, cutoff, cutoff, as_of.isoformat(), cutoff, as_of.isoformat()))
        connection.commit()
        return connection.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0]
    finally:
        connection.close()


def write_rejections(db_path: Path, path: Path):
    def safe_cell(value):
        value = str(value)
        return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value

    with closing(sqlite3.connect(db_path)) as connection, path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["source", "line_number", "record_id", "reason"])
        writer.writerows(tuple(safe_cell(value) for value in row)
                         for row in connection.execute("SELECT * FROM rejected_rows ORDER BY source,line_number"))


def render_report(db_path: Path, as_of: date, path: Path):
    with closing(sqlite3.connect(db_path)) as connection:
        count, on_time, open_count = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(is_on_time),0), COALESCE(SUM(is_open_exception),0) FROM snapshot"
        ).fetchone()
        rejects = connection.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0]
        market_rows = connection.execute(
            "SELECT market,instructions,on_time,open_exceptions FROM market_metrics ORDER BY open_exceptions DESC,market"
        ).fetchall()
        exposure = connection.execute("""
            SELECT currency, COUNT(*), SUM(amount_cents)
            FROM snapshot WHERE is_open_exception=1 GROUP BY currency ORDER BY currency
        """).fetchall()
        exceptions = connection.execute("""
            SELECT instruction_id,client,market,currency,amount_cents,intended_date,
                   COALESCE(latest_status,'NO EVENT')
            FROM snapshot WHERE is_open_exception=1
            ORDER BY intended_date,instruction_id LIMIT 12
        """).fetchall()

    def tr(cells):
        return "<tr>" + "".join(f"<td>{escape(str(cell))}</td>" for cell in cells) + "</tr>"

    market_html = "".join(tr((m, n, f"{(o/n*100):.1f}%", x)) for m, n, o, x in market_rows)
    exposure_html = "".join(tr((c, n, f"{amount/100:,.2f} {c}")) for c, n, amount in exposure)
    exception_html = "".join(tr((i, cl, m, f"{a/100:,.2f} {c}", d, s)) for i, cl, m, c, a, d, s in exceptions)
    rates = "".join(
        f'<div class="bar-row"><span>{escape(m)}</span><div class="track"><div class="fill" style="width:{(o/n*100):.1f}%"></div></div><b>{(o/n*100):.1f}%</b></div>'
        for m, n, o, _ in market_rows
    )
    document = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Settlement Pulse · {as_of}</title><style>
:root{{--ink:#132a3a;--muted:#5c7080;--teal:#007d75;--pale:#e8f5f2;--line:#dbe4e8;--bg:#f4f7f8}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 system-ui,sans-serif}}
header{{background:#102d3a;color:white;padding:42px max(24px,calc((100vw - 1100px)/2)) 34px}}
header small{{color:#82d8c6;letter-spacing:.18em;text-transform:uppercase;font-weight:700}}
h1{{font-size:clamp(2.2rem,5vw,3.7rem);line-height:1.05;margin:14px 0}}header p{{max-width:690px;color:#c8d9dd;margin:0}}
main{{max-width:1150px;padding:28px 24px 60px;margin:auto}}.meta{{color:var(--muted);font-size:.9rem;margin-bottom:20px}}
.cards{{display:grid;grid-template-columns:repeat(4,1fr);gap:15px}}.card,.panel{{background:white;border:1px solid var(--line);border-radius:13px;box-shadow:0 4px 20px #18334108}}
.card{{padding:20px}}.card span{{display:block;color:var(--muted);font-size:.84rem}}.card strong{{font-size:2rem;line-height:1.3}}.card em{{display:block;font-size:.75rem;color:var(--muted);font-style:normal}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px}}.panel{{padding:22px;overflow:auto}}h2{{margin:0 0 12px;font-size:1.2rem}}p.note{{font-size:.88rem;color:var(--muted);margin:3px 0 18px}}table{{border-collapse:collapse;width:100%;font-size:.89rem}}th{{text-align:left;color:var(--muted);font-weight:600}}th,td{{padding:9px 10px;border-bottom:1px solid var(--line);white-space:nowrap}}td:first-child,th:first-child{{padding-left:0}}.wide{{margin-top:16px}}
.bar-row{{display:flex;align-items:center;gap:10px;margin:15px 0}}.bar-row span{{width:34px}}.bar-row b{{width:56px;text-align:right;font-size:.88rem}}.track{{flex:1;background:var(--pale);height:14px;border-radius:10px;overflow:hidden}}.fill{{background:var(--teal);height:100%}}
a{{color:var(--teal)}}footer{{font-size:.8rem;color:var(--muted);margin-top:20px}}@media(max-width:800px){{.cards,.grid{{grid-template-columns:1fr 1fr}}}}@media(max-width:560px){{.cards,.grid{{grid-template-columns:1fr}}}}
</style></head><body>
<header><small>Synthetic investor services data product</small><h1>Settlement Pulse</h1><p>Operational visibility into instructions, late settlement and open exceptions. Built from validated CSVs with an auditable SQLite snapshot.</p></header>
<main><div class="meta">As of {as_of} (UTC) · Deterministic demonstration data · Definitions below</div>
<section class="cards">
<div class="card"><span>Valid instructions</span><strong>{count:,}</strong><em>Included in snapshot</em></div>
<div class="card"><span>Settled on time</span><strong>{on_time/count*100 if count else 0:.1f}%</strong><em>{on_time:,} instructions</em></div>
<div class="card"><span>Open exceptions</span><strong>{open_count:,}</strong><em>Past intended date, still unsettled</em></div>
<div class="card"><span>Rejected source rows</span><strong>{rejects:,}</strong><em><a href="rejected_rows.csv">Download reasons</a></em></div>
</section>
<section class="grid"><div class="panel"><h2>On-time rate by market</h2><p class="note">First settlement on or before intended date.</p>{rates}</div>
<div class="panel"><h2>Open exposure by currency</h2><p class="note">Amounts stay in their original currency; no FX conversion.</p><table><thead><tr><th>Currency</th><th>Instructions</th><th>Amount</th></tr></thead><tbody>{exposure_html}</tbody></table></div></section>
<section class="panel wide"><h2>Market summary</h2><table><thead><tr><th>Market</th><th>Instructions</th><th>On-time rate</th><th>Open exceptions</th></tr></thead><tbody>{market_html}</tbody></table></section>
<section class="panel wide"><h2>Oldest open exceptions</h2><p class="note">First 12 by intended date. The SQLite snapshot holds the full list.</p><table><thead><tr><th>Instruction</th><th>Client</th><th>Market</th><th>Amount</th><th>Intended</th><th>Latest status</th></tr></thead><tbody>{exception_html}</tbody></table></section>
<footer>Open exception = unsettled and intended date before the as-of date. This is a synthetic operational example, not regulatory reporting or investment advice.</footer></main></body></html>"""
    path.write_text(document, encoding="utf-8")


def run(instructions: Path, events: Path, as_of: date, out: Path):
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=out) as temporary:
        stage = Path(temporary)
        db = stage / "settlement.db"
        rejected = build_database(instructions, events, as_of, db)
        write_rejections(db, stage / "rejected_rows.csv")
        render_report(db, as_of, stage / "report.html")
        for filename in ("settlement.db", "rejected_rows.csv", "report.html"):
            os.replace(stage / filename, out / filename)
    print(f"Wrote {out / 'report.html'} ({rejected} rejected source rows)")


def write_csv(path: Path, fields: list[str], rows: list[dict]):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def generate_demo(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    rng = random.Random(2027)
    instructions = []
    events = []
    markets = [("SE", "SEK"), ("FI", "EUR"), ("DK", "DKK"), ("NO", "NOK")]
    for number in range(1, 241):
        market, currency = markets[(number - 1) % 4]
        trade = date(2026, 6, 1) + timedelta(days=rng.randrange(22))
        intended = trade + timedelta(days=2)
        identifier = f"INS-{number:04d}"
        instructions.append(dict(instruction_id=identifier, client=f"Client {number % 18 + 1:02d}", market=market,
                                 currency=currency, amount=f"{rng.randrange(1000, 100000):.2f}",
                                 trade_date=trade.isoformat(), intended_date=intended.isoformat()))
        events.append(dict(event_id=f"EVT-{number:04d}-1", instruction_id=identifier,
                           event_ts=f"{trade}T10:00:00Z", status="SENT"))
        outcome = 0.9 if number == 19 else rng.random()
        if outcome < 0.7:
            when, status = intended, "SETTLED"
        elif outcome < 0.85:
            when, status = intended + timedelta(days=2), "SETTLED"
        elif outcome < 0.95:
            when, status = intended, "FAILED"
        else:
            continue
        events.append(dict(event_id=f"EVT-{number:04d}-2", instruction_id=identifier,
                           event_ts=f"{when}T15:00:00Z", status=status))
    # A late resolution after the report cutoff must remain open in the June snapshot.
    events.append(dict(event_id="EVT-0019-3", instruction_id="INS-0019", event_ts="2026-07-02T11:00:00Z", status="SETTLED"))
    instructions.append({**instructions[0], "amount": "not-a-number", "instruction_id": "BAD-AMOUNT"})
    instructions.append(dict(instructions[1]))
    events.append(dict(event_id="EVT-ORPHAN", instruction_id="INS-MISSING", event_ts="2026-06-12T11:00:00Z", status="FAILED"))
    write_csv(directory / "instructions.csv", INSTRUCTION_FIELDS, instructions)
    write_csv(directory / "events.csv", EVENT_FIELDS, events)
    return directory / "instructions.csv", directory / "events.csv"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("demo", help="Generate deterministic synthetic inputs and report")
    demo.add_argument("--out", type=Path, default=Path("output"))
    custom = subparsers.add_parser("run", help="Process instruction and event CSV files")
    custom.add_argument("--instructions", type=Path, required=True)
    custom.add_argument("--events", type=Path, required=True)
    custom.add_argument("--as-of", type=date.fromisoformat, required=True)
    custom.add_argument("--out", type=Path, default=Path("output"))
    args = parser.parse_args()
    if args.command == "demo":
        inputs = generate_demo(args.out / "input")
        run(*inputs, date(2026, 6, 30), args.out)
    else:
        run(args.instructions, args.events, args.as_of, args.out)


if __name__ == "__main__":
    main()
