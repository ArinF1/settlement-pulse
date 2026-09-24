import csv
from contextlib import closing
from datetime import date
from pathlib import Path
import sqlite3
import tempfile
import unittest

from settlement_pulse import generate_demo, run, write_csv, INSTRUCTION_FIELDS, EVENT_FIELDS


class PipelineTests(unittest.TestCase):
    def test_demo_snapshot_and_repeatability(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            instructions, events = generate_demo(out / "input")
            run(instructions, events, date(2026, 6, 30), out)
            first_report = (out / "report.html").read_bytes()
            with closing(sqlite3.connect(out / "settlement.db")) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM instructions").fetchone()[0], 240)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0], 3)
                self.assertEqual(db.execute("SELECT is_open_exception FROM snapshot WHERE instruction_id='INS-0019'").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM snapshot WHERE is_on_time=1").fetchone()[0], 179)
            run(instructions, events, date(2026, 6, 30), out)
            self.assertEqual((out / "report.html").read_bytes(), first_report)

    def test_terminal_event_and_spreadsheet_escaping(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            instructions = out / "instructions.csv"
            events = out / "events.csv"
            write_csv(instructions, INSTRUCTION_FIELDS, [dict(instruction_id="A", client="Client", market="SE",
                currency="SEK", amount="100.00", trade_date="2026-06-01", intended_date="2026-06-03")])
            write_csv(events, EVENT_FIELDS, [
                dict(event_id="E1", instruction_id="A", event_ts="2026-06-02T10:00:00Z", status="SETTLED"),
                dict(event_id="=MALICIOUS", instruction_id="A", event_ts="2026-06-03T10:00:00Z", status="FAILED"),
            ])
            run(instructions, events, date(2026, 6, 30), out)
            with closing(sqlite3.connect(out / "settlement.db")) as db:
                self.assertEqual(db.execute("SELECT latest_status,is_on_time,is_open_exception FROM snapshot").fetchone(),
                                 ("SETTLED", 1, 0))
            with (out / "rejected_rows.csv").open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["record_id"], "'=MALICIOUS")
            self.assertEqual(rows[0]["reason"], "event follows terminal SETTLED")
            run(instructions, events, date(2026, 5, 31), out)
            with closing(sqlite3.connect(out / "settlement.db")) as db:
                self.assertEqual(db.execute("SELECT COUNT(*) FROM snapshot").fetchone()[0], 0)

    def test_bad_schema_preserves_previous_report(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            instructions, events = generate_demo(out / "input")
            run(instructions, events, date(2026, 6, 30), out)
            original = (out / "report.html").read_bytes()
            instructions.write_text("wrong,column\n1,2\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                run(instructions, events, date(2026, 6, 30), out)
            self.assertEqual((out / "report.html").read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
