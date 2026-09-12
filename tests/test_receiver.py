import hashlib
import http.client
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import threading
import unittest
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "receiver"))
from receiver import Handler, Inbox, Receiver


class ReceiverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.inbox = Inbox(Path(self.temp.name))
        self.server = Receiver(("127.0.0.1", 0), Handler)
        self.server.inbox = self.inbox
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.device = str(uuid.uuid4())

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def upload(self, body=b"test audio", chunk_id=None, started="2026-09-10T12:00:00.000Z", **overrides):
        chunk_id = chunk_id or str(uuid.uuid4())
        headers = {
            "Authorization": "Bearer " + self.inbox.token,
            "X-Device-ID": self.device,
            "X-Started-At": started,
            "X-Duration-Seconds": "60.0",
            "X-Audio-SHA256": hashlib.sha256(body).hexdigest(),
            "Content-Type": "audio/mp4",
        }
        headers.update(overrides)
        client = http.client.HTTPConnection(*self.server.server_address, timeout=5)
        client.request("POST", "/v1/chunks/" + chunk_id, body, headers)
        response = client.getresponse()
        result = response.status, json.loads(response.read()), chunk_id
        client.close()
        return result

    def test_acknowledgment_survives_receiver_restart(self):
        status, receipt, chunk_id = self.upload()
        self.assertEqual(status, 201)
        self.assertTrue(receipt["durable"])
        reopened = Inbox(self.inbox.root)
        row = reopened.receipt(chunk_id)
        self.assertEqual(Path(row["path"]).read_bytes(), b"test audio")
        self.assertEqual(row["sha256"], receipt["sha256"])

    def test_lost_receipt_retry_is_idempotent_even_after_audio_deletion(self):
        _, _, chunk_id = self.upload()
        self.inbox.complete(chunk_id, "A test sentence.")
        status, receipt, _ = self.upload(chunk_id=chunk_id)
        self.assertEqual(status, 200)
        self.assertTrue(receipt["durable"])
        self.assertEqual(self.inbox.status(), {"complete": 1})
        self.assertEqual((self.inbox.root / "life.md").read_text().count("A test sentence."), 1)
        self.assertEqual(list(self.inbox.audio.iterdir()), [])

    def test_id_collision_rejected(self):
        _, _, chunk_id = self.upload()
        status, _, _ = self.upload(body=b"different recording", chunk_id=chunk_id)
        self.assertEqual(status, 409)
        self.assertEqual(Path(self.inbox.receipt(chunk_id)["path"]).read_bytes(), b"test audio")

    def test_checksum_failure_keeps_no_receipt(self):
        status, _, chunk_id = self.upload(**{"X-Audio-SHA256": "0" * 64})
        self.assertEqual(status, 422)
        self.assertIsNone(self.inbox.receipt(chunk_id))
        self.assertEqual(list(self.inbox.audio.iterdir()), [])

    def test_unauthorized_upload_rejected(self):
        status, _, _ = self.upload(**{"Authorization": "Bearer wrong"})
        self.assertEqual(status, 401)
        self.assertEqual(self.inbox.status(), {})

    def test_offline_backlog_is_exported_by_capture_time(self):
        _, _, later = self.upload(started="2026-09-10T16:00:00.000Z")
        self.inbox.complete(later, "Later audio.")
        _, _, earlier = self.upload(started="2026-09-09T16:00:00.000Z")
        self.inbox.complete(earlier, "Earlier audio.")
        text = (self.inbox.root / "life.md").read_text()
        self.assertLess(text.index("Earlier audio."), text.index("Later audio."))
        self.assertTrue((self.inbox.days / "2026-09-09.md").exists())
        self.assertTrue((self.inbox.days / "2026-09-10.md").exists())

    def test_untranscribed_audio_retained(self):
        _, _, chunk_id = self.upload()
        self.inbox.cleanup_completed()
        self.assertTrue(Path(self.inbox.receipt(chunk_id)["path"]).exists())

    def test_bad_metadata_and_path_rejected(self):
        for override in ({"X-Duration-Seconds": "NaN"}, {"X-Duration-Seconds": "601"},
                         {"X-Started-At": "2026-09-10"}, {"X-Device-ID": "../escape"}):
            self.assertEqual(self.upload(**override)[0], 400)
        self.assertEqual(self.upload(chunk_id="../escape")[0], 400)

    def test_concurrent_duplicate_uploads_get_one_receipt(self):
        chunk_id = str(uuid.uuid4())
        responses = []
        jobs = [threading.Thread(target=lambda: responses.append(self.upload(chunk_id=chunk_id)[0])) for _ in range(4)]
        for job in jobs: job.start()
        for job in jobs: job.join()
        self.assertEqual(sorted(responses), [200, 200, 200, 201])
        self.assertEqual(self.inbox.status(), {"pending": 1})

    def test_interrupted_upload_never_acknowledged(self):
        chunk_id = str(uuid.uuid4())
        headers = (f"POST /v1/chunks/{chunk_id} HTTP/1.1\r\nHost: localhost\r\n"
                   f"Authorization: Bearer {self.inbox.token}\r\nX-Device-ID: {self.device}\r\n"
                   "X-Started-At: 2026-09-10T12:00:00.000Z\r\nX-Duration-Seconds: 60\r\n"
                   f"X-Audio-SHA256: {hashlib.sha256(b'abcd').hexdigest()}\r\nContent-Length: 4\r\n\r\nab")
        with socket.create_connection(self.server.server_address) as client:
            client.sendall(headers.encode())
            client.shutdown(socket.SHUT_WR)
            response = client.recv(4096)
        self.assertIn(b"400", response)
        self.assertIsNone(self.inbox.receipt(chunk_id))


class ExportTests(unittest.TestCase):
    """The incremental export must be indistinguishable from a full rebuild."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def transcripts(self, inbox):
        return {str(path.relative_to(inbox.root)): path.read_bytes()
                for path in sorted(inbox.root.rglob("*.md"))}

    def populate(self, inbox, starts):
        with inbox.connect() as db:
            for index, started in enumerate(starts):
                db.execute("INSERT INTO chunks (id,sha256,device,started,duration,path,received)"
                           " VALUES (?,?,?,?,?,?,?)",
                           (f"{index:032x}", "0" * 64, "dev", started, 60.0, "", 0.0))

    def build(self, order, starts, texts, incremental):
        root = Path(tempfile.mkdtemp(dir=self.root))
        inbox = Inbox(root)
        self.populate(inbox, starts)
        for index in order:
            if incremental:
                inbox.complete(f"{index:032x}", texts[index])
            else:
                with inbox.connect() as db:
                    db.execute("UPDATE chunks SET status='complete',transcript=? WHERE id=?",
                               (texts[index], f"{index:032x}"))
        if not incremental:
            inbox._rebuild()
        return self.transcripts(inbox)

    def test_incremental_export_matches_a_full_rebuild(self):
        count = 40
        starts, texts = [], []
        for index in range(count):
            hour = (23 + index // 20) % 24
            day = 10 + (23 + index // 20) // 24
            starts.append(f"2026-03-{day:02d}T{hour:02d}:{index % 60:02d}:00.000Z")
            # Every seventh clip is silence the cleaner rejects, which must not
            # emit a section or disturb the ordering watermark.
            texts.append("" if index % 7 == 3 else f"clip {index} about the roadmap")
        orders = {
            "in order": list(range(count)),
            "reverse backlog": list(range(count))[::-1],
            "late straggler": list(range(1, count)) + [0],
        }
        for name, order in orders.items():
            with self.subTest(order=name):
                self.assertEqual(self.build(order, starts, texts, incremental=True),
                                 self.build(order, starts, texts, incremental=False))

    def test_export_repairs_a_transcript_edited_underneath_it(self):
        inbox = Inbox(self.root)
        self.populate(inbox, ["2026-03-10T09:00:00.000Z", "2026-03-10T09:01:00.000Z"])
        inbox.complete(f"{0:032x}", "first clip")
        (inbox.root / "life.md").write_bytes(b"truncated")
        inbox.complete(f"{1:032x}", "second clip")
        life = (inbox.root / "life.md").read_text()
        self.assertIn("first clip", life)
        self.assertIn("second clip", life)

    def test_a_missing_day_file_is_rebuilt_rather_than_replaced(self):
        """A day file that vanished must not come back holding only the newest
        clip while life.md still carries the rest."""
        inbox = Inbox(self.root)
        self.populate(inbox, ["2026-03-10T09:00:00.000Z", "2026-03-10T09:01:00.000Z",
                              "2026-03-10T09:02:00.000Z"])
        inbox.complete(f"{0:032x}", "first clip")
        inbox.complete(f"{1:032x}", "second clip")
        (inbox.days / "2026-03-10.md").unlink()
        inbox.complete(f"{2:032x}", "third clip")
        day = (inbox.days / "2026-03-10.md").read_text()
        for text in ("first clip", "second clip", "third clip"):
            self.assertIn(text, day)

    def test_a_silent_clip_does_not_bless_a_truncated_day_file(self):
        """A clip the cleaner rejects writes nothing, so it must not carry the
        watermark past a day file it never looked at."""
        inbox = Inbox(self.root)
        self.populate(inbox, ["2026-03-10T09:00:00.000Z", "2026-03-10T09:01:00.000Z",
                              "2026-03-10T09:02:00.000Z", "2026-03-10T09:03:00.000Z"])
        inbox.complete(f"{0:032x}", "first clip")
        inbox.complete(f"{1:032x}", "second clip")
        (inbox.days / "2026-03-10.md").write_bytes(b"# 2026-03-10\n\n")
        inbox.complete(f"{2:032x}", "[BLANK_AUDIO]")
        inbox.complete(f"{3:032x}", "fourth clip")
        day = (inbox.days / "2026-03-10.md").read_text()
        for text in ("first clip", "second clip", "fourth clip"):
            self.assertIn(text, day)

    def test_audio_that_cannot_be_deleted_is_left_for_the_next_sweep(self):
        """A stuck file must not fail a transcript that is already durable."""
        inbox = Inbox(self.root)
        stuck = inbox.audio / "stuck.m4a"
        stuck.mkdir()  # unlink() refuses a directory
        (stuck / "inner").write_bytes(b"audio")
        with inbox.connect() as db:
            db.execute("INSERT INTO chunks (id,sha256,device,started,duration,path,received)"
                       " VALUES (?,?,?,?,?,?,?)",
                       (f"{0:032x}", "0" * 64, "dev", "2026-03-10T09:00:00.000Z", 60.0,
                        str(stuck), 0.0))
        inbox.complete(f"{0:032x}", "spoken words")
        self.assertIn("spoken words", (inbox.root / "life.md").read_text())
        with inbox.connect() as db:
            self.assertEqual(db.execute("SELECT path FROM chunks WHERE id=?",
                                        (f"{0:032x}",)).fetchone()["path"], str(stuck))

    def test_transcribed_audio_is_deleted_and_not_revisited(self):
        inbox = Inbox(self.root)
        audio = inbox.audio / "clip.m4a"
        audio.write_bytes(b"audio")
        with inbox.connect() as db:
            db.execute("INSERT INTO chunks (id,sha256,device,started,duration,path,received)"
                       " VALUES (?,?,?,?,?,?,?)",
                       (f"{0:032x}", "0" * 64, "dev", "2026-03-10T09:00:00.000Z", 60.0,
                        str(audio), 0.0))
        inbox.complete(f"{0:032x}", "spoken words")
        self.assertFalse(audio.exists())
        with inbox.connect() as db:
            self.assertEqual(db.execute("SELECT path FROM chunks WHERE id=?",
                                        (f"{0:032x}",)).fetchone()["path"], "")

    def test_no_connection_outlives_the_call_that_opened_it(self):
        """One transcribed chunk opens several connections. Left to the garbage
        collector they each hold three WAL descriptors, and macOS caps a process
        at 256."""
        inbox = Inbox(self.root)
        self.populate(inbox, ["2026-03-10T09:00:00.000Z"])
        opened = []
        real_connect = sqlite3.connect

        def watched(*args, **kwargs):
            db = real_connect(*args, **kwargs)
            opened.append(db)
            return db

        sqlite3.connect = watched
        try:
            inbox.complete(f"{0:032x}", "spoken words")
            inbox.status()
        finally:
            sqlite3.connect = real_connect
        self.assertTrue(opened)
        for db in opened:
            with self.assertRaises(sqlite3.ProgrammingError):
                db.execute("SELECT 1")

    def test_startup_sweeps_audio_left_by_a_crash(self):
        inbox = Inbox(self.root)
        audio = inbox.audio / "orphan.m4a"
        audio.write_bytes(b"audio")
        with inbox.connect() as db:
            db.execute("INSERT INTO chunks (id,sha256,device,started,duration,path,status,"
                       "transcript,received) VALUES (?,?,?,?,?,?,?,?,?)",
                       (f"{0:032x}", "0" * 64, "dev", "2026-03-10T09:00:00.000Z", 60.0,
                        str(audio), "complete", "spoken words", 0.0))
        Inbox(self.root)  # restart
        self.assertFalse(audio.exists())


if __name__ == "__main__":
    unittest.main()
