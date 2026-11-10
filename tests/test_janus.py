"""Zero-dependency test suite using standard library unittest."""

import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from hermes_janus.cas import ContentAddressableStorage
from hermes_janus.wal import WriteAheadLog
from hermes_janus.dag import MerkleDag
from hermes_janus.engine import JanusEngine


class TestJanus(unittest.TestCase):
    def setUp(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_cas_storage(self):
        cas = ContentAddressableStorage(self.temp_dir)
        h = cas.put_bytes(b"Test CAS")
        self.assertEqual(len(h), 64)
        self.assertTrue(cas.has_blob(h))
        self.assertEqual(cas.get_bytes(h), b"Test CAS")

    def test_wal_durability(self):
        wal = WriteAheadLog(self.temp_dir)
        tx = wal.prepare("write_file", {"path": "test.txt"}, {})
        self.assertEqual(len(wal.get_pending_transactions()), 1)
        wal.commit(tx, "sha123")
        self.assertEqual(len(wal.get_pending_transactions()), 0)

    def test_merkle_dag(self):
        dag = MerkleDag(self.temp_dir)
        c1 = dag.save_commit({"f1": "h1"}, "Commit 1")
        c2 = dag.save_commit({"f1": "h2"}, "Commit 2")
        self.assertEqual(c2.parent_sha, c1.sha)
        timeline = dag.render_timeline_ascii()
        self.assertIn("Commit 1", timeline)
        self.assertIn("Commit 2", timeline)

    def test_engine_rewind_and_branch(self):
        engine = JanusEngine(self.temp_dir)
        f = self.temp_dir / "app.py"
        f.write_text("v1")
        engine.snapshot("v1")

        f.write_text("v2-broken")
        engine.snapshot("v2")

        # Rewind
        success, msg = engine.rewind(1)
        self.assertTrue(success)
        self.assertEqual(f.read_text(), "v1")

        # Branch
        engine.branch("feature-x")
        f2 = self.temp_dir / "x.py"
        f2.write_text("feat x")
        engine.snapshot("feat x")

        # Switch to main
        engine.branch("main")
        self.assertFalse(f2.exists())

        # Merge
        success, _ = engine.merge("feature-x")
        self.assertTrue(success)
        self.assertTrue(f2.exists())

    def test_garbage_collection(self):
        engine = JanusEngine(self.temp_dir)
        f = self.temp_dir / "data.txt"
        f.write_text("initial content")
        engine.snapshot("initial")

        # Generate a blob that will be overwritten
        f.write_text("intermediate content to prune")
        engine.snapshot("temp")

        f.write_text("final content")
        engine.snapshot("final")

        info_before = engine.storage_info()
        self.assertGreater(info_before["total_bytes"], 0)
        self.assertGreaterEqual(info_before["blob_count"], 3)

        # Run GC with max_history=1 to prune older commits
        gc_result = engine.gc(max_history=1)
        self.assertGreater(gc_result["commits_pruned"], 0)

        info_after = engine.storage_info()
        self.assertLessEqual(info_after["commit_count"], 1)


if __name__ == "__main__":
    unittest.main()
