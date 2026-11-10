"""Comprehensive test suite for hermes-janus core engine."""

import shutil
import tempfile
from pathlib import Path
import pytest

from hermes_janus.cas import ContentAddressableStorage
from hermes_janus.wal import WriteAheadLog
from hermes_janus.dag import MerkleDag
from hermes_janus.engine import JanusEngine


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


class TestCAS:
    def test_put_and_get_bytes(self, temp_dir):
        cas = ContentAddressableStorage(temp_dir)
        data = b"Hello Hermes Janus"
        h = cas.put_bytes(data)
        assert len(h) == 64
        assert cas.has_blob(h)
        assert cas.get_bytes(h) == data

    def test_deduplication(self, temp_dir):
        cas = ContentAddressableStorage(temp_dir)
        f1 = temp_dir / "f1.txt"
        f2 = temp_dir / "f2.txt"
        f1.write_text("identical content")
        f2.write_text("identical content")

        h1, s1 = cas.put_file(f1)
        h2, s2 = cas.put_file(f2)
        assert h1 == h2
        assert s1 == s2
        assert len(list((temp_dir / "objects").glob("*/*"))) == 1


class TestWAL:
    def test_prepare_and_commit(self, temp_dir):
        wal = WriteAheadLog(temp_dir)
        tx_id = wal.prepare("write_file", {"path": "auth.py"}, {"auth.py": "oldhash"})
        assert len(wal.get_pending_transactions()) == 1
        assert wal.get_pending_transactions()[0].tx_id == tx_id

        wal.commit(tx_id, commit_sha="abc1234")
        assert len(wal.get_pending_transactions()) == 0

    def test_abort(self, temp_dir):
        wal = WriteAheadLog(temp_dir)
        tx_id = wal.prepare("terminal", {"command": "rm -rf"}, {})
        assert len(wal.get_pending_transactions()) == 1

        wal.abort(tx_id, error="Interrupted")
        assert len(wal.get_pending_transactions()) == 0


class TestDAG:
    def test_commit_chain_and_vector_clocks(self, temp_dir):
        dag = MerkleDag(temp_dir)
        assert dag.get_current_branch() == "main"

        c1 = dag.save_commit(manifest={"a.txt": "hash1"}, message="First commit")
        assert c1.vector_clock == {"main": 1}
        assert dag.get_head_sha() == c1.sha

        c2 = dag.save_commit(manifest={"a.txt": "hash2"}, message="Second commit")
        assert c2.vector_clock == {"main": 2}
        assert c2.parent_sha == c1.sha

        history = dag.get_history()
        assert len(history) == 2
        assert history[0].sha == c2.sha
        assert history[1].sha == c1.sha

        timeline_ascii = dag.render_timeline_ascii()
        assert "Second commit" in timeline_ascii
        assert "First commit" in timeline_ascii


class TestJanusEngine:
    def test_snapshot_and_rewind(self, temp_dir):
        engine = JanusEngine(temp_dir)
        test_file = temp_dir / "main.py"
        test_file.write_text("print('version 1')")

        c1 = engine.snapshot("v1 created")

        test_file.write_text("print('version 2 with bug')")
        c2 = engine.snapshot("v2 bug introduced")

        assert "version 2" in test_file.read_text()

        # Rewind 1 step
        success, msg = engine.rewind(1)
        assert success
        assert "version 1" in test_file.read_text()

    def test_file_deletion_on_rewind(self, temp_dir):
        engine = JanusEngine(temp_dir)
        base_file = temp_dir / "base.txt"
        base_file.write_text("base")
        c1 = engine.snapshot("base")

        extra_file = temp_dir / "temp_break.txt"
        extra_file.write_text("unwanted file")
        c2 = engine.snapshot("added temp")
        assert extra_file.exists()

        success, _ = engine.rewind(1)
        assert success
        assert not extra_file.exists()
        assert base_file.exists()

    def test_branching_and_merge(self, temp_dir):
        engine = JanusEngine(temp_dir)
        file_a = temp_dir / "feature.py"
        file_a.write_text("# initial")
        engine.snapshot("initial")

        # Fork to branch-b
        engine.branch("branch-b")
        file_b = temp_dir / "module_b.py"
        file_b.write_text("# code from branch b")
        engine.snapshot("added module b on branch-b")

        # Switch back to main
        engine.branch("main")
        assert not file_b.exists()

        # Merge branch-b into main
        success, msg = engine.merge("branch-b")
        assert success
        assert file_b.exists()
        assert file_b.read_text() == "# code from branch b"
