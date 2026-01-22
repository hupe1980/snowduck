import os
import tempfile

from snowduck.connector import Connector


def test_put_copies_file_to_stage_dir(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        stage_dir = os.path.join(tmpdir, "stage")
        monkeypatch.setenv("SNOWDUCK_STAGE_DIR", stage_dir)

        source_path = os.path.join(tmpdir, "data.csv")
        with open(source_path, "w", encoding="utf-8") as f:
            f.write("a,b\n1,2\n")

        conn = Connector().connect()
        cur = conn.cursor()
        cur.execute(f"PUT file://{source_path} @mystage")

        copied = os.path.join(stage_dir, "mystage", "data.csv")
        assert os.path.exists(copied)
