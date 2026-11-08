"""Fixture checks that do not require Docker or allocate a large index."""

import json
import tempfile
import unittest
from pathlib import Path

from s3_metadata import ACCOUNT, BUCKET, write_fixture


class MetadataFixtureTest(unittest.TestCase):
    def test_aliases_and_versions_are_counted_as_metadata_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp) / "data"
            size = write_fixture(data, 4, 32)
            snapshot = data / "s3-objects.json"
            objects = json.loads(snapshot.read_text())
            self.assertEqual(size, snapshot.stat().st_size)
            self.assertEqual(len(objects), 4)
            self.assertEqual(sum("#v#" in key for key in objects), 2)
            for key, value in objects.items():
                self.assertTrue(key.startswith(f"{ACCOUNT}/{BUCKET}/"))
                self.assertEqual(len(value["metadata"]["synthetic-padding"]), 32)
                self.assertNotIn("data", value)
            self.assertEqual(objects[list(objects)[0]], objects[list(objects)[1]])
            buckets = json.loads((data / "s3-buckets.json").read_text())
            self.assertEqual(buckets[f"{ACCOUNT}/{BUCKET}"]["region"], "us-west-2")
            self.assertEqual(
                buckets[f"{ACCOUNT}/{BUCKET}"]["versioningStatus"], "Enabled"
            )

    def test_odd_entry_count_stays_exact(self):
        with tempfile.TemporaryDirectory() as temp:
            data = Path(temp)
            write_fixture(data, 3, 0)
            objects = json.loads((data / "s3-objects.json").read_text())
            self.assertEqual(len(objects), 3)
            self.assertEqual(sum("#v#" in key for key in objects), 1)

    def test_rejects_invalid_fixture_sizes(self):
        with tempfile.TemporaryDirectory() as temp:
            for entries, padding in [(0, 0), (-1, 0), (1, -1)]:
                with (
                    self.subTest(entries=entries, padding=padding),
                    self.assertRaises(ValueError),
                ):
                    write_fixture(Path(temp), entries, padding)


if __name__ == "__main__":
    unittest.main()
