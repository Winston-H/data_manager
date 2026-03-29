import tempfile
import unittest
from pathlib import Path

from app.services.importer import _load_csv_with_polars, _prepare_polars_frame


class ImportCsvInferenceTest(unittest.TestCase):
    def test_csv_with_late_alphanumeric_id_reads_successfully(self) -> None:
        target_id = "51082119450609001X"
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "late_x.csv"
            rows = ["name,id_no,birth_year"]
            for idx in range(100):
                rows.append(f"样本{idx},11010119900101{idx:04d},2023")
            rows.append(f"尾行,{target_id},2023")
            csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

            df = _load_csv_with_polars(csv_path)
            cleaned, total_rows, skipped_rows = _prepare_polars_frame(df)

        self.assertEqual(total_rows, 101)
        self.assertEqual(skipped_rows, 0)
        self.assertEqual(cleaned.height, 101)
        self.assertIn(target_id, cleaned.get_column("id_no").to_list())
        self.assertEqual(cleaned.get_column("birth_year_raw").tail(1).item(), "2023")


if __name__ == "__main__":
    unittest.main()
