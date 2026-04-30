import unittest

from backend.services.job_status_adapter import (
    legacy_job_status,
    legacy_upload_job_status,
    progress_result,
    public_job_result,
)


class JobStatusAdapterTest(unittest.TestCase):
    def test_progress_result_stores_internal_detail_and_metadata(self):
        result = progress_result("Membaca file...", detected_file_type="swfm_event")

        self.assertEqual(result["_progress_detail"], "Membaca file...")
        self.assertEqual(result["_detected_file_type"], "swfm_event")
        self.assertIsNone(public_job_result(result))

    def test_legacy_job_status_maps_running_to_processing(self):
        status = legacy_job_status(
            {
                "status": "running",
                "progress_phase": "reading",
                "progress_current": 2,
                "progress_total": 10,
                "result": progress_result("Membaca file..."),
                "error_message": None,
            }
        )

        self.assertEqual(status["status"], "processing")
        self.assertEqual(status["progress"]["detail"], "Membaca file...")
        self.assertEqual(status["progress"]["row"], 2)

    def test_legacy_upload_status_uses_detected_file_type(self):
        status = legacy_upload_job_status(
            {
                "status": "running",
                "progress_phase": "normalizing",
                "progress_current": 0,
                "progress_total": 0,
                "payload": {"filename": "tickets.parquet", "file_type": "auto"},
                "result": progress_result("Tipe terdeteksi", detected_file_type="swfm_realtime"),
                "error_message": None,
            }
        )

        self.assertEqual(status["filename"], "tickets.parquet")
        self.assertEqual(status["file_type"], "swfm_realtime")
        self.assertIsNone(status["result"])

    def test_legacy_job_status_exposes_failure_error(self):
        status = legacy_job_status(
            {
                "status": "failed",
                "progress_phase": "failed",
                "progress_current": 0,
                "progress_total": 0,
                "result": {},
                "error_message": "bad file",
            }
        )

        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["progress"]["detail"], "bad file")
        self.assertEqual(status["error"], "bad file")


if __name__ == "__main__":
    unittest.main()
