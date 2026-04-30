import unittest

from backend.services.canonical_ticket_schema import (
    normalize_column_name,
    select_bronze_columns_sql,
    validate_ticket_columns,
)


class CanonicalTicketSchemaTest(unittest.TestCase):
    def test_normalize_column_name(self):
        self.assertEqual(normalize_column_name("Ticket Number INAP"), "ticket_number_inap")
        self.assertEqual(normalize_column_name(" Site ID "), "site_id")

    def test_validate_ticket_columns_requires_site_and_severity(self):
        result = validate_ticket_columns(["Ticket Number INAP", "Site ID"])

        self.assertFalse(result["valid"])
        self.assertEqual(result["missing_required"], ["severity"])

    def test_validate_ticket_columns_accepts_required_columns(self):
        result = validate_ticket_columns(["Site ID", "Severity", "Occured Time"])

        self.assertTrue(result["valid"])
        self.assertIn("site_id", result["known_columns"])
        self.assertIn("severity", result["known_columns"])

    def test_select_bronze_columns_sql_quotes_and_aliases(self):
        sql = select_bronze_columns_sql(["Site ID", "Severity"])

        self.assertIn('"Site ID" AS "site_id"', sql)
        self.assertIn('"Severity" AS "severity"', sql)


if __name__ == "__main__":
    unittest.main()
