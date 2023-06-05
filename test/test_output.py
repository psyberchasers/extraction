import unittest
import json



class TestJson(unittest.TestCase):
    def test_json_structure(self):
        expected_json = [
            {
        "created_time": "2023-04-27",
        "file": "term-sheet-3.pdf",
        "interest_rate": "12.50-13.00% - Interest Rate is variable and based on Wall Street Journal published Prime Rate, currently at 8% plus 4.50-5.00% Spread and adjusted quarterly.",
        "loan_duration": "72-120 Months, Fully Amortized. Based on loan amount",
        "monthly_payment": "Monthly estimated payments to be determined for principal and interest will be made through electronic transfer/Automated Clearing House (ACH) on the first Business Day of each month."

            },
           
        ]

        with open("output/output.json") as f:
            actual_json = json.load(f)

        self.assertEqual(expected_json, actual_json)


if __name__ == "__main__":
    unittest.main()
