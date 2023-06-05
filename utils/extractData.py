import asyncio
import os
from typing import List
from kor import create_extraction_chain

from kor.nodes import Object, Text, Number

from langchain.chat_models import ChatOpenAI
from langchain.docstore.document import Document

from tools.korAPI import extract_from_documents

schema = Object(
    id="loan_info",
    description="Information about loans.",
    attributes=[
        Text(
            id="amount",
            description="Amount of the loan",
            examples=[
                (
                    """Borrower has requested that Lender provide a loan in the maximum aggregate principal amount of
                        Six Million Six Hundred Forty-Six Thousand and
                        No/100 Dollars ($6,646,000.00)""",
                    "6646000",
                ),
            ],
        ),
        Text(
            id="lender",
            description="Name of the lender",
            examples=[
                (
                    """between LAS VEGAS-CLARK COUNTY
                        LIBRARY DISTRICT FOUNDATION, INC., a Nevada non-profit corporation (“Lender”)""",
                    "LAS VEGAS-CLARK COUNTY LIBRARY DISTRICT FOUNDATION, INC",
                ),
                (
                    """This Revenue Share Agreement (“Agreement”) is made and entered into as of (mm/dd/yyyy)
                        12/12/2012 (“Effective Date”) between Clear Finance Technology Corporation (“we”, “us” or
                        “our”) and the company listed below """,
                    "Clear Finance Technology Corporation",
                ),
            ],
        ),
        Text(
            id="borrower",
            description="Name of the borrower",
            examples=[
                (
                    """CHASE NMTC MESQUITE LIBRARY INVESTMENT FUND, LLC, a Delaware
                        limited liability company (“Borrower”).""",
                    "CHASE NMTC MESQUITE LIBRARY INVESTMENT FUND, LLC",
                ),
                (
                    """COMPANY INFORMATION
                        Company Legal Name: ABC limited company""",
                    "ABC limited company",
                ),
            ],
        ),
        Text(
            id="created_time",
            description="Date the loan was created",
            examples=[
                (
                    """THIS FUND LOAN AGREEMENT (this “Agreement”), dated as of [December __,
                        2017] (“Effective Date”)""",
                    "2017-12",
                ),
            ],
        ),
        Text(
            id="interest_rate",
            description="Interest rate of the loan",
            examples=[
                (
                    """Interest Rate: 12.5% - Interest Rate is variable and based on Wall Street Journal
                        published Prime Rate, currently at 8% plus 4.5% Spread and adjusted quarterly.""",
                    "12.5% - Interest Rate is variable and based on Wall Street Journal published Prime Rate, currently at 8% plus 4.5% Spread and adjusted quarterly.",
                ),
                ("""Specified Percentage: 12%""", "12%"),
            ],
        ),
        Text(
            id="loan_duration",
            description="Duration of the loan until it's paid off",
            examples=[
                (
                    """Loan Term: 120.00 Months, Fully Amortized""",
                    "120 months",
                ),
            ],
        ),
        Text(
            id="monthly_payment",
            description="How much an individual pays each month",
            examples=[
                (
                    """Borrower shall make 103 payments of $3,000""",
                    "$3,000",
                ),
            ],
        ),

        #Text(id="summary", description="General summary of the loan document"),
    ],
    examples=[
        (
            "General summary of the loan document",
            [
                {
                    "amount": "100000",
                    "lender": "John Smith",
                    "borrower": "Jane Doe",
                    "created_time": "2020-10-20",
                    #"summary": "The loan is for the purpose of buying house, interest rate 3 percent anually, pay in 25 years",
                    "interest_rate": "The interest payable by the Borrower for each Interest Period shall be at a rate equal to the Reference Rate for the Loan Currency plus the Variable Spread",
                    "loan_term": "The principal amount of the Loan shall be repaid in accordance with the amortization schedule set forth in Schedule 3 to this Agreement.",
                    "monthly_payment": "Borrower shall make 103 payments of $3,000",
                },
                {
                    "amount": "200000",
                    "lender": "John Snow",
                    "borrower": "Viking Owner",
                    "created_time": "2020-12-20",
                    #"summary": "The loan is for the purpose of buying car, interest rate 3 percent anually, pay in 5 years",
                    "interest_rate": "12.5% - Interest Rate is variable and based on Wall Street Journal published Prime Rate, currently at 8% plus 4.5% Spread and adjusted quarterly.",
                    "loan_term": "120 months",
                },
            ],
        )
    ],
    many=True,
)


def extract_data(documents: List[Document], api_key: str):
    llm = ChatOpenAI(
        openai_api_key=api_key,
        model_name="gpt-3.5-turbo",
        temperature=0,
        max_tokens=2000,
    )
    chain = create_extraction_chain(
        llm,
        schema,
        encoder_or_encoder_class="JSON",
        input_formatter="triple_quotes",
    )

    document_extraction_results = asyncio.run(extract_from_documents(chain, documents))

    return document_extraction_results[0]["loan_info"]
