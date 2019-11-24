"""
test_query.py

Validate that the query method returns the expected number of records for Python 2.7 and Python 3.4+
"""

import os
import logging

from directaccess import DirectAccessV2

DIRECTACCESS_API_KEY = os.getenv('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.getenv('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.getenv('DIRECTACCESS_CLIENT_SECRET')


def test_query():
    """
    Authenticate and query Direct Access API Rigs endpoint and validate that records are returned
    :return:
    """
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=5,
        log_level=logging.DEBUG
    )

    query = d2.query('rigs', pagesize=10000, deleteddate='null')
    records = list()
    for i, row in enumerate(query, start=1):
        records.append(row)

    assert records

    return


if __name__ == '__main__':
    test_query()
