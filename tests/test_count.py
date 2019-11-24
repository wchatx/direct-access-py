"""
test_query.py

Validate that the query method returns the expected number of records for Python 2.7 and Python 3.4+
"""

import os
from datetime import datetime, timedelta

from directaccess import DirectAccessV2

DIRECTACCESS_API_KEY = os.getenv('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.getenv('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.getenv('DIRECTACCESS_CLIENT_SECRET')


def test_count():
    """
    Authenticate and query Direct Access API Rigs endpoint and validate that records are returned
    :return:
    """
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10
    )

    count = d2.count('rigs', deleteddate='null', updateddate='ge({})'.format(
        datetime.strftime(datetime.now() - timedelta(days=30), '%Y-%m-%d')
    ))
    assert count is not None
    assert isinstance(count, int)

    return


if __name__ == '__main__':
    test_count()
