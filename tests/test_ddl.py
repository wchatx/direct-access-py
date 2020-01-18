"""
test_ddl.py

Validate that the query method returns the expected DDL statement for Python 2.7 and Python 3.4+
"""
import os
import logging
from tempfile import TemporaryFile

from directaccess import DirectAccessV2

DIRECTACCESS_API_KEY = os.getenv('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.getenv('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.getenv('DIRECTACCESS_CLIENT_SECRET')


def test_ddl():
    """
    Authenticate and query Direct Access API Rigs endpoint for a MSSQL DDL statement
    :return:
    """
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10,
        log_level=logging.INFO
    )

    ddl = d2.ddl('rigs', database='pg')
    with TemporaryFile(mode='w+') as f:
        f.write(ddl)
        f.seek(0)
        for line in f:
            assert line.split(' ')[0] == 'CREATE'
            break

    return
