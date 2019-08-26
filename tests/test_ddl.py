"""
test_ddl.py

Validate that the query method returns the expected DDL statement for Python 2.7 and Python 3.4+
"""
import os

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
        client_secret=DIRECTACCESS_CLIENT_SECRET
    )

    ddl = d2.ddl('rigs', database='pg')
    with open('create_rigs_table.sql', mode='wb+') as f:
        f.write(ddl)
        f.seek(0)
        for line in f:
            assert line.split(b' ')[0] == b'CREATE'
            break

    return


if __name__ == '__main__':
    test_ddl()
