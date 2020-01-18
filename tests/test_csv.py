"""
test_csv.py

Test writing query results to CSV
"""

import os
import csv
import logging
from tempfile import mkdtemp

from directaccess import DirectAccessV2

DIRECTACCESS_API_KEY = os.getenv('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.getenv('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.getenv('DIRECTACCESS_CLIENT_SECRET')


def test_csv():
    """
    Write Direct Access query results to CSV

    :return:
    """
    tempdir = mkdtemp()
    path = os.path.join(tempdir, 'rigs.csv')
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10,
        log_level=logging.INFO
    )

    dataset = 'rigs'
    options = dict(
        pagesize=10000,
        deleteddate='null'
    )
    count = d2.count(dataset, **options)
    query = d2.query(dataset, **options)
    d2.to_csv(query, path=path, log_progress=True, delimiter=',', quoting=csv.QUOTE_MINIMAL)

    with open(path, mode='r') as f:
        reader = csv.reader(f)
        row_count = len([x for x in reader])
        assert row_count == (count + 1)


if __name__ == '__main__':
    test_csv()
