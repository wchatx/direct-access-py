"""
test_query.py

Test the DirectAccessV2 methods
"""

import os
import logging
from tempfile import TemporaryFile

from directaccess import DirectAccessV1, DirectAccessV2, DADatasetException, DAQueryException, DAAuthException

DIRECTACCESS_API_KEY = os.environ.get('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.environ.get('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.environ.get('DIRECTACCESS_CLIENT_SECRET')
LOG_LEVEL = logging.DEBUG
if os.environ.get('CIRCLE_JOB'):
    LOG_LEVEL = logging.ERROR


def test_query():
    """
    Authenticate and query Direct Access API for docs, ddl, count and query methods

    :return:
    """
    # Test V1 query
    d1 = DirectAccessV1(
        api_key=DIRECTACCESS_API_KEY,
        log_level=LOG_LEVEL
    )
    query = d1.query('rigs', pagesize=1000)
    records = list()
    for i, row in enumerate(query, start=1):
        records.append(row)
        if i % 1000 == 0:
            break
    assert records

    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL
    )

    # Test docs
    docs = d2.docs('well-origins')
    if docs:
        assert isinstance(docs, list)

    # Test DDL
    ddl = d2.ddl('rigs', database='pg')
    with TemporaryFile(mode='w+') as f:
        f.write(ddl)
        f.seek(0)
        for line in f:
            assert line.split(' ')[0] == 'CREATE'
            break

    # Test count
    count = d2.count('rigs', deleteddate='null')
    assert count is not None
    assert isinstance(count, int)

    # Neg - test count for invalid dataset
    try:
        count = d2.count('invalid')
    except DADatasetException as e:
        pass

    # Neg - test ddl with invalid database parameter
    try:
        ddl = d2.ddl('rigs', database='invalid')
    except DAQueryException:
        pass

    # Test query
    query = d2.query('rigs', pagesize=10000, deleteddate='null')
    records = list()
    for i, row in enumerate(query, start=1):
        records.append(row)
        if i % 1000 == 0:
            break
    assert records

    # Test token refresh
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
        access_token='invalid'
    )
    invalid_token = d2.access_token
    query = d2.query('rigs', pagesize=10000, deleteddate='null')
    assert len([x for x in query]) == count
    assert invalid_token != d2.access_token

    # Test client with no credentials
    try:
        d2 = DirectAccessV2(
            api_key=None,
            client_id=None,
            client_secret=None,
            log_level=LOG_LEVEL
        )
    except DAAuthException as e:
        pass

    return


if __name__ == '__main__':
    test_query()
