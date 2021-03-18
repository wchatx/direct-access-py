import os
import logging
from tempfile import TemporaryFile

from directaccess import (
    DirectAccessV2,
    DADatasetException,
    DAQueryException,
    DAAuthException,
)
from tests.utils import set_token

set_token()


LOG_LEVEL = logging.DEBUG
if os.environ.get("GITHUB_SHA"):
    LOG_LEVEL = logging.ERROR
DIRECTACCESS_API_KEY = os.environ.get("DIRECTACCESS_API_KEY")
DIRECTACCESS_CLIENT_ID = os.environ.get("DIRECTACCESS_CLIENT_ID")
DIRECTACCESS_CLIENT_SECRET = os.environ.get("DIRECTACCESS_CLIENT_SECRET")
DIRECTACCESS_TOKEN = os.environ.get("DIRECTACCESS_TOKEN")


def test_v2_query():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        access_token=DIRECTACCESS_TOKEN,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
    )

    query = d2.query("rigs", pagesize=10000, deleteddate="null")
    records = list()
    for i, row in enumerate(query, start=1):
        records.append(row)
        if i % 1000 == 0:
            break
    assert records


def test_docs():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        access_token=DIRECTACCESS_TOKEN,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
    )
    docs = d2.docs("well-origins")
    if docs:
        assert isinstance(docs, list)
    return


def test_ddl():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        access_token=DIRECTACCESS_TOKEN,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
    )
    ddl = d2.ddl("rigs", database="pg")
    with TemporaryFile(mode="w+") as f:
        f.write(ddl)
        f.seek(0)
        for line in f:
            assert line.split(" ")[0] == "CREATE"
            break

    # Neg - test ddl with invalid database parameter
    try:
        ddl = d2.ddl("rigs", database="invalid")
    except DAQueryException:
        pass

    return


def test_count():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        access_token=DIRECTACCESS_TOKEN,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
    )
    count = d2.count("rigs", deleteddate="null")
    assert count is not None
    assert isinstance(count, int)

    # Neg - test count for invalid dataset
    try:
        count = d2.count("invalid")
    except DADatasetException as e:
        pass
    return


def test_token_refresh():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10,
        log_level=LOG_LEVEL,
        access_token="invalid",
    )
    invalid_token = d2.access_token
    count = d2.count("rigs", deleteddate="null")
    query = d2.query("rigs", pagesize=10000, deleteddate="null")
    assert len([x for x in query]) == count
    assert invalid_token != d2.access_token

    # Test client with no credentials
    try:
        d2 = DirectAccessV2(
            api_key=None, client_id=None, client_secret=None, log_level=LOG_LEVEL
        )
    except DAAuthException as e:
        pass

    return
