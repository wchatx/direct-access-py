import os
import logging
from multiprocessing import Process

from directaccess import DirectAccessV2
from tests.utils import set_token

set_token()


LOG_LEVEL = logging.DEBUG
if os.environ.get("GITHUB_SHA"):
    LOG_LEVEL = logging.ERROR
DIRECTACCESS_API_KEY = os.environ.get("DIRECTACCESS_API_KEY")
DIRECTACCESS_CLIENT_ID = os.environ.get("DIRECTACCESS_CLIENT_ID")
DIRECTACCESS_CLIENT_SECRET = os.environ.get("DIRECTACCESS_CLIENT_SECRET")
DIRECTACCESS_TOKEN = os.environ.get("DIRECTACCESS_TOKEN")


def query(endpoint, access_token, **options):
    """
    Query method target for multiprocessing child processes.

    :param endpoint: a valid Direct Access API dataset endpoint
    :param access_token: a Direct Access API access token
    :param options: kwargs of valid query parameters for the dataset endpoint
    :return:
    """
    client = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=5,
        access_token=access_token,
        log_level=LOG_LEVEL,
    )

    resp = client.query(endpoint, **options)
    next(resp)
    assert resp
    return


def test_multiple_processes():
    """
    Launch two child processes, one for rigs and one for permits.
    :return:
    """
    procs = list()
    a = Process(
        target=query, kwargs=dict(endpoint="rigs", access_token=DIRECTACCESS_TOKEN)
    )
    procs.append(a)

    b = Process(
        target=query, kwargs=dict(endpoint="permits", access_token=DIRECTACCESS_TOKEN)
    )
    procs.append(b)

    [x.start() for x in procs]
    [x.join() for x in procs]
    return
