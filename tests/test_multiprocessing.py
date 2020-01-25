"""
test_multiprocessing.py

Test that the originally acquired access_token is still valid and that no new
authentication request was sent from within the child processes.
"""

import os
import logging
from multiprocessing import Process

from directaccess import DirectAccessV2

DIRECTACCESS_API_KEY = os.environ.get('DIRECTACCESS_API_KEY')
DIRECTACCESS_CLIENT_ID = os.environ.get('DIRECTACCESS_CLIENT_ID')
DIRECTACCESS_CLIENT_SECRET = os.environ.get('DIRECTACCESS_CLIENT_SECRET')

if not os.environ.get('DIRECTACCESS_ACCESS_TOKEN'):
    access_token = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
    ).access_token
    os.environ['DIRECTACCESS_ACCESS_TOKEN'] = access_token
DIRECTACCESS_ACCESS_TOKEN = os.environ.get('DIRECTACCESS_ACCESS_TOKEN')

LOG_LEVEL = logging.DEBUG
if os.environ.get('CIRCLE_JOB'):
    LOG_LEVEL = logging.ERROR


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
        log_level=LOG_LEVEL
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
    if not DIRECTACCESS_ACCESS_TOKEN:
        access_token = DirectAccessV2(
            api_key=DIRECTACCESS_API_KEY,
            client_id=DIRECTACCESS_CLIENT_ID,
            client_secret=DIRECTACCESS_CLIENT_SECRET,
            retries=5,
            backoff_factor=10
        ).access_token
        os.environ['DIRECTACCESS_ACCESS_TOKEN'] = access_token
    else:
        access_token = DIRECTACCESS_ACCESS_TOKEN

    procs = list()
    a = Process(
        target=query,
        kwargs=dict(
            endpoint='rigs',
            access_token=access_token
        )
    )
    procs.append(a)

    b = Process(
        target=query,
        kwargs=dict(
            endpoint='permits',
            access_token=access_token
        )
    )
    procs.append(b)

    [x.start() for x in procs]
    [x.join() for x in procs]
    return
