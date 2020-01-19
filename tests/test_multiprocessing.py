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


def query(endpoint, access_token, **options):
    """
    Query method target for multiprocessing child processes. Validates that after the first request,
    the originally acquired access_token equals the token available on the child client.

    :param endpoint: a valid Direct Access API dataset endpoint
    :param access_token:
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
        log_level=logging.INFO
    )

    resp = client.query(endpoint, **options)
    next(resp)
    assert access_token == client.access_token
    return


def test_multiple_processes():
    """
    Launch two child processes, one for rigs and one for permits.
    :return:
    """
    access_token = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        retries=5,
        backoff_factor=10
    ).access_token

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
