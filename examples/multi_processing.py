"""
multi_processing.py

This example demonstrates concurrent loading of Drillinginfo datasets via Python's multiprocessing module.

The DirectAccessV2 class accepts an optional access_token keyword argument (beginning in version 1.2.0).
When provided, an initial authentication request will not be made. We still provide our API Key, Client ID
and Client Secret to the class so that the access token can be refreshed if needed.

In the sample below, we simultaneously write three CSVs from the producing-entities, well-rollups and permits
API endpoints. This results in much faster loading time than when done sequentially.
"""
import os
import csv
from multiprocessing import Process

from directaccess import DirectAccessV2

# Retrieve our access token
ACCESS_TOKEN = DirectAccessV2(
    api_key=os.getenv('DIRECTACCESS_API_KEY'),
    client_id=os.getenv('DIRECTACCESS_CLIENT_ID'),
    client_secret=os.getenv('DIRECTACCESS_CLIENT_SECRET')
).access_token


def load(endpoint, **options):
    """
    A generic load function that will be called by each of the three processes.

    :param endpoint: the Direct Access API endpoint
    :param options: the query parameters to provide on the endpoint
    :return:
    """
    # Create a DirectAccessV2 client within the function, providing it our already existing access token
    # and thus avoiding unnecessary authentication calls
    client = DirectAccessV2(
        api_key=os.getenv('DIRECTACCESS_API_KEY'),
        client_id=os.getenv('DIRECTACCESS_CLIENT_ID'),
        client_secret=os.getenv('DIRECTACCESS_CLIENT_SECRET'),
        access_token=ACCESS_TOKEN
    )

    count = None
    with open(endpoint + '.csv', mode='w') as f:
        writer = csv.writer(f)
        for i, row in enumerate(client.query(endpoint, **options), start=1):
            count = i
            if count == 1:
                writer.writerow(row.keys())
            writer.writerow(row.values())

            if count % options.get('pagesize', 100000) == 0:
                print('Wrote {} records for {}'.format(count, endpoint))

    print('Completed writing {}. Final count: {}'.format(endpoint, count))
    return


def main():
    procs = list()
    well_rollups_process = Process(
        target=load,
        kwargs=dict(
            endpoint='well-rollups',
            pagesize=10000,
            deleteddate='eq(null)'
        )
    )
    procs.append(well_rollups_process)

    producing_entity_process = Process(
        target=load,
        kwargs=dict(
            endpoint='producing-entities',
            pagesize=100000,
            deleteddate='eq(null)'
        )
    )
    procs.append(producing_entity_process)

    permits_process = Process(
        target=load,
        kwargs=dict(
            endpoint='permits',
            pagesize=100000,
            deleteddate='eq(null)'
        )
    )
    procs.append(permits_process)

    [x.start() for x in procs]
    [x.join() for x in procs]
    return


if __name__ == '__main__':
    main()
