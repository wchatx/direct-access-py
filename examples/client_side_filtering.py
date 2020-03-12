"""
client_side_filtering.py

This example demonstrates using client-side filtering to query on columns that
aren't filterable via the API. While there's no speed up using
this method, we're able to query the API responses down in a memory-efficient way
and without loading unneeded records into our workflow.

Consider this the equivalent of a full table scan in a database.

In the sample below, we're requesting all records in Texas and without DeletedDates in batches of 10k.
Then, we're filtering the responses down to those records that have had their
production allocated using Drillinginfo's production allocation algorithm and
where LowerPerf values exist and are greater than or equal to 2000 and UpperPerf
values exist and are less than or equal to 3000.
"""
import os
try:  # Use the memory-efficient ifilter function available in itertools for Python 2
    from itertools import ifilter as filter
except ImportError:  # The built in filter function returns a generator in Python 3
    pass

from directaccess import DirectAccessV2

# Initialize our Direct Access object
d2 = DirectAccessV2(
    api_key=os.getenv('DIRECTACCESS_API_KEY'),
    client_id=os.getenv('DIRECTACCESS_CLIENT_ID'),
    client_secret=os.getenv('DIRECTACCESS_CLIENT_SECRET')
)

# Build the API query
query = d2.query('producing-entities', pagesize=10000, deleteddate='eq(null)', state='TX')

# Build the client-side filter
rows = filter(lambda x:
              x['AllocPlus'] == 'Y'
              and x['LowerPerf'] is not None
              and x['LowerPerf'] >= 2000
              and x['UpperPerf'] is not None
              and x['UpperPerf'] <= 3000,
              query)

# Execute the query and filter the responses
# Note that there will be periods of apparent inactivity while records we don't need are tossed
for row in rows:
    print(row)
