# direct-access-py
[![CircleCI](https://circleci.com/gh/wchatx/direct-access-py/tree/master.svg?style=svg)](https://circleci.com/gh/wchatx/direct-access-py/tree/master)
[![PyPI version](https://badge.fury.io/py/directaccess.svg)](https://badge.fury.io/py/directaccess) 

A thin wrapper around Drillinginfo's Direct Access API.  

This module is built and tested on Python 3.6 but should work on Python 2.7 and up.

### Install
```commandline
pip install directaccess
```

### Usage
```commandline
from directaccess import DirectAccessV1, DirectAccessV2

# For version 1 of the API, create an instance of the DirectAccessV1 class and provide it your API key
d1 = DirectAccessV1(api_key=<your-api-key>)

# Provide the query method the dataset as the first argument and any query parameters as keyword arguments.
# See valid dataset names and query params in the Direct Access documentation.
# The query method returns a generator of API responses as dicts.
for row in d1.query('landtrac-leases', county_parish='Reeves', state_province='TX', min_expiration_date='2018-06-01'):
    print(row)

# For version 2 of the API, create an instance of the DirectAccessV2 class, providing it your API key, client id and client secret.
# The returned access token will be available as an attribute on the instance (d2.access_token) and the Authorization
# header is set automatically
d2 = DirectAccessV2(
    client_id=<your-client-id>,
    client_secret=<your-client-secret>,
    api_key=<your-api-key>
)

# Like with the V1 class, provide the query method the dataset and query params. All query parameters must match the valid
# parameters found in the Direct Access documentation and be passed as keyword arguments.
for row in d2.query('well-origins', pagesize=10000):
    print(row)
```