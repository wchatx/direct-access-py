# direct-access-py
![directaccess](https://github.com/wchatx/direct-access-py/workflows/directaccess/badge.svg)
[![PyPI version](https://badge.fury.io/py/directaccess.svg)](https://badge.fury.io/py/directaccess) 

A thin wrapper around Drillinginfo's Direct Access API. Handles authentication and token management, pagination and
network-related error handling/retries.  

This module is built and tested on Python 3.6 but should work on Python 2.7 and up.


## Install
```commandline
pip install directaccess
```

## Usage

### Direct Access Version 1
For version 1 of the API, create an instance of the DirectAccessV1 class and provide it your API key
```python
from directaccess import DirectAccessV1

d1 = DirectAccessV1(api_key='<your-api-key>')
```

Provide the query method the dataset as the first argument and any query parameters as keyword arguments.
See valid dataset names and query params in the Direct Access documentation.
The query method returns a generator of API responses as dicts.
```python
for row in d1.query('legal-leases', county_parish='Reeves', state_province='TX', min_expiration_date='2018-06-01'):
    print(row)
```

### Direct Access Version 2
For version 2 of the API, create an instance of the DirectAccessV2 class, providing it your API key, client id and client secret.
The returned access token will be available as an attribute on the instance (d2.access_token) and the Authorization
header is set automatically
```python
from directaccess import DirectAccessV2

d2 = DirectAccessV2(
    api_key='<your-api-key>',
    client_id='<your-client-id>',
    client_secret='<your-client-secret>',
)
```

Like with the V1 class, provide the query method the dataset and query params. All query parameters must match the valid
parameters found in the Direct Access documentation and be passed as keyword arguments.
```python
for row in d2.query('well-origins', county='REEVES', pagesize=10000):
    print(row)
```

### Filter functions
Direct Access version 2 supports filter funtions. These can be passed as strings on the keyword arguments.

Some common filters are greater than (`gt()`), less than (`lt()`), null, not null (`not(null)`) and between (`btw()`).  
See the Direct Access documentation for a list of all available filters.

```python
# Get well records updated after 2018-08-01 and without deleted dates
for row in d2.query('well-origins', updateddate='gt(2018-08-01)', deleteddate='null'):
    print(row)
    
# Get permit records with approved dates between 2018-03-01 and 2018-06-01
for row in d2.query('permits', approveddate='btw(2018-03-01,2018-06-01)'):
    print(row) 
```

You can use the `fields` keyword to limit the returned fields in your request.
```python
for row in d2.query('rigs', fields='DrillType,LeaseName,PermitDepth'):
    print(row)

```

### Escaping
When making requests containing certain characters like commas, use a backslash to escape them.  
```python
# Escaping the comma before LLC
for row in d2.query('producing-entities', curropername='PERCUSSION PETROLEUM OPERATING\, LLC'):
    print(row)

```

### Errors
Direct Access is a data api with hundreds of millions of records depending on your subscription. Networks are inherently unreliable
and errors are going to happen at some point. This module provides two means of dealing with errors;
configurable retries with exponential backoff (available for v1 and v2), and exposing the pagination
link as an attribute (v2 only).  

Retrying while making requests
```python
from directaccess import DirectAccessV1

# Retry 5 times, backing off exponentially 
# (1 second, 2 seconds, 4 seconds, 16 seconds, 256 seconds)
d1 = DirectAccessV1(
    api_key='<your-api-key>',
    retries=5,
    backoff_factor=1
)
```

In the event of an unrecoverable error, you can write your process in a way that persists the pagination links
so that you can pick back up where you left off. A basic implementation might look like this:
```python
import os
import json
from directaccess import DirectAccessV2

RECOVERY_FILE = 'your-api-links.json'

d2 = DirectAccessV2(
    api_key='<your-api-key>',
    client_id='<your-client-id>',
    client_secret='<your-client-secret>',
    retries=5,
    backoff_factor=1    
)

# if there's an existing recovery file, provide it to the instance
if os.path.exists(RECOVERY_FILE):
    with open(RECOVERY_FILE) as f:
        d2.links = json.loads(f.read())

# interact with the api, writing out a recovery file in the event of an unrecoverable error. 
# this will overwrite any previously existing file.
try:
    for row in d2.query('permits'):
        print(row)
except Exception:
    with open(RECOVERY_FILE, mode='w') as f:
        f.write(json.dumps(d2.links))

```
You could persist the pagination links any way you want. If provided, the DirectAccessV2 class expects a dictionary like 
the one provided from the [Requests module's links](http://docs.python-requests.org/en/master/user/advanced/#link-headers) 
and the json example above is just one way to do this.
