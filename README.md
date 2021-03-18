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

### Direct Access Version 1 (deprecated)
Version 1 of the API has been deprecated and removed.

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

Provide the query method the dataset and query params. All query parameters must match the valid
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

### Network request handling
This module exposes functionality in python-requests for modifying network requests handling, namely:
* retries and backoff
* network proxies
* ssl verification

#### Retries and backoff
Specify the number of retry attempts in `retries` and the backoff factor in `backoff_factor`. See the urllib3
[Retry](https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.Retry) utility API for more info
```python
from directaccess import DirectAccessV2

d2 = DirectAccessV2(
    api_key='<your-api-key>',
    client_id='<your-client-id>',
    client_secret='<your-client-secret>',
    retries=5,
    backoff_factor=1
)
```

You can specify a network proxy by passing a dictionary with the host and port of your proxy to `proxies`. See the
[proxies](https://requests.readthedocs.io/en/master/user/advanced/#proxies) section of the python-requests documentation
for more info.
```python
from directaccess import DirectAccessV2

d2 = DirectAccessV2(
    api_key='<your-api-key>',
    client_id='<your-client-id>',
    client_secret='<your-client-secret>',
    proxies={'https': 'http://10.10.1.10:1080'}
)
```

Finally, if you're in an environment that provides its own SSL certificates that might not be in your trusted store,
you can choose to ignore SSL verification altogether. This is typically not a good idea and you should seek to resolve
certificate errors instead of ignore them.
```python
from directaccess import DirectAccessV2

d2 = DirectAccessV2(
    api_key='<your-api-key>',
    client_id='<your-client-id>',
    client_secret='<your-client-secret>',
    verify=False
)
```
