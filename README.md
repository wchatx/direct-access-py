# direct-access-py
[![CircleCI](https://circleci.com/gh/wchatx/direct-access-py/tree/master.svg?style=svg)](https://circleci.com/gh/wchatx/direct-access-py/tree/master)
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

d1 = DirectAccessV1(api_key=<your-api-key>)
```

Provide the query method the dataset as the first argument and any query parameters as keyword arguments.
See valid dataset names and query params in the Direct Access documentation.
The query method returns a generator of API responses as dicts.
```python
for row in d1.query('landtrac-leases', county_parish='Reeves', state_province='TX', min_expiration_date='2018-06-01'):
    print(row)
```

### Direct Access Version 2
For version 2 of the API, create an instance of the DirectAccessV2 class, providing it your API key, client id and client secret.
The returned access token will be available as an attribute on the instance (d2.access_token) and the Authorization
header is set automatically
```python
from directaccess import DirectAccessV2

d2 = DirectAccessV2(
    client_id=<your-client-id>,
    client_secret=<your-client-secret>,
    api_key=<your-api-key>
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
When making requests containing certain characters, use a backslash to escape them.  
```python
# Escaping the comma before LLC
for row in d2.query('producing-entities', curropername='PERCUSSION PETROLEUM OPERATING\, LLC'):
    print(row)

```
