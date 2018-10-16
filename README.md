# direct-access-py
A thin wrapper around Drillinginfo's Direct Access API


### Install
```commandline
python setup.py install
```

### Usage
```commandline
from directaccess import DirectAccessV1, DirectAccessV2

d1 = DirectAccessV1(api_key=<your-api-key>)
for row in d1.query('landtrac-leases', county_parish='Reeves', state_province='TX', min_expiration_date='2018-06-01'):
    print(row)

# initialize DirectAccessV2 object
d2 = DirectAccessV2(
    client_id=<your-client-id>,
    client_secret=<your-client-secret>,
    api_key=<your-api-key>
)

# iterate over rows as dicts
for row in d2.query('well-origins', pagesize=10000):
    print(row)
```