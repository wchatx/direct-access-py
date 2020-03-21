import os
import logging

from pandas.api.types import is_datetime64_ns_dtype, is_float_dtype, is_int64_dtype

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


def test_dataframe():
    d2 = DirectAccessV2(
        api_key=DIRECTACCESS_API_KEY,
        client_id=DIRECTACCESS_CLIENT_ID,
        client_secret=DIRECTACCESS_CLIENT_SECRET,
        access_token=DIRECTACCESS_TOKEN
    )
    df = d2.to_dataframe('rigs', pagesize=10000, deleteddate='null')

    # Check index is set to API endpoint "primary key"
    assert df.index.name == 'RigID'

    # Check datetime64 dtypes
    assert is_datetime64_ns_dtype(df.CreatedDate)
    assert is_datetime64_ns_dtype(df.DeletedDate)
    assert is_datetime64_ns_dtype(df.SpudDate)
    assert is_datetime64_ns_dtype(df.UpdatedDate)

    # Check Int64 dtypes
    assert is_int64_dtype(df.PermitDepth)
    assert is_int64_dtype(df.FormationDepth)

    # Check float dtypes
    assert is_float_dtype(df.RigLatitudeWGS84)
    assert is_float_dtype(df.RigLongitudeWGS84)

    return
