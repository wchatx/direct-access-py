import os

from directaccess import DirectAccessV2


def set_token():
    if not os.environ.get("DIRECTACCESS_TOKEN"):
        os.environ["DIRECTACCESS_TOKEN"] = DirectAccessV2(
            client_id=os.environ.get("DIRECTACCESS_CLIENT_ID"),
            client_secret=os.environ.get("DIRECTACCESS_CLIENT_SECRET"),
            api_key=os.environ.get("DIRECTACCESS_API_KEY"),
        ).access_token
    return
