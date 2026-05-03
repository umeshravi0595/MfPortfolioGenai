from kiteconnect import KiteConnect
import os
import hashlib
from dotenv import load_dotenv


load_dotenv()

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_API_SECRET = os.getenv("KITE_API_SECRET")


def get_login_url():
    """
    Returns Zerodha login URL
    """
    kite = KiteConnect(api_key=KITE_API_KEY)
    return kite.login_url()


def generate_session(request_token):
    """
    Exchanges request_token for access_token
    """
    kite = KiteConnect(api_key=KITE_API_KEY)

    data = kite.generate_session(
        request_token,
        api_secret=KITE_API_SECRET
    )

    return data["access_token"]