from kiteconnect import KiteConnect
from config import KITE_API_KEY
 

class ZerodhaClient:

    def __init__(self, access_token):
        self.kite = KiteConnect(api_key=KITE_API_KEY)
        self.kite.set_access_token(access_token)

    def get_holdings(self):

        return self.kite.holdings()

    def get_positions(self):
        return self.kite.positions()

    def get_orders(self):
        return self.kite.orders()

    def get_trades(self, from_date=None, to_date=None):
        return self.kite.trades()