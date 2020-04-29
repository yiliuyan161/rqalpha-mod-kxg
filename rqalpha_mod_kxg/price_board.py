from rqalpha.data.bar_dict_price_board import BarDictPriceBoard
from rqalpha.environment import Environment

class KXGPriceBoard(BarDictPriceBoard):
    def __init__(self):
        self._env = Environment.get_instance()
        super(BarDictPriceBoard, self).__init__()

    def _get_bar(self, order_book_id):
        return self._env.get_bar(order_book_id)
