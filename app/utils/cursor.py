from cachetools import TTLCache

class CursorCache(TTLCache):

    def __init__(self, ttl):
        super().__init__(1, ttl)

    def popitem(self):
        key, value = super().popitem()
        try:
            value['cursor'].close()
        except Exception as e:
            logger.error(e)
        try:
            value['connection'].close()
        except Exception as e:
            logger.error(e)
        return key, value