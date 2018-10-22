class Event:
    __slots__ = 'datetime'
    medium_time = []
    medium_time_2 = []

    def __init__(self, datetime):
        self.datetime = datetime
        pass

    def print(self):
        print(self.__dict__)

    def to_string(self):
        return str(self.__class__.__name__) + ": "

    def to_short_string(self):
        return self.to_string()


class PurchaseEvent(Event):
    __slots__ = 'obj_name', 'status', 'price'

    def __init__(self, purchase, status, price, datetime):
        super().__init__(datetime)
        self.obj_name = purchase
        self.status = status
        self.price = price



    def to_string(self):
        info = super().to_string()
        info += "Purchase: " + str(self.obj_name) + ", Price: " + self.price + ", Status: " + str(self.status)
        return info
