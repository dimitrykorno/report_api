class Event:
    __slots__ = 'datetime', 'test_name'
    medium_time = []
    medium_time_2 = []

    # (!) НЕ МЕНЯТЬ название группы "А" по-умолчанию. На него завязаны отчеты. (!)
    def __init__(self, datetime, test_name="A"):
        self.datetime = datetime
        self.test_name = test_name
        pass

    def set_ab_test_name(self,test_name):
        self.test_name = test_name

    def print(self):
        print(self.__dict__)

    def to_string(self):
        return str(self.__class__.__name__) + ": "

    def to_short_string(self):
        return self.to_string()


class PurchaseEvent(Event):
    __slots__ = 'obj_name', 'status', 'price'

    def __init__(self, purchase, status, price=0, datetime="1010-01-01"):
        super().__init__(datetime)
        self.obj_name = purchase
        self.status = status
        self.price = price

    def to_string(self):
        info = super().to_string()
        info += "Purchase: " + str(self.obj_name) + ", Price: " + str(self.price) + ", Status: " + str(self.status)
        return info
