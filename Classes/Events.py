class Event:
    medium_time = []

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
    def __init__(self, purchase, status, price, datetime):
        super().__init__(datetime)
        self.purchase = purchase
        self.status = status
        self.price = price

    def to_string(self):
        info = super().to_string()
        info += "Purchase: " + str(self.purchase) + ", Price: " + self.price + ", Status: " + str(self.status)
        return info