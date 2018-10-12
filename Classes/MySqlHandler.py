from _mysql_exceptions import OperationalError
from report_api.Data.Data import get_data
from report_api.Utilities.Utils import time_count


class MySQLHandler():
    users_chunk = 0
    current_chunk = 0
    multiplier = 0.6
    installs_list = []

    events_handler = None
    installs_handler = None

    by_row = True
    result = None
    db = None

    completed_connection = False
    '''
    def __init__(self, events_handler=None, installs_handler=None, users_chunk=0):
        MySQLHandler.events_handler = events_handler
        MySQLHandler.installs_handler = installs_handler
        if installs_handler:
            MySQLHandler.current_chunk = 0
            MySQLHandler.users_chunk = users_chunk
            MySQLHandler.get_installs()
        MySQLHandler.completed_connection = False
    '''

    @classmethod
    def reset(cls):
        cls.users_chunk = 0
        cls. current_chunk = 0
        cls.installs_list = []
        cls.events_handler = None
        cls.installs_handler = None
        cls.by_row = True
        cls.result = None
        cls.db = None
        cls.completed_connection=False

    @classmethod
    #@time_count
    def fetch_next_event(cls):
        while True:

            if cls.by_row and cls.result:
                # если нужно построчное подключение
                #try:
                event_data = cls.result.fetch_row(maxrows=1, how=1)
                # берем 0й элемент, т.к. оно приходит в виду (event_data,)
                if event_data:
                    # отправляем событие в отчёт
                    return event_data[0]
                elif cls.completed_connection:
                    # если все данные получены, закрываем подключение и останавливаем отчёт
                    cls.db.close()
                    cls.result = None
                    return None
                else:
                    # закрываем предыдущее подключение и уходим в цикл
                    cls.db.close()
                    cls.result = None
                '''
                except OperationalError:
                    # в случае ошибки с базой изменяем chunk
                    cls.completed_connection = False
                    cls.users_chunk = int(cls.users_chunk * cls.multiplier) if cls.users_chunk != 0 else int(
                        len(cls.installs_list) * cls.multiplier)
                    cls.current_chunk = 0
                    print("Ошибка получения данных из базы. Установка chunk:", cls.current_chunk * cls.users_chunk, "-",
                          str(min((cls.current_chunk + 1) * cls.users_chunk, len(cls.installs_list))), "/",
                          len(cls.installs_list))
                '''
            elif not cls.by_row and cls.result:
                # если в массиве что-то есть, отправляем первое событие
                return cls.result.pop()
            elif not cls.completed_connection:
                cls.get_next_events()
            else:
                return None

    @classmethod
    def _update_events_handler(cls):
        if cls.users_chunk != 0:
            cls.events_handler.add_users_list(
                cls.installs_list[cls.users_chunk * cls.current_chunk: cls.users_chunk * (cls.current_chunk + 1)])
            cls.current_chunk += 1
            if cls.users_chunk * cls.current_chunk >= len(cls.installs_list):
                cls.completed_connection = True
                # print("No more chuncks")
        else:
            cls.events_handler.add_users_list(cls.installs_list)

    @classmethod
    def get_next_events(cls):
        got_next_events = False
        while not got_next_events:
            if cls.users_chunk != 0:
                print("Выставлен размер chunk'а: ", str(cls.current_chunk * cls.users_chunk) + "-" +
                      str(min((cls.current_chunk + 1) * cls.users_chunk, len(cls.installs_list))), " / ",
                      len(cls.installs_list))
            if cls.installs_list:
                cls._update_events_handler()
            try:
                cls.result, cls.db = get_data(cls.events_handler.get_query(), by_row=cls.by_row,
                                              name="Запрос к базе событий.")
                got_next_events = True

                if not cls.by_row:
                    cls.db.close()

            except OperationalError:
                cls.completed_connection = False
                if cls.installs_list:
                    cls.users_chunk = int(cls.users_chunk * cls.multiplier) if cls.users_chunk != 0 else int(
                        len(cls.installs_list) * cls.multiplier)
                    cls.current_chunk = 0
                    print("Ошибка запроса к базе. Слишком длинный запрос")
                else:
                    print("Ошибка запроса к базе.")
                    break
                continue
        if cls.users_chunk == 0:
            cls.completed_connection = True

    @classmethod
    def set_fetch_mode(cls, by_row=False):
        cls.by_row = by_row

    @classmethod
    def get_installs(cls):
        query = cls.installs_handler.get_query()

        result, db = get_data(query, by_row=False, name="Запрос к базе установок.")
        cls.installs_list = result
        db.close()
