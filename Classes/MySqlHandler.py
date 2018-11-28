try:
    from MySQLdb import OperationalError
except:
    from mysql.connector import OperationalError

#from _mysql_exceptions import OperationalError
from report_api.Data.Data import get_data
from report_api.Utilities.Utils import time_count
from report_api.OS import OS
from  report_api import Report
import time

class MySQLHandler():
    """
    Обработчик MySQL осуществяет подключение к базе, получение списка установок, получение событий в chunk'ах,
    или построчно, получение следующего события для отчёта.
    """
    # порции установок (0 - все установки сразу) в запросе
    users_chunk = 0
    # текущая порция
    current_chunk = 0
    # части, на которые делится слишком большой кусок установок
    multiplier = 0.6
    installs_list = []
    installs_dict_aid={}
    installs_dict_id = {}

    # обработчики запросов событий и установок
    events_handler = None
    installs_handler = None

    # получение результата построчно или загрузка всего в оперативную память
    app = None
    by_row = True
    result = None
    db = None

    #время fetch
    time_0=[]
    i=0

    # подключение закончено (получены все chunk событий из запроса)
    completed_connection = False


    @classmethod
    def reset(cls):
        """
        Обнуление данных
        :return:
        """

        cls.users_chunk = 0
        cls.current_chunk = 0
        cls.installs_list = []
        cls.events_handler = None
        cls.installs_handler = None
        cls.by_row = True
        cls.result = None
        cls.db = None
        cls.completed_connection = False

    @classmethod
    def fetch_next_event(cls):

        while True:

            if cls.by_row and cls.result:
                # если нужно построчное подключение
                # try:

                #time1=time.perf_counter()
                event_data = cls.result.fetchoneDict()
                #if time.perf_counter()-time1>1:
                #    print("Long fetch.",cls.i,': {0:.8f} sec'.format(time.perf_counter()-time1))
                cls.i+=1
                #event_data = cls.result.fetch_row(maxrows=1, how=1)
                #cls.time_0.append(time.perf_counter()-time1)
                # берем 0й элемент, т.к. оно приходит в виду (event_data,)
                if event_data:
                    # отправляем событие в отчёт
                    #return event_data[0]
                    return event_data
                elif cls.completed_connection:
                    # если все данные получены, закрываем подключение и останавливаем отчёт
                    cls.result.close()
                    cls.db.close()
                    cls.result = None
                    return None
                else:
                    # закрываем предыдущее подключение и уходим в цикл
                    cls.result.close()
                    cls.db.close()
                    cls.result = None

            elif (not cls.by_row) and cls.result:
                # если в массиве что-то есть, отправляем первое событие
                return cls.result.pop()
            # если подключение не завершено, получаем следующую порцию событий
            elif not cls.completed_connection:
                cls.get_next_events()
            else:
                return None

    @classmethod
    def _update_events_handler(cls):
        """
        Добавление списка установок в запрос
        :return:
        """
        # if cls.users_chunk == 0 and len(cls.installs_list) > 15000:
        #     cls.users_chunk = 15000
        #     cls.current_chunk = 0
        # если chunk нулевой, добавляем все установки сразу, если нет, то добавляем следующий chunk
        if cls.users_chunk != 0:
            cls.events_handler.add_users_list(
                cls.installs_list[cls.users_chunk * cls.current_chunk: cls.users_chunk * (cls.current_chunk + 1)])
            cls.current_chunk += 1
            # если chunk последний, говорим, что подключение закончено
            # если добавляются все установки сразу, то окончание подключения определяется в другом месте
            if cls.users_chunk * cls.current_chunk >= len(cls.installs_list):
                cls.completed_connection = True
        else:
            cls.events_handler.add_users_list(cls.installs_list)

    @classmethod
    def get_next_events(cls):
        """
        Получение следующей порции событий из базы
        :return:
        """
        # Пока слеюущая порция события не получена
        got_next_events = False
        while not got_next_events:
            # добавление установок, если он иесть
            if cls.installs_list:
                cls._update_events_handler()
            # вывод сообщения о следующем chunk
            if cls.users_chunk != 0:
                print("Выставлен chunk пользователей: ", str((cls.current_chunk-1) * cls.users_chunk) + "-" +
                      str(min(cls.current_chunk  * cls.users_chunk, len(cls.installs_list))), " / ",
                      len(cls.installs_list))
            try:
                # подключение к базе и получение следующих данных
                cls.result, cls.db = get_data(cls.events_handler.get_query(), db=cls.app, by_row=cls.by_row,
                                              name="Запрос к базе событий.")
                got_next_events = True

                # если получаем chunk'ами и подключение завершено, то отключаем базу
                if not cls.by_row and cls.completed_connection:
                    cls.result.close()
                    cls.db.close()

            # учитываем ошибку слишком длинного запроса
            except OperationalError as er:
                print(er.args)
                # подключение не завершено
                cls.completed_connection = False
                # устанавливаем новые chunk установок
                if cls.installs_list:
                    cls.users_chunk = int(cls.users_chunk * cls.multiplier) if cls.users_chunk != 0 else int(
                        len(cls.installs_list) * cls.multiplier)
                    cls.current_chunk = 0
                    print("Ошибка запроса к базе. Слишком длинный запрос")
                else:
                    print("Ошибка запроса к базе.")
                    break
                cls.multiplier = cls.multiplier * 0.8
                continue
            except Exception as er:
                print(er.args)
                continue
        if cls.users_chunk == 0:
            cls.completed_connection = True

    @classmethod
    def set_fetch_mode(cls, by_row=False):
        cls.by_row = by_row

    @classmethod
    def get_installs(cls):
        """
        Получение списка установок
        :return:
        """
        query = cls.installs_handler.get_query()
        result, db = get_data(query, db=cls.app, by_row=False, name="Запрос к базе установок.")
        cls.installs_list = result
        db.close()
        #словарь для более выстрого доступа к конкретному пользователю
        id_1=OS.get_aid(cls.installs_handler.os)
        id_2 =OS.get_id(cls.installs_handler.os)
        for install in cls.installs_list:
            cls.installs_dict_aid[install[id_1]]=install
            cls.installs_dict_id[install[id_2]]=install
