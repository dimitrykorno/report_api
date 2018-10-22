import MySQLdb
import MySQLdb.cursors
from _mysql_exceptions import OperationalError
from report_api.Utilities.Utils import time_count


@time_count
def get_data(sql,db, by_row=True, name=""):
    """
    Подключение к базе и запрос к базе
    :param sql: SQL запрос
    :param by_row: Построчное получение данных из базы (False - полная выгрузка в оперативную память)
    :param name: Название подключения для вывода в консоль.
    :return:
    """
    # если задано имя - печатаем название подключения
    if name != "":
        print(name + " Построчно: " + str(by_row))

    # в зависимости от метода получения данных подключаемся по-разному
    if by_row:
        try:
            # # стандартное подключение
            # db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db=db+"_events", charset='utf8')
            # # запрос
            # db.query(sql)
            # # получаем данные построчно
            # result = db.use_result()
            db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db=db + "_events", charset='utf8', cursorclass=MySQLdb.cursors.SSDictCursor)
            db.ping(True)
            c=db.cursor()
            c.execute('SET GLOBAL connect_timeout=28800')
            c.execute('SET GLOBAL wait_timeout=28800')
            c.execute('SET GLOBAL interactive_timeout=28800')

            c.execute(sql)
            result = c
        except OperationalError:
            raise OperationalError
    else:
        try:
            # подключение через особый курсор, возвращающий список словарей с данными
            db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db=db+"_events", charset='utf8',
                                 cursorclass=MySQLdb.cursors.SSDictCursor)
            # создаем курсор для этого подключения
            c = db.cursor()
            # запрос
            c.execute(sql)
            # результат будет списком
            result = list(c)
        # учитываем ошибку слишком длинного запроса
        except OperationalError:
            raise OperationalError

    if result:
        return result, db
    else:
        print("Ничего не найдено.")
