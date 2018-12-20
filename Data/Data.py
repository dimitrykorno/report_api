ENV = "laptop"
try:
    import MySQLdb
    import MySQLdb.cursors
    from _mysql_exceptions import OperationalError
except:
    import mysql.connector as MySQLdb
    import mysql.connector.cursor as cursors
    from mysql.connector import OperationalError
    ENV="server"

from report_api.Utilities.Utils import time_count


@time_count
def get_data(sql, db, by_row=True, name=""):
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

    try:
        print("Connecting from laptop. ","Checked" if ENV=="laptop" else "Libraries error")
        db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db=db + "_events", charset='utf8',
                             cursorclass=MySQLdb.cursors.SSDictCursor)
        c = db.cursor()
    except:
        print("Connecting from laptop. ", "Checked" if ENV == "server" else "Libraries error")
        db = MySQLdb.connect(host="localhost", user="root", passwd="hjkl098", db="analytics", charset='utf8')
        c = db.cursor(dictionary=True)

    # подключение через курсор, возвращающий список словарей с данными
    # запрос
    #db.ping(True)
    c.execute('SET NET_WRITE_TIMEOUT = 3600')
    new_range_capacity=8388608*10
    c.execute('SET range_optimizer_max_mem_size = {}'.format(new_range_capacity))
    c.execute(sql)

    # в зависимости от метода получения данных подключаемся по-разному
    try:
        if by_row:
                #результат - поток
                result = c
        else:
            # результат - список
            result = list(c) if c else None
            c.close()

    except OperationalError:
        raise OperationalError

    if not result:
        print("Ничего не найдено.")
    return result, db



def get_env():
    return ENV
