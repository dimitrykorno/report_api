import MySQLdb
import MySQLdb.cursors
from _mysql_exceptions import OperationalError
from report_api.Utilities.Utils import time_count


@time_count
def get_data(sql, by_row=True, name=""):
    if name != "":
        print(name + " Построчно: " + str(by_row))

    # db.query(sql)

    if by_row:
        db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db="sop_events", charset='utf8')
        db.query(sql)
        result = db.use_result()
    else:
        try:
            db = MySQLdb.connect(host="localhost", user="root", passwd="0000", db="sop_events", charset='utf8',
                                 cursorclass=MySQLdb.cursors.SSDictCursor)
            c = db.cursor()
            c.execute(sql)
            result = list(c)
        except OperationalError:
            raise OperationalError

    if result:
        return result, db
    else:
        print("Ничего не найдено.")
