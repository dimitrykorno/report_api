from report_api.Data.Data import get_data
from report_api.Utilities.Utils import time_count,check_folder,check_arguments
import matplotlib.pyplot as plt
from report_api.OS import OS
from report_api.Classes.QueryHandler import QueryHandler
import os
# noinspection PyDefaultArgument,PyDefaultArgument
@time_count
def new_report(os_list=["iOS"],
               folder_dest=None,
               app="sop",
               max_days=100,
               app_versions=['1.0'],
               users_limit=10000,
               app_entry_event_check=""):
    """
    Построение графиков Lifetime по одному/нескольким версиям одновременно
    :param os_list:
    :param max_days: максимальное кол-во дней на графике
    :param app_versions: версии приложения (вводить в виде '5.1', '5,3' )
    :return:
    """
    errors = check_arguments(locals())
    result_files = []
    if hasattr(new_report,'user'):
        folder_dest+=str(new_report.user)+"/"
    check_folder(folder_dest)

    if errors:
        return errors, result_files

    if not users_limit:
        users_limit=100000
    else:
        users_limit=int(users_limit)
    if not max_days:
        max_days = 365
    else:
        max_days = int(max_days)
    for os_str in os_list:
        plt.figure(figsize=(12, 8))
        plt.title("Отвалы " + os_str)
        user_aid=OS.get_aid(os_str)
        for app_version in app_versions:
            # Пользователи, у которых первое событие с нужной версией приложения
            sql = """
            select (MAX(event_timestamp)-MIN(event_timestamp)) as lifetime_sec
            from {0}
            where {1} in (select ios_ifa
                                from {0}
                                where {1}<>"" 
                                group by {1}, app_version_name
                                having MIN(app_version_name)={4}
                                )
                    and {2}
            group by {1}
            LIMIT {3}
            """.format(QueryHandler.get_db_name(os_str.lower(),app),user_aid ,app_entry_event_check,users_limit,app_version)
            file = open("sql " + os_str.lower() + " events.txt", "w")
            file.write(sql)
            file.close()
            lifetime_values, db = get_data(sql=sql, db=app, by_row=False, name="Загрузка лайфтаймов.")
            lifetime_values = [value["lifetime_sec"] for value in lifetime_values]
            db.close()

            lt = [0] * max_days

            # Формируем распределение лайфтаймов по дням
            for lifetime in lifetime_values:
                lifetime = round(lifetime / (60 * 60 * 24), 1)
                if int(lifetime) < max_days:
                    lt[int(lifetime)] += 1
            average_lifetime = int((sum(lifetime_values) / len(lifetime_values)) / (60 * 60 * 24))

            # удаляем хвосты
            while lt[-1] == 0:
                lt.pop()

            #print("Average lifetime", app_version, ":", average_lifetime, "дней")
            plt.plot(range(len(lt)), lt, label=app_version)

        plt.legend()
        filename = folder_dest + "users gone per day " + str(app_versions) + " " + os_str + ".png"
        plt.savefig(filename)
        result_files.append(os.path.abspath(filename))
        #plt.show()
    return errors,result_files