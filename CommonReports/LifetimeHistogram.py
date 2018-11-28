from report_api.Data.Data import get_data
from report_api.Utilities.Utils import time_count
import matplotlib.pyplot as plt


# noinspection PyDefaultArgument,PyDefaultArgument
@time_count
def new_report(os_list=["iOS"], max_days=100, app_versions=['5.1', '5.3']):
    """
    Построение графиков Lifetime по одному/нескольким версиям одновременно
    :param os_list:
    :param max_days: максимальное кол-во дней на графике
    :param app_versions: версии приложения (вводить в виде '5.1', '5,3' )
    :return:
    """
    for os_str in os_list:
        plt.figure(figsize=(12, 8))
        plt.title("Отвалы " + os_str)
        for app_version in app_versions:
            # Пользователи, у которых первое событие с нужной версией приложения
            sql = """
            select (MAX(event_timestamp)-MIN(event_timestamp)) as lifetime_sec
            from sop_events.events_{}
            where ios_ifa in (select ios_ifa
                                from sop_events.events_ios
                                where ios_ifa<>"" and event_json like "%InitGameState%"
                                group by ios_ifa, app_version_name
                                having MIN(app_version_name)={})
                    and event_json like "%InitGameState%"
            group by ios_ifa
            """.format(os_str, app_version)
            lifetime_values, db = get_data(sql=sql, by_row=False, name="Загрузка лайфтаймов.")
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

            print("Average lifetime", app_version, ":", average_lifetime, "дней")
            plt.plot(range(len(lt)), lt, label=app_version)
            plt.savefig("Results/Гистограма отвалов по lifetime/Отвалы по дням " + app_version + " " + os_str + ".png")
        plt.legend()
        plt.show()
