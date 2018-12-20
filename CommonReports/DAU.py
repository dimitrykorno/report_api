from datetime import datetime
import pandas as pd
from report_api.Classes.QueryHandler import QueryHandler
from report_api.Report import Report
from report_api.Data import Data
from report_api.Utilities.Utils import check_folder, draw_plot, try_save_writer,check_arguments
from report_api.Classes.Events import *
import os

# noinspection PyDefaultArgument,PyDefaultArgument
def new_report(app=None,
               folder_dest=None,
               events_list=[],
               os_list=["iOS", "Android"],
               period_start="2018-10-01",
               period_end="2018-11-15",
               min_version=None,
               max_version=None,
               countries_list=[]
               ):
    """
    Расчет накопительного ARPU и ROI по паблишерам и трекинговым ссылкам(источникам)

    :param os_list:
    :param min_version:
    :param max_version:
    :param countries_list:
    :param period_start: начало периода
    :param period_end: конец периода
    :param days_since_install: рассчитное кол-во дней после установки

    :return:
    """
    errors = check_arguments(locals())
    result_files = []
    if hasattr(new_report,'user'):
        folder_dest+=str(new_report.user)+"/"
    check_folder(folder_dest)

    if errors:
        return errors, result_files

    if not period_start:
        period_start = "2018-01-01"
    # Приводим границы периода к виду datetime.date
    if isinstance(period_start, str):
        period_start = datetime.strptime(period_start, "%Y-%m-%d").date()
    if period_end:
        if isinstance(period_end, str):
            period_end = datetime.strptime(period_end, "%Y-%m-%d").date()
    else:
        period_end = datetime.now().date()

    title = " MAU " + str(period_start) + "-" + str(period_end)
    # БАЗА ДАННЫХ
    json_line = ""
    if events_list:
        json_line = QueryHandler.add_events_line(events_list)

    app_version = ""
    if min_version:
        app_version += " and app_version_name > '{}' ".format(min_version)
        title += " " + min_version
    if max_version:
        app_version += " and app_version_name < '{}' ".format(max_version)
        title += "-" + max_version
    countries = ""
    if countries_list:
        countries = " and country_iso_code in ('" + "','".join(countries_list)+"')"
        title += " in " + str(countries_list)

    for os_str in os_list:
        title=os_str+title
        sql = """
            SELECT date(event_datetime) as day,COUNT(ios_ifa) as users
            from {0}
            where event_datetime between '{1}' and '{2}' {3} {4} {5}
            group by date(event_datetime)
            order by date(event_datetime)
            """.format(QueryHandler.get_db_name(os_str.lower(),app), period_start, period_end, json_line, app_version,countries)

        result, db = Data.get_data(sql, app, by_row=False, name="Рассчет DAU.")
        db.close()
        x = [r["day"] for r in result]
        y = [r["users"] for r in result]
        check_folder(folder_dest)

        draw_plot(range(len(x)), {"DAU": y}, x_ticks_labels=x, show=True, folder=folder_dest,
                  title=title, calculate_sum=False)
        df = pd.DataFrame(index=x, columns=["users"])
        df["users"] = y
        filename=folder_dest + title + ".xlsx"
        writer = pd.ExcelWriter(filename)
        df.to_excel(writer)
        try_save_writer(writer, filename)
        result_files.append(os.path.abspath(filename))
    return errors,result_files
