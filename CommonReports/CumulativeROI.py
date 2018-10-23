from datetime import datetime, timedelta
from operator import truediv
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dateutil.rrule import rrule, DAILY
from report_api.OS import OS
from report_api.Report import Report
from report_api.Utilities.Utils import time_count, log_approximation
from Classes.Events import *



# noinspection PyDefaultArgument,PyDefaultArgument
def new_report(parser=None,
               user_class=None,
               app=None,
               folder_dest=None,
               events_list=[],
               os_list=["iOS", "Android"],
               days_since_install=28,
               period_start="2018-06-19",
               period_end=None,
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
    # Приводим границы периода к виду datetime.date
    if isinstance(period_start, str):
        period_start = datetime.strptime(period_start, "%Y-%m-%d").date()
    if isinstance(period_end, str):
        period_end = datetime.strptime(period_end, "%Y-%m-%d").date()

    # Списки CPI известных трекинговых ссылок
    cpi = {
        "unknown": 0,
        "Google Play": 0,
        "App Store": 0,
        "AppStore": 0,
        "Amazon": 0,
        "KB pro Masha": 15,
        "facebook_android": 0,

        "Mult_8_video_eng": 36.1,
        "Mult_1_eng": 40,
        "Mult_2_eng": 43.5,
        "Mult_3_eng": 93.7,
        "Mult_4_eng": 39.9,
        "Mult_5_eng": 51.4,
        "Mult_6_eng": 47.6,
        "Mult_7_eng": 95.3,
        "Mult_8_video_rus": 9.6,
        "Mult_Mishki_2_video_rus": 14.1,
        "Mult_1": 34.9,
        "Mult_2": 69.9,
        "Mult_3": 46.4,
        "Mult_4": 37.9,
        "Mult_5": 54.1,
        "Mult_6": 97.7,
        "Mult_7": 109.1
    }
    not_sound_cpi = set()

    def get_cpi(s):
        if s in cpi.keys():
            return cpi[s]
        else:
            not_sound_cpi.add(s)
            return 1

    for os_str in os_list:
        os_obj = OS.get_os(os_str)
        # БАЗА ДАННЫХ
        Report.set_app_data(parser=parser, event_class=Event, user_class=user_class, os=os_str, app=app,
                            user_status_check=False)
        Report.set_installs_data(additional_parameters=None,
                                 period_start=period_start,
                                 period_end=period_end,
                                 min_version=min_version,
                                 max_version=max_version,
                                 countries_list=countries_list)

        Report.set_events_data(additional_parameters=None,
                               period_start=period_start,
                               period_end=None,
                               min_version=None,
                               max_version=None,
                               countries_list=[],
                               events_list=events_list)

        # параметры
        parameters = ["Installs", "Paying"]
        accumulating_parameters = [str(i) + "d" for i in range(0, days_since_install + 1)]
        parameters += accumulating_parameters
        sources = {}
        transactions = {}
        dau = {}
        user_dau = {}
        if not period_end:
            period_end = datetime.now().date()
        for dt in rrule(DAILY, dtstart=period_start, until=(period_end + timedelta(days=days_since_install))):
            dau[dt.date()] = {"Users": 0, "Revenue": 0}
            user_dau[dt.date()] = None

        # ПОЛЬЗОВАТЕЛЬСКИЕ ПАРАМЕТРЫ
        user_accumulative = dict.fromkeys(accumulating_parameters, 0)
        user_transactions = []
        ltv = 0
        previous_day_in_game = 0

        # Запись пользовательских данных в общие
        def flush_user_data():
            # if len(user_transactions)>0:
            #    print(Report.previous_user.user_id, Report.previous_user.install_date)
            #    print(user_dau)
            #   print(user_transactions)
            for d1 in range(day_in_game, days_since_install + 1):
                user_accumulative[str(d1) + "d"] = ltv
            if publisher not in sources.keys():
                sources[publisher] = {}
                transactions[publisher] = {}
                dau[publisher] = {}
            if source not in sources[publisher].keys():
                sources[publisher][source] = {}
                transactions[publisher][source] = []
                dau[publisher][source] = {}
                for date_t in rrule(DAILY, dtstart=period_start,
                                    until=(period_end + timedelta(days=days_since_install))):
                    dau[publisher][source][date_t.date()] = {"Users": 0, "Revenue": 0}
            if Report.previous_user.install_date not in sources[publisher][source].keys():
                sources[publisher][source][Report.previous_user.install_date] = dict.fromkeys(parameters, 0)
            for param in accumulating_parameters:
                sources[publisher][source][Report.previous_user.install_date][param] += user_accumulative[param]
            if len(user_transactions) > 0:
                sources[publisher][source][Report.previous_user.install_date]["Paying"] += 1
                transactions[publisher][source].append(user_transactions)
            for user_d in user_dau.keys():
                if user_dau[user_d] is not None:
                    dau[publisher][source][user_d]["Users"] += 1
                    dau[publisher][source][user_d]["Revenue"] += user_dau[user_d]
                    # if len(user_transactions) > 0:
                    #    print(dau[publisher][source])

        # Цикл обработки данных
        while Report.get_next_event():

            # Переносим пользовательские данные в общие и обнуляем пользователськие парамтеры
            if Report.is_new_user():
                flush_user_data()
                user_accumulative = dict.fromkeys(accumulating_parameters, 0)
                user_transactions = []
                for dt in rrule(DAILY, dtstart=period_start, until=period_end + timedelta(days=days_since_install)):
                    user_dau[dt.date()] = None
                ltv = 0
                previous_day_in_game = 0

            publisher = Report.current_user.publisher
            source = Report.current_user.source

            # Определяем день после установки
            day_in_game = Report.get_time_since_install(measure="day")

            # Если день изменился, то заполняем дни от предыдущего до нынешнего предыдущим LTV
            if day_in_game > previous_day_in_game:
                for day in range(previous_day_in_game, day_in_game):
                    user_accumulative[str(day) + "d"] = ltv
            if Report.current_event.datetime.date() in user_dau.keys() and user_dau[
                    Report.current_event.datetime.date()] is None:
                user_dau[Report.current_event.datetime.date()] = 0

            # Обновляем LTV новой покупкой и Revenue в DAU
            if issubclass(type(Report.current_event),PurchaseEvent):
                ltv += Report.current_event.price
                if Report.current_event.datetime.date() in user_dau.keys():
                    user_dau[Report.current_event.datetime.date()] += Report.current_event.price
                # Сохраняем транзации для расчета метрик
                user_transactions.append(Report.current_event.price)
                # Переходим на следующий день
            previous_day_in_game = day_in_game

        flush_user_data()

        # РАСЧЕТЫ И ВЫВОД
        # По каждому источнику
        for publisher in sources.keys():
            # Общее ARPU и установки по паблишеру
            overall_publisher_arpu = [0] * (days_since_install + 1)
            overall_publisher_installs = 0

            # Запись в отдельную таблицу
            writer = pd.ExcelWriter(
                folder_dest+ OS.get_os_string(os_obj) + " " + publisher + " Cummulative ROI.xlsx")
            # По каждоый трекинговой ссылке
            for source in sources[publisher].keys():
                overall_source_arppu = dict.fromkeys(accumulating_parameters, 0)
                overall_source_arpu = dict.fromkeys(parameters, 0)
                overall_source_arpdau = dict.fromkeys(parameters, 0)
                # Таблица с данными для записи в файл
                df = pd.DataFrame(index=[],
                                  columns=["Install date", "App", "Installs", "Paying", "CPI",
                                           "Day"] + accumulating_parameters)

                # Заполняем пропущенные дни, в которых не было покупок
                for install in Report.get_installs():
                    if (install["publisher_name"] == publisher or
                            (install["publisher_name"] == "" and publisher == "Organic")) and \
                            (install["tracker_name"] == source
                             or (install["tracker_name"] == "unknown" and source == OS.get_source(os_obj))) and install[
                        "install_datetime"].date() not in sources[publisher][source].keys() and \
                            not {install[OS.get_aid(Report.os)],
                                 install[OS.get_id(Report.os)]} & Report.user_skip_list:
                        # print(install["install_datetime"].date())
                        # print()
                        sources[publisher][source][install["install_datetime"].date()] = dict.fromkeys(parameters, 0)

                # Цикл по каждому дню
                for install_date in sources[publisher][source].keys():
                    installs_number = 0
                    app_version = None
                    # Считаем количество установок в этот день и версию устанавливаемого приложения
                    for install in Report.get_installs():
                        if (install["publisher_name"] == publisher or
                                (install["publisher_name"] == "" and publisher == "Organic")) and \
                                (install["tracker_name"] == source or (
                                                install["tracker_name"] == "unknown" and
                                                source == OS.get_source(os_obj))) and install[
                            "install_datetime"].date() == install_date and \
                                not {install[OS.get_aid(Report.os)],
                                     install[OS.get_id(Report.os)]} & Report.user_skip_list:
                            installs_number += 1
                            app_version = install["app_version_name"]
                    sources[publisher][source][install_date]["Installs"] = installs_number
                    # print(publisher, source, install_date, sources[publisher][source][install_date])
                    for i in range(0, days_since_install + 1):
                        overall_source_arpu[str(i) + "d"] += sources[publisher][source][install_date][str(i) + "d"]
                        overall_source_arppu[str(i) + "d"] += sources[publisher][source][install_date][str(i) + "d"]
                        overall_source_arpdau[str(i) + "d"] += sources[publisher][source][install_date][str(i) + "d"]
                        if sources[publisher][source][install_date]["Installs"] > 0:
                            sources[publisher][source][install_date][str(i) + "d"] = round(
                                sources[publisher][source][install_date][str(i) + "d"] /
                                sources[publisher][source][install_date]["Installs"], 2)
                        else:
                            sources[publisher][source][install_date][str(i) + "d"] = 0
                    # print(publisher, source,sources[publisher][source][install_date]["Installs"])

                    # Добавляем в таблицу строку с ARPU и данными об установках за этот день
                    df = df.append({
                        "Install date": str(install_date),
                        "App": app_version,
                        "CPI": get_cpi(source),
                        "Day": "ARPU",
                        **sources[publisher][source][install_date]
                    }, ignore_index=True)

                    # Если речь не об органическом трафике, то
                    if get_cpi(source) != 0:
                        # Считаем ROI как ((Revenue-I)*100%)/I по каждому дню
                        roi = dict.fromkeys(parameters, 0)
                        for i in range(0, days_since_install + 1):
                            if get_cpi(source) == 0 or sources[publisher][source][install_date]["Installs"] == 0:
                                roi[str(i) + "d"] = 100
                            else:
                                roi[str(i) + "d"] = str(round(
                                    ((sources[publisher][source][install_date][str(i) + "d"] - get_cpi(
                                        source)) * 100) / get_cpi(source), 0)) + "%"

                        # добавляем строку с ROI
                        df = df.append({
                            "Install date": str(install_date),
                            "App": " ",
                            "CPI": " ",
                            "Day": "ROI",
                            **roi
                        }, ignore_index=True)

                # Сортируем таблицу по дню установки и типу строки (в поле DAY стоит тип данных - ARPU или ROI)
                df.sort_values(by=["Install date", "Day"], inplace=True)

                # Считаем общее ARPU по источнику (и добавляем в общее ARPU паблишера)
                overall_source_roi = dict.fromkeys(parameters, 0)
                for install_date in sources[publisher][source].keys():
                    for i in range(0, days_since_install + 1):
                        overall_publisher_arpu[i] += sources[publisher][source][install_date][str(i) + "d"]
                    overall_source_arpu["Installs"] += sources[publisher][source][install_date]["Installs"]
                    overall_publisher_installs += sources[publisher][source][install_date]["Installs"]
                    overall_source_arpu["Paying"] += sources[publisher][source][install_date]["Paying"]
                # Paying
                if overall_source_arpu["Installs"] > 0:
                    paying_percent = round(overall_source_arpu["Paying"] * 100 / overall_source_arpu["Installs"], 1)
                else:
                    paying_percent = 0
                overall_source_arpu["Paying"] = str(overall_source_arpu["Paying"]) + " (" + str(paying_percent) + "%)"
                # overall arpu
                for i in range(0, days_since_install + 1):
                    if overall_source_arpu["Installs"] > 0:
                        overall_source_arpu[str(i) + "d"] = round(
                            overall_source_arpu[str(i) + "d"] / overall_source_arpu["Installs"], 2)
                    else:
                        overall_source_arpu[str(i) + "d"] = 0
                # Добавляем строку с общим ARPU по источнику
                df = df.append({
                    "Install date": "OVERALL",
                    "App": " ",
                    "CPI": get_cpi(source),
                    "Day": "ARPU",
                    **overall_source_arpu
                }, ignore_index=True)

                # Если это не органческий трафик, рассчитываем общий ROI ((Revenue-I)*100%)/I
                if get_cpi(source) != 0:
                    # Подготавливаем данные для графика
                    source_roi_y = []
                    source_arpu_y = []
                    for i in range(0, days_since_install + 1):
                        if overall_source_arpu["Installs"] != 0:
                            overall_source_roi[str(i) + "d"] = round(
                                (
                                    (overall_source_arpu[str(i) + "d"] - get_cpi(
                                        source)) * 100) /
                                (get_cpi(source)), 0)
                        else:
                            overall_source_roi[str(i) + "d"] = 100
                        source_arpu_y.append(overall_source_arpu[str(i) + "d"])
                        source_roi_y.append(overall_source_roi[str(i) + "d"])
                        overall_source_roi[str(i) + "d"] = str(overall_source_roi[str(i) + "d"]) + "%"

                    # Для красоты
                    for ind in [i for i in df.index.values if i % 2 == 1]:
                        df.at[ind, "Install date"] = ""
                    df.at[ind, "Installs"] = None
                    df.at[ind, "Paying"] = ""
                    overall_source_roi["Installs"] = ""
                    overall_source_roi["Paying"] = ""

                    # Добавляем строку с общим ROI в таблицу
                    df = df.append({
                        "Install date": " ",
                        "App": " ",
                        "CPI": " ",
                        "Day": "ROI",
                        **overall_source_roi
                    }, ignore_index=True)


                    # Рисуем графики
                    if not os.path.exists(folder_dest+publisher+"/"+OS.get_os_string(os_obj)+"/"):
                        os.makedirs(folder_dest+publisher+"/"+OS.get_os_string(os_obj)+"/")
                    Report.draw_plot(range(0, days_since_install + 1),
                                     { source + " ARPU": source_arpu_y},
                                     show=False,
                                     folder=folder_dest+publisher+"/"+OS.get_os_string(os_obj)+"/")
                    Report.draw_plot(range(0, days_since_install + 1),
                                     {source + " ROI": source_roi_y,
                                      " ": [100] * (days_since_install + 1)}, show=False,
                                     folder=folder_dest+publisher+"/"+OS.get_os_string(os_obj)+"/")

                # Расчет доп метрик
                # ARPPU
                if len(transactions[publisher][source]) > 0:
                    for i in range(0, days_since_install + 1):
                        overall_source_arppu[str(i) + "d"] = round(
                            overall_source_arppu[str(i) + "d"] / len(transactions[publisher][source]), 2)
                df = df.append({
                    "Install date": "OVERALL",
                    "App": " ",
                    "CPI": " ",
                    "Day": "ARPPU",
                    **overall_source_arppu
                }, ignore_index=True)

                # Средний чек и Транзакции на платящего пользователя
                avg_check = 0
                avg_trans = 0
                if len(transactions[publisher][source]) > 0:
                    trans = []
                    trans_per_paying = []
                    for transaction in transactions[publisher][source]:
                        trans_per_paying.append(len(transaction))
                        trans += transaction
                    if len(trans) > 0:
                        avg_check = round(sum(trans) / len(trans), 1)
                    avg_trans = round(sum(trans_per_paying) / len(trans_per_paying), 1)
                df = df.append({
                    "Install date": "AVG CHECK",
                    "App": avg_check,
                }, ignore_index=True)
                df = df.append({
                    "Install date": "AVG TRANS",
                    "App": avg_trans,
                }, ignore_index=True)

                # ARPDAU
                arpdau = []
                avg_dau = []
                for d in dau[publisher][source].keys():
                    if dau[publisher][source][d]["Users"] > 0:
                        arpdau.append(dau[publisher][source][d]["Revenue"] / dau[publisher][source][d]["Users"])
                        avg_dau.append(dau[publisher][source][d]["Users"])
                avg_dau = round(sum(avg_dau) / len(avg_dau), 0)
                arpdau = round(sum(arpdau) / len(arpdau), 2) if len(arpdau) > 0 else 0
                df = df.append({
                    "Install date": "DAU",
                    "App": avg_dau,
                }, ignore_index=True)
                df = df.append({
                    "Install date": "ARPDAU",
                    "App": arpdau,
                }, ignore_index=True)

                df.fillna("", inplace=True)
                # Вывод и печать в Excel
                # print(df.to_string(index=False))
                df.to_excel(excel_writer=writer, sheet_name=source, index=False)
                writer.save()

            # График общего ARPU по паблишеру
            plt.figure(figsize=(16, 8))
            if overall_publisher_installs > 0:
                y_real_arpu = list(
                    map(truediv, overall_publisher_arpu,
                        [overall_publisher_installs] * len(overall_publisher_arpu)))
            else:
                y_real_arpu = [0] * 20
                print(publisher, "no installs")
            approximator = log_approximation(range(len(y_real_arpu)), y_real_arpu)
            y = approximator(np.arange(0, 60, 1))
            plt.plot(range(len(y_real_arpu)), y_real_arpu, '*', color="green", label="known")
            plt.plot(np.arange(0, 60, 1), y, '--', color="red", label="approximate")
            plt.legend()
            title = OS.get_os_string(os_obj) + " Прогноз ARPU по всем источникам " + publisher
            plt.title(title)
            plt.savefig(folder_dest+title + ".png", bbox_inches='tight')

            plt.close()
        print("Not found CPI sources", not_sound_cpi)
