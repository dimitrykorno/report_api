from datetime import datetime, timedelta
from operator import truediv
import os
from time import sleep
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


    for os_str in os_list:
        os_obj = OS.get_os(os_str)
        # БАЗА ДАННЫХ
        Report.set_app_data(parser=parser, event_class=Event, user_class=user_class, os=os_str, app=app,
                            user_status_check=False)
        json_line = ""
        if events_list:
            for tup in events_list:
                json_line+="and event_name"


        sql="""SELECT date(event_datetime) as day,COUNT(ios_ifa) as users
            from {0}_events.events_{1}
            where event_datetime between '{2}' and '{3}' {}
            group by date(event_datetime)
            """.format(app,os_str.lower(),period_start,period_end,)