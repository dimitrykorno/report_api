from report_api.Classes.Events import *
from report_api.Report import Report
from datetime import datetime
import pandas as pd
from dateutil import rrule

def new_report(shop=None,
               parser=None,
               user_class=None,
               app=None,
               folder_dest=None,
               events_list=[],
               os_list=["iOS", "Android"],
               after_release_months_graphs=None,
               period_start="2018-06-19",
               period_end=None,
               min_version=None,
               max_version=None,
               countries_list=[]
               ):
    if isinstance(period_start, str):
        period_start = datetime.strptime(period_start, "%Y-%m-%d").date()
    if isinstance(period_end, str):
        period_end = datetime.strptime(period_end, "%Y-%m-%d").date()

    for os_str in os_list:

        Report.set_app_data(parser=parser, event_class=Event, user_class=user_class, os=os_str, app=app,
                            user_status_check=False)

        Report.set_events_data(additional_parameters=["country_iso_code"],
                               period_start=period_start,
                               period_end=period_end,
                               min_version=min_version,
                               max_version=max_version,
                               countries_list=countries_list,
                               events_list=events_list,
                               order=False)

        # общие продажи по дням недели
        def get_week(my_date):
            '''
            Разница между датой и началом периода в неделях
            :param my_date: дата, недели до которой нужно посчитать
            :return:
            '''
            weeks = rrule.rrule(rrule.WEEKLY, dtstart=period_start, until=my_date)
            return weeks.count()



        # формируем таблицу отчета
        parameters = ["Category 1", "Category 2", "Category 3", "First purchase", "Price", "Revenue", "Sales"]
        short_parameters = ["Category 1", "Category 2", "Category 3" "First purchase", "Price"]
        countries = {}

        months_list = set()
        unique_inapps = set()

        while Report.get_next_event():

            country = Report.current_user.country
            if country not in countries.keys():
                countries[country] = {}
            in_app = Report.current_event.obj_name
            unique_inapps.add(in_app)
            if in_app not in countries[country].keys():
                countries[country][in_app] = dict.fromkeys(short_parameters, 0)
                countries[country][in_app]["First purchase"] = ""
                countries[country][in_app]["Price"] = Report.current_event.price
                countries[country][in_app]["Category 1"], countries[country][in_app]["Category 2"], \
                countries[country][in_app]["Category 3"], = shop.get_categories(in_app)
            purchase_date = Report.current_event.datetime.date()
            if not countries[country][in_app]["First purchase"] or \
                            purchase_date < countries[country][in_app]["First purchase"]:
                countries[country][in_app]["First purchase"] = purchase_date
            month = Report.current_event.datetime.strftime("%m/%y")
            months_list.add(month)
            if month not in countries[country][in_app].keys():
                countries[country][in_app][month] = 0
            countries[country][in_app][month] += 1

        months_list = sorted(months_list)
        writer = pd.ExcelWriter(folder_dest + "/Sales" + os_str + ".xlsx")
        for country in countries.keys():
            df = pd.DataFrame(index=unique_inapps, columns=parameters + months_list)
            for in_app in unique_inapps:
                if in_app not in countries[country].keys():
                    countries[country][in_app] = dict.fromkeys(short_parameters, 0)
                    countries[country][in_app]["First purchase"] = ""
                    countries[country][in_app]["Price"] = Report.current_event.price
                    countries[country][in_app]["Category 1"], countries[country][in_app]["Category 2"], \
                    countries[country][in_app]["Category 3"], = shop.get_categories(in_app)
                for param in parameters + months_list:
                    if param in countries[country][in_app].keys():
                        df.at[in_app, param] = countries[country][in_app][param]
                    else:
                        df.at[in_app, param] = 0
            df["First purchase"].fillna(0, inplace=True)
            df["First purchase"].replace(0, "", inplace=True)
            df["Category 1"].fillna(0, inplace=True)
            df["Category 1"].replace(0, "", inplace=True)
            df["Category 2"].fillna(0, inplace=True)
            df["Category 2"].replace(0, "", inplace=True)
            df["Sales"] = df[months_list].sum(axis=1)
            df["Revenue"] = df["Sales"] * df["Price"]
            df.sort_values(by=["Category 1", "Category 2", "Category 3", "Sales"], ascending=False, inplace=True)
            df.to_excel(writer, sheet_name=country)
        writer.save()
