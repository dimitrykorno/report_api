from Classes.Events import *
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
        # Report.set_installs_data(additional_parameters=None,
        #                          period_start=period_start,
        #                          period_end=period_end,
        #                          min_version=min_version,
        #                          max_version=max_version,
        #                          countries_list=countries_list)

        Report.set_events_data(additional_parameters=["country_iso_code"],
                               period_start=period_start,
                               period_end=period_end,
                               min_version=min_version,
                               max_version=max_version,
                               countries_list=countries_list,
                               events_list=events_list)

        # общие продажи по дням недели
        def get_week(my_date):
            '''
            Разница между датой и началом периода в неделях
            :param my_date: дата, недели до которой нужно посчитать
            :return:
            '''
            weeks = rrule.rrule(rrule.WEEKLY, dtstart=period_start, until=my_date)
            return weeks.count()

        def get_week_after_first_purchase(my_date, first_purchase_date):
            '''
            Разница между датой и началом периода в неделях
            :param my_date: дата, недели до которой нужно посчитать
            :return:
            '''
            weeks = rrule.rrule(rrule.WEEKLY, dtstart=first_purchase_date, until=my_date).count()
            if weeks < after_release_months_graphs * 4:
                return weeks
            else:
                return

        # X_days_sales = []
        # X_weeks_sales = [0]
        # X_week_labels = []
        # if after_release_months_graphs:
        #      X_weeks_after_first_purchase = list(range(after_release_months_graphs * 4))
        # previous_week = 1
        # week_added = False
        #
        # for d in daterange(period_start, period_end):
        #     X_days_sales.append(str(d.day) + "." + str(d.month))
        #     week = get_week(d)
        #     month = d.strftime("%m/%y")
        #     if previous_week and week != previous_week:
        #         week_added = False
        #     # if previous_month and month != previous_month:
        #     #    week_label = 1
        #     # previous_month = month
        #     previous_week = week
        #     week_label = week_of_month(d)
        #
        #     week_month = str(week_label) + "." + str(month)
        #     if not week_added and week_month not in X_week_labels:
        #         X_week_labels.append(week_month)
        #         week_added = True
        #     if week not in X_weeks_sales:
        #         X_weeks_sales.append(week)
        # Y_past_sales = [0] * len(X_days_sales)
        # Y_past_money = [0] * len(X_days_sales)

        # формируем таблицу отчета
        parameters = ["Category 1", "Category 2", "Category 3", "First purchase", "Price", "Revenue", "Sales"]
        short_parameters = ["Category 1", "Category 2", "Category 3" "First purchase", "Price"]
        countries = {}

        months_list = set()
        unique_inapps = set()
        # brands_list = get_brands_list()
        # if after_release_months_graphs:
        #     Y_inapps_this_period = {}
        #     Y_inapps_after_start = {}
        #     for X, Y in zip([X_weeks_sales, X_weeks_after_first_purchase],
        #                     [Y_inapps_this_period, Y_inapps_after_start]):
        #         for brand in brands_list:
        #             for pack in (True, False):
        #                 for lang in ("rus", "eng"):
        #                     in_app_list = get_in_apps_list(brand=brand, pack=pack, language=lang)
        #                     if in_app_list:
        #                         Y[(brand, pack, lang)] = dict.fromkeys(in_app_list)
        #                         for in_app in in_app_list:
        #                             Y[(brand, pack, lang)][in_app] = [0] * len(X)

        while Report.get_next_event():

            country = Report.current_user.country
            if country not in countries.keys():
                countries[country] = {}
            in_app = Report.current_event.purchase
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

            # pack = is_pack(in_app)
            # lang = get_lang(in_app)




            # if after_release_months_graphs:
            #     # Графики
            #     # график спроса на инаппы по неделям
            #     Y_inapps_this_period[(brand, pack, lang)][in_app][get_week(purchase_date)] += 1
            #     # график спроса на инаппы по неделям с момента выхода
            #     week = get_week_after_first_purchase(purchase_date, df.at[in_app, "First purchase"])
            #     if week:
            #         Y_inapps_after_start[(brand, pack, lang)][in_app][week] += 1
            #     # график общего спроса и дохода по дням
            #     Y_past_sales[X_days_sales.index(str(purchase_date.day) + "." + str(purchase_date.month))] += 1
            #     Y_past_money[X_days_sales.index(
            #         str(purchase_date.day) + "." + str(purchase_date.month))] += Report.current_event.price

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

        # if after_release_months_graphs:
        #     for X, Y, labels in zip([X_weeks_sales, X_weeks_after_first_purchase],
        #                             [Y_inapps_this_period, Y_inapps_after_start],
        #                             [X_week_labels, None]):
        #         folder = "Result_AfterRelease/" if Y == Y_inapps_after_start else "Result_Sales/"
        #
        #         for brand in brands_list:
        #             for pack in (True, False):
        #                 for lang in ("rus", "eng"):
        #                     if (brand, pack, lang) in Y.keys():
        #                         p = "pack" if pack else "solo"
        #                         title = str(OS.get_os_string(Report.os)) + "." + lang + "." + brand + "." + p
        #                         xticks_move = 0 if labels else 1
        #                         draw_plot(X, Y[(brand, pack, lang)], xtick_steps=1, xticks_move=xticks_move,
        #                                   x_ticks_labels=labels, title=title, folder=folder)
