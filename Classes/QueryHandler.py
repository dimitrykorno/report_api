from datetime import datetime, timedelta

from report_api.OS import OS
from report_api.Data.Data import get_env


class QueryHandler():
    """
    Обработчик, составляющий запросы к базе
    """

    def __init__(self, database="events",
                 os=OS.ios,
                 app="sop",
                 additional_parameters=[],
                 period_start=None,
                 period_end=None,
                 min_version=None,
                 max_version=None,
                 countries_list=[],
                 events_list=[],
                 users_list=[],
                 order=True):

        self.select_row = ""
        self.from_row = ""
        self.where_row = ""
        self.users_row = ""
        self.order_row = ""

        self.user_id1 = ""
        self.user_id2 = ""

        self.os = os
        self.database = database
        self.app = app
        self.result = ""

        self.user_id1 = OS.get_aid(os)
        self.user_id2 = OS.get_id(os)
        # if database == "installs" and (period_start, period_end, min_version, max_version) == (None, None, None, None):
        #    return
        self.add_select_parameters(additional_parameters)
        self.add_from_database(os=os, app=app, database=database)
        self.add_where_parameters(period_start=period_start,
                                  period_end=period_end,
                                  min_app_version=min_version,
                                  max_app_version=max_version,
                                  countries_list=countries_list,
                                  events_list=events_list)

        self.result = "".join([self.select_row, self.from_row, self.where_row])

        if database == "events":
            self.add_users_list(users_list)
        if order:
            self.add_order_parameters()

    def add_select_parameters(self, parameters=[]):
        """
        Добавление дополнительных параметров (столбцов) в запрос
        :param parameters: параметры
        :return:
        """
        # разные обязательные сталбцы для установок и событий
        if self.database is "installs":
            mandatory_parameters = {self.user_id1,
                                    self.user_id2,
                                    "publisher_name",
                                    "install_datetime",
                                    "tracker_name",
                                    "app_version_name",
                                    "country_iso_code"}
        else:
            mandatory_parameters = {self.user_id1,
                                    self.user_id2,
                                    "event_name",
                                    "event_json",
                                    "event_datetime",
                                    "app_version_name"
                                    }
        if parameters:
            parameters = (
                set(parameters) | mandatory_parameters)
        else:
            parameters = mandatory_parameters
        self.select_row = "select " + ",".join(parameters)
        # /*+ NO_RANGE_OPTIMIZATION({} {}) */".format("events_android",OS.get_id(self.os))

    def add_from_database(self, os=OS.ios, app="sop", database="events"):
        """
        Добавление строки, из какой базы получать данные
        :param os: ос
        :param app: приложение
        :param database: база данных (событий или установки)
        :return:
        """
        self.from_row = QueryHandler.get_db_name(os, app, database)

    @staticmethod
    def get_db_name(os=OS.ios, app="sop", database="events"):
        if get_env() == "laptop":
            return """
            from {}_events.{}_{}
            """.format(app, database, OS.get_os_string(os))
        elif get_env() == "server":
            if database == "events":
                return """
                from analytics.events
                """
            elif database == "installs":
                return """
                from analytics.installations
                """

    def add_where_parameters(self,
                             period_start=None,
                             period_end=None,
                             min_app_version=None,
                             max_app_version=None,
                             countries_list=[],
                             events_list=[]):
        """
        Добавление параметров выборки
        :param period_start: начало периода
        :param period_end: конец периода
        :param min_app_version: мин версия приложения
        :param max_app_version: макс версия приложения
        :param countries_list: список стран
        :param events_list: список событий [event_name: event_json] (возможны списки и множества событий и Json)
        :return:
        """

        self.where_row = QueryHandler.where_line(period_start,
                                                 period_end,
                                                 min_app_version,
                                                 max_app_version,
                                                 countries_list,
                                                 events_list,
                                                 self.os,
                                                 self.database)

    @staticmethod
    def where_line(period_start=None,
                   period_end=None,
                   min_app_version=None,
                   max_app_version=None,
                   countries_list=[],
                   events_list=[],
                   os_str=OS.ios,
                   database="events"):
        user_id1 = OS.get_aid(os_str)
        user_id2 = OS.get_id(os_str)
        where_string = ""
        if period_start and isinstance(period_start, str):
            try:
                period_start = datetime.strptime(period_start, "%Y-%m-%d").date()
            except Exception:
                print("Period start error", period_start)
                period_start = None
        if period_end and isinstance(period_end, str):
            try:
                period_end = datetime.strptime(period_end, "%Y-%m-%d").date()

            except Exception:
                print("Period end error", period_end)
                period_end = None
        if period_end:
            period_end += timedelta(days=1)

        where_string += """
        where (
        ( {} <> "" or {} <> "" )
        """.format(user_id1, user_id2)

        min_app_version = """
        and 
        app_version_name >= {}""".format(str(min_app_version)) if min_app_version else ""
        max_app_version = """
        and 
        app_version_name <= {}
        """.format(str(max_app_version)) if max_app_version else ""

        datetime_period = ""
        datetime_name = "install_datetime " if "install" in database else "event_datetime"
        if period_end and period_start:
            datetime_period = """
        and
        {} between '{}' and '{}'
        """.format(datetime_name, str(period_start), str(period_end))
        elif period_start or period_end:
            operation = ">=" if period_start else "<="
            period = period_start if period_start else period_end
            datetime_period = """
        and
        {} {} '{}'
        """.format(datetime_name, operation, period)

        countries = ""
        if countries_list:
            countries = """
            and 
            country_iso_code in ('{}')
            """.format("','".join(map(lambda x: x.upper(), countries_list)))

        for row in (min_app_version, max_app_version, datetime_period, countries):
            if row != "":
                where_string += row + """
                """

        if events_list:
            where_string += QueryHandler.add_events_line(events_list) + """
                """
        return where_string + ")"
        # print(self.where_row)

    @staticmethod
    def add_events_line(events_list):
        # добавление списка событий event_name  и json
        events = ""
        for index, event in enumerate(events_list):
            if events == "":
                events = """
                and 
                (
                        """

            event_names_list = []
            if event[0] != "":
                # приводим к виду списка, чтобы итерироваться (даже если 1)
                if not isinstance(event[0], list):
                    event_names_list.append(event[0])
                else:
                    event_names_list = event[0]
                events += QueryHandler.form_line(event_names_list, "event_name")

            if len(event) > 1:
                if event[0] != "":
                    events += " and "
                json_list = []
                # приводим к виду списка, чтобы итерироваться (даже если 1)
                if not isinstance(event[1], list):
                    json_list.append(event[1])
                else:
                    json_list = event[1]
                events += QueryHandler.form_line(json_list, "event_json")

            if index != len(events_list) - 1:
                events += """
                        or
                        """
        if events != "":
            events += """
                )"""
        return events

    # set - in (_,_,_ )
    # list _ or _or _ or _
    # tuple _ and _ and _ and _
    @staticmethod
    def form_line(event_name, column_name):
        line = "( "
        if isinstance(event_name, str):
            event_name = [event_name]
        if isinstance(event_name, set):
            positive = [e for e in event_name if not e.startswith("not ")]
            negative = [e[4:] for e in event_name if e.startswith("not ")]
            if positive:
                line += column_name + " in ('" + "','".join(positive) + "')"
            if positive and negative:
                line += ' and '
            if negative:
                line += column_name + " in ('" + "','".join(negative) + "')"
        elif isinstance(event_name, list) or isinstance(event_name, tuple):
            positive = [e for e in event_name if not e.startswith("not ")]
            negative = [e[4:] for e in event_name if e.startswith("not ")]
            if positive:
                for index, e in enumerate(positive):
                    if "%" in e:
                        join_element = " like '"
                    else:
                        join_element = " = '"
                    line += column_name + join_element + e + "'"
                    if index != len(positive) - 1:
                        if isinstance(event_name, list):
                            line += " or "
                        elif isinstance(event_name, tuple):
                            line += " and "

            if positive and negative:
                line += ') and ('
            if negative:
                for index, e in enumerate(negative):
                    if "%" in e:
                        join_element = " not like '"
                    else:
                        join_element = " != '"
                    line += column_name + join_element + e + "'"
                    if index != len(negative) - 1:
                        if isinstance(event_name, list):
                            line += " or "
                        elif isinstance(event_name, tuple):
                            line += " and "

        line += ")"
        return line

    def add_users_list(self, users_list):
        """
        Добавление списка установок
        :param users_list:
        :return:
        """
        if users_list:
            # user_id1_list = [install[self.user_id1] for install in users_list]
            user_id2_list = [install[self.user_id2] for install in users_list]

            #     self.users_row = """
            # and
            # (
            #    {0} in ("{1}")
            #     and
            #    {2} in ("{3}")
            # )
            # """.format(self.user_id1,
            #            '","'.join(map(str, user_id1_list)),
            #            self.user_id2,
            #            '","'.join(map(str, user_id2_list))
            #            )
            self.users_row = """
                    and
                    (
                       {0} in ("{1}")
                    )
                    """.format(
                self.user_id2,
                '","'.join(map(str, user_id2_list))
            )

    def add_order_parameters(self):
        datetime_name = "install_datetime " if "install" in self.database else "event_datetime"
        self.order_row = """
        order by {},{},{}
        """.format(self.user_id1, self.user_id2, datetime_name)

    def get_query(self):
        # print("".join([self.result, self.users_row, self.order_row]))

        query = "".join([self.result, self.users_row, self.order_row])
        file = open("sql " + OS.get_os_string(self.os) + " " + self.database + ".txt", "w")
        file.write(query)
        file.close()
        return query
