from datetime import datetime, timedelta

from report_api.OS import OS


class QueryHandler():
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
                 users_list=[]):

        self.select_row = ""
        self.from_row = ""
        self.where_row = ""
        self.users_row = ""
        self.order_row = ""

        self.user_id1 = ""
        self.user_id2 = ""

        self.database = database
        self.result = ""

        self.user_id1 = OS.get_aid(os)
        self.user_id2 = OS.get_id(os)
        #if database == "installs" and (period_start, period_end, min_version, max_version) == (None, None, None, None):
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

        self.add_users_list(users_list)
        self.add_order_parameters()

    def add_select_parameters(self, parameters=[]):
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

    def add_from_database(self, os=OS.ios, app="sop", database="events"):
        self.from_row = """
        from {}_events.{}_{}
        """.format(app, database, OS.get_os_string(os))

    def add_where_parameters(self,
                             period_start=None,
                             period_end=None,
                             min_app_version=None,
                             max_app_version=None,
                             countries_list=[],
                             events_list=[]):

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

        self.where_row += """
        where
        ( {} <> "" or {} <> "" )
        """.format(self.user_id1, self.user_id2)

        min_app_version = """
        and 
        app_version_name >= {}""".format(str(min_app_version)) if min_app_version else ""
        max_app_version = """
        and 
        app_version_name <= {}
        """.format(str(max_app_version)) if max_app_version else ""

        datetime_period = ""
        datetime_name = "install_datetime " if "install" in self.database else "event_datetime"
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
            country_iso_code in ({})
            """.format(','.join(countries_list))

        events = ""
        for event in events_list:
            if events == "":
                events = """
        and 
        (
        """

            events += "("
            event_names_list = []
            if not isinstance(event[0], list):
                event_names_list.append(event[0])
            else:
                event_names_list = event[0]
            for event_name in event_names_list:
                if isinstance(event_name, set):
                    events += 'event_name in ("' + '","'.join(event_name) + '")'
                elif "%" in event_name:
                    if event_name[:3] == "not":
                        events += "event_name not like '" + event_name[4:] + "'"
                    else:
                        events += "event_name like '" + event_name + "'"
                else:
                    events += "event_name = '" + event_name + "'"
                if not event_name is event_names_list[-1]:
                    if event_name[:3] == "not":
                        events += " and "
                    else:
                        events += " or "
            events += ")"

            if len(event) > 1:
                events += "and ("
                json_list = []
                if not isinstance(event[1], list):
                    json_list.append(event[1])
                else:
                    json_list = event[1]
                for event_json in json_list:
                    if isinstance(event_json, set):
                        event_json += "event_json in ('" + "','".join(event_json) + "')"
                    elif "%" in event_json:
                        if event_json[:3] == "not":
                            events += "event_json not like '" + event_json[4:] + "'"
                        else:
                            events += "event_json like '" + event_json + "'"
                    else:
                        events += "event_json = '" + event_json + "'"
                    if not event_json is json_list[-1]:
                        events += " or "
                events += ")"

            if not event is events_list[-1]:
                events += """
        or
        """
            else:
                events += """
        )"""

        for row in (min_app_version, max_app_version, datetime_period, countries, events):
            if row != "":
                self.where_row += row + """
                """

                # print(self.where_row)

    def add_users_list(self, users_list):
        if users_list:
            user_id1_list = [install[self.user_id1] for install in users_list]
            user_id2_list = [install[self.user_id2] for install in users_list]

            self.users_row = """
        and
        (
           {0} in ("{1}")
            and
           {2} in ("{3}")
        )
        """.format(self.user_id1, '","'.join(map(str, user_id1_list)), self.user_id2,
                   '","'.join(map(str, user_id2_list)))

    def add_order_parameters(self):
        datetime_name = "install_datetime " if "install" in self.database else "event_datetime"
        self.order_row = """
        order by {},{},{}
        """.format(self.user_id1, self.user_id2, datetime_name)

    def get_query(self):
        # print("".join([self.result, self.users_row, self.order_row]))

        query = "".join([self.result, self.users_row, self.order_row])
        file = open("sql " + self.user_id1 + ".txt", "w")
        file.write(query)
        return query
