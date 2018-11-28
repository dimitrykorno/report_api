from collections import OrderedDict
from matplotlib import pyplot as plt
from report_api.Utilities.Utils import time_count
from report_api.OS import OS
from report_api.Data import Data
app = "sop"


# noinspection PyDefaultArgument,PyDefaultArgument
@time_count
def new_report(os_list=["iOS"],
               parser=None,
               app=None,
               folder_dest=None,
               app_version='7.0',
               users_limit=10000 ):

    for os_str in os_list:
        user_aid = OS.get_aid(os_str)
        sql = """
                    select {2}, event_name, event_json, MAX(event_datetime)
                    from {0}_events.events_{1}
                    where {2} in (select ios_ifa
                                        from {0}_events.installs_{1}
                                        where {2}<>"" 
                                        group by {2}, app_version_name
                                        having MIN(app_version_name)={4}
                                        )
                    group by {2}
                    LIMIT {3}
                    """.format(app, os_str.lower(), user_aid, users_limit, app_version)
        file = open("sql " + os_str.lower()+ " events.txt", "w")
        file.write(sql)
        file.close()
        last_events, db = Data.get_data(sql=sql, db=app,by_row=False, name="Загрузка последних событий.")
        last_events = [parser.parse_event(event["event_name"],event["event_json"],event["MAX(event_datetime)"]) for event  in last_events]
        db.close()
        events = {}

        for event in last_events:
                last_event_class = event.__class__.__name__
                # Оно добавляется в список последних событий
                if last_event_class not in events:
                    events[last_event_class] = 0
                events[last_event_class] += 1

        ordered_events = OrderedDict(sorted(events.items(), key=lambda t: t[1]))

        # Рисовка гистограммы
        plt.figure(figsize=(15, 6))
        plt.barh(range(len(list(ordered_events.values()))), list(ordered_events.values()))
        plt.yticks(range(len(list(ordered_events.values()))), list(ordered_events.keys()))
        plt.savefig(folder_dest+"last_event_histo " + os_str + ".png")
        plt.show()
