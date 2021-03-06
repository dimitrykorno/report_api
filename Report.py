from datetime import datetime
from weakref import WeakValueDictionary

from report_api.OS import OS
from report_api.Classes.MySqlHandler import MySQLHandler
from report_api.Classes.QueryHandler import QueryHandler
from report_api.Utilities.Utils import test_devices_android, test_devices_ios, get_timediff, draw_plot


# works in Python 2 & 3

class _Singleton(type):
    """ A metaclass that creates a Singleton base class when called. """
    _instances = WeakValueDictionary()

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super(_Singleton, cls).__call__(*args, **kwargs)
            cls._instances[cls] = instance
        else:
            print("Recreating Singleton object! Check your code.")
        return cls._instances[cls]

    def get_instance(cls):
        if cls._instances:
            return cls._instances
        else:
            return None


class Singleton(_Singleton('SingletonMeta', (object,), {})):
    def tearDown(cls):
        cls._instances = {}


class Report(Singleton):
    '''
    Класс Report - ядро отчёта. Оно осуществляет подключение к базе данных, извлечение событий, их парсинг,
    определение статуса текущего пользователя, появление нового пользователя, исключает тестеров,
    контролирует солблюдение версии приложения, автоматически определяет первую сессию, время с установки,
    время с последнего входа..
    '''
    not_found = set()
    Parser = None
    Event_class = None
    User = None
    os = OS.ios
    app = "sop"
    user_skip_list = test_devices_android if OS == OS.android else test_devices_ios
    installs = None
    total_users = 0
    current_event = None
    previous_event = None
    current_user = None
    previous_user = None
    current_app_version = None
    event_data = None
    user_status_check = False

    @classmethod
    def reset(cls):
        cls.not_found = set()
        cls.Parser = None
        cls.Event_class = None
        cls.User = None
        cls.os = OS.ios
        cls.app = "sop"
        cls.user_skip_list = test_devices_ios if OS == OS.ios else test_devices_android
        cls.installs = None
        cls.total_users = 0
        cls.current_event = None
        cls.previous_event = None
        cls.current_user = None
        cls.previous_user = None
        cls.current_app_version = None
        cls.event_data = None
        cls.user_status_check = False

    @classmethod
    def get_next_event(cls):
        '''
        Получение и обработка следующего события из базы.
        В случае, когда события закончатся, база данных отключается
        :return: True - Событие обработано, получен игрок и событие, None - события закончились
        '''
        event = None
        while not event:
            # Попытка достать из потока базы событие
            cls.event_data = MySQLHandler.fetch_next_event()
            # если больше нет событий, останавливаем цикл в отчёте
            if not cls.event_data:
                for log in cls.not_found:
                    print(log)
                Singleton.tearDown(cls)
                return None

            event = None
            # пытаемся получить игрока из события
            got_user = cls._get_next_user()
            # если с ним нет проблем (прошел все проверки)
            if got_user:
                # получаем следующее время события и версию приложения
                if "app_version_name" in cls.event_data.keys():
                    cls.current_app_version = cls.event_data["app_version_name"]

                # заменяем предыдущее событие текущим
                if cls.current_event:
                    cls.previous_event = cls.current_event
                # парсим событие
                if "event_json" in cls.event_data.keys():
                    json = cls.event_data["event_json"]
                else:
                    json = None

                event = cls.Parser.parse_event(event_name=cls.event_data["event_name"],
                                               event_json=json, datetime=cls.event_data["event_datetime"])
                cls.current_event = event

                # если нужно, обновляем статус игрока
                if cls.user_status_check:
                    cls.current_user.user_status_update(cls.current_event, cls.previous_event)

                # проверка первой сессии
                if not cls.is_new_user() \
                        and cls.current_user \
                        and cls.current_user.first_session \
                        and cls.current_user.is_new_session(cls.previous_event, cls.current_event):
                    cls.current_user.first_session = False

                # если это первое событие, то делаем предыдущее событие равным ему
                if not cls.previous_event:
                    cls.previous_event = cls.current_event
            # если все хорошо, и мы получили текущего игрока и событие, они передадутся в отчёт
            # в противном случае возьмем следующее событие по циклу
            if got_user and event:
                return True

    @classmethod
    def _get_next_user(cls):
        '''
        Получить игрока текущего события
        :return: True - игрок получен (старый или новый),
                    None - игрок не получен (тестер, установка неверной версии приложения, в списке пропуска)
        '''
        # делаем текущего предыдущим (будет верно даже если следующий "не подойдет")
        if cls.current_user:
            cls.previous_user = cls.current_user

        user_id1 = cls.event_data[OS.get_aid(cls.os)]
        user_id2 = cls.event_data[OS.get_id(cls.os)]
        # проверка на тестеров
        if {user_id1, user_id2} & cls.user_skip_list:
            # print("skip test", new_user.user_id, new_user.installed_app_version)
            # cls.testers.add(new_user.user_id)
            return None

        # если пользователь отличается от предыдущего
        if cls.is_new_user(next_id1=user_id1, next_id2=user_id2):

            new_user = cls.User(id_1=user_id1, id_2=user_id2)

            '''
            # БЛОК ПРОВЕРОК ПОЛЬЗОВАТЕЛЯ
            # проверка версии приложения
            if cls.min_app_version not in (None, "0", "0.0.0") and (
                            (not cls.exact and new_user.installed_app_version < cls.min_app_version) or (
                                    cls.exact and new_user.installed_app_version != cls.min_app_version) or (
                                    user_id1 in cls.user_ifa_skip_list or
                                    user_id2 in cls.user_ifv_skip_list)):
                print("skip vers", new_user.user_id, new_user.installed_app_version)
                cls.user_ifa_skip_list.add(user_id1)
                cls.user_ifv_skip_list.add(user_id2)
                return None
            '''

            '''
                # проверка на установку
            elif cls.installs and not cls._in_installs(user_id1, user_id2):
                print("skip inst", new_user.user_id)
                cls.user_ifa_skip_list.add(user_id1)
                cls.user_ifv_skip_list.add(user_id2)
                new_user.skipped = True
                return None
            '''
            '''
                # проверка по странам
            elif cls.countries and new_user.country not in cls.countries:
                print("skip countries", cls.countries, new_user.country)
                return None
            '''
            # если он прошел проверки на версию установленного приложения и на тестера, то становится
            # новым текущим пользователем

            if MySQLHandler.installs_list:
                new_user.install_date, \
                new_user.publisher, \
                new_user.source, \
                new_user.installed_app_version, \
                new_user.country = cls._get_install_data(user_id1, user_id2)
            else:
                new_user.install_date = cls.event_data["event_datetime"].date()
                new_user.publisher = None
                new_user.source = None
                new_user.installed_app_version = cls.event_data[
                    "app_version_name"] if "app_version_name" in cls.event_data.keys() else None
                new_user.country = cls.event_data[
                    "country_iso_code"] if "country_iso_code" in cls.event_data.keys() else None

            new_user.first_session = True
            new_user.entries.append(new_user.install_date)

            cls.current_user = new_user
            cls.total_users += 1

        if cls.current_user:
            # Проверка, текущего пользователя могли добавить в пропуск
            if cls.current_user.is_skipped():
                return None

            # Обновляем заходы пользователя
            if not cls.current_user.first_session and cls.current_user.is_new_session(cls.previous_event,cls.current_event) and cls.event_data["event_datetime"].date() not in cls.current_user.entries:
                cls.current_user.entries.append(cls.event_data["event_datetime"].date())
            if "event_datetime" in cls.event_data.keys():
                cls.current_user.last_enter = cls.event_data["event_datetime"]

        # если текущий юзер первый, то вначале мы не смогли обновить предыдущего, поэтому делаем предыдущего им же
        if not cls.previous_user:
            cls.previous_user = cls.current_user

        return True

    @classmethod
    def set_app_data(cls, parser, user_class, event_class, app="sop", os="ios", user_status_check=False):
        if not user_class:
            print("Класс User не найден.")
        if not parser or not parser.parse_event:
            print("Парсер не найден. Или функция parse_event в парсере не найдена.")
            return None
        cls.reset()
        MySQLHandler.reset()
        cls.Parser = parser
        cls.Event_class = event_class
        cls.User = user_class
        cls.app = app
        cls.os = OS.get_os(os)
        cls.user_status_check = user_status_check

    @classmethod
    def set_installs_data(cls, additional_parameters=[],
                          period_start=datetime.strptime("2018-06-14", "%Y-%m-%d").date(),
                          period_end=str(datetime.now().date()),
                          min_version=None,
                          max_version=None,
                          countries_list=[],
                          users_chunk=0):
        MySQLHandler.installs_list = []
        MySQLHandler.installs_handler = QueryHandler(os=cls.os,
                                                     database="installs",
                                                     app=cls.app,
                                                     additional_parameters=additional_parameters,
                                                     period_start=period_start,
                                                     period_end=period_end,
                                                     min_version=min_version,
                                                     max_version=max_version,
                                                     countries_list=countries_list
                                                     )
        MySQLHandler.current_chunk = 0
        MySQLHandler.users_chunk = users_chunk
        MySQLHandler.get_installs()

    @classmethod
    def set_events_data(cls, additional_parameters=[],
                        period_start=datetime.strptime("2018-06-14", "%Y-%m-%d").date(),
                        period_end=str(datetime.now().date()),
                        min_version=None,
                        max_version=None,
                        countries_list=[],
                        events_list=[]):
        MySQLHandler.result = None
        MySQLHandler.db = None
        MySQLHandler.fetched_events_list = []
        MySQLHandler.events_handler = QueryHandler(os=cls.os,
                                                   database="events",
                                                   app=cls.app,
                                                   additional_parameters=additional_parameters,
                                                   period_start=period_start,
                                                   period_end=period_end,
                                                   min_version=min_version,
                                                   max_version=max_version,
                                                   countries_list=countries_list,
                                                   events_list=events_list
                                                   )

    @classmethod
    def is_new_user(cls, next_id1=None, next_id2=None):
        '''
        Определение новый ли пользователь
        :param next_id1: id первого пользоваетели
        :param next_id2: id второго пользователя
        :return: новый ли пользователь
        '''
        if cls.current_user and (next_id1 or next_id2):
            return  cls.current_user.user_id not in (next_id1, next_id2)
        elif cls.current_user and cls.previous_user:
            if cls.current_user.user_id == "" and cls.previous_user.user_id == "":
                if cls.previous_user.country != cls.current_user.country:
                    return True
                if cls.previous_user.installed_app_version > cls.current_user.installed_app_version:
                    return True
            return cls.current_user.user_id != cls.previous_user.user_id

        elif cls.current_user and not cls.previous_user:
            return False

        elif not cls.current_user:
            return True
        else:
            return False

    @classmethod
    def _in_installs(cls, user_id1, user_id2):
        """
        Проверка на вхождение юзера в список загрузок
        :param user_id1: ads_id
        :param user_id2: device_id
        :return:
        """
        if MySQLHandler.installs_list:
            for install in MySQLHandler.installs_list:
                if user_id2 != "" and user_id2 in install[OS.get_id(cls.os)] or \
                                        user_id1 != "" and user_id1 in install[OS.get_aid(cls.os)]:
                    return True
            return False
        else:
            return True

    @classmethod
    def _get_install_data(cls, user_id1, user_id2):
        """
        Получение данных о дате установки, паблишере и трекере
        :param user_id1: ads_id
        :param user_id2: device_id
        :return: (дата установки, паблишер, трекер)
        """
        for install in MySQLHandler.installs_list:
            if user_id2 != "" and user_id2 in install[OS.get_id(cls.os)] or \
                                    user_id1 != "" and user_id1 in install[OS.get_aid(cls.os)]:
                publisher_name = install["publisher_name"] if install["publisher_name"] != "" else "Organic"
                tracker_name = install["tracker_name"] if install["tracker_name"] not in (
                    "", "unknown") else OS.get_source(cls.os)
                return install["install_datetime"].date(), \
                       publisher_name, \
                       tracker_name, \
                       install["app_version_name"], \
                       install["country_iso_code"]
        return None, None, None, None, None

    '''
    @staticmethod
    def _insert_installs_in_sql(sql):
        """
        Функция для добавления в SQL запрос строк с проверкой на вхождение в списки установок
        (сохраняет txt файл в корне проекта с последним исполненным запросом для проверки руками)
        :param sql: оригинальный SQL запрос
        :return: SQL запрос с вставленными строками
        """
        user_id1_list = []
        user_id2_list = []
        for install in Report.installs:
            user_id1_list.append(install[OS.get_aid(Report.os)])
            user_id2_list.append(install[OS.get_id(Report.os)])
        lines = sql.splitlines()
        last_line_index = -2
        first_lines = lines
        for index, line in enumerate(lines):
            if "order by" in line:
                first_lines = lines[:-(len(lines) - index)]
                last_line_index = index

        new_sql = '\n'.join(first_lines)
        new_sql += """
        and
        (
        {0} in ("{1}") 
        and
        {2} in ("{3}")
        )
""".format(OS.get_aid(Report.os), '","'.join(map(str, user_id1_list)), OS.get_id(Report.os),
           '","'.join(map(str, user_id2_list)))
        new_sql += lines[last_line_index]
        file = open("sql.txt", "w")
        file.write(new_sql)
        return new_sql
    '''

    @classmethod
    def get_timediff(cls, datetime_1=None, datetime_2=None, measure="min"):
        '''
        Определение разницы во времени между текущим и предыдущим событием
        :param measure: мера min/sec/day (min по умолчанию)
        :return: разница во времени по умолчанию - между текущим и предыдущим событием
        '''
        if not datetime_1 and not datetime_2:
            datetime_1 = cls.current_event.datetime
            datetime_2 = cls.previous_event.datetime
        return get_timediff(datetime_1, datetime_2, measure=measure)

    @classmethod
    def get_time_since_install(cls, measure="day", user="current"):
        '''
        время с момента установки
        :param measure: мера min/sec/day (по умолчанию day)
        :param user: текущий или предыдущий пользователь
        :return: время с момента установки
        '''
        if user == "current":
            new_user = cls.current_user
            new_datetime = cls.current_event.datetime
        else:
            new_user = cls.previous_user
            new_datetime = cls.previous_event.datetime

        return get_timediff(new_datetime, new_user.install_date, measure=measure)

    @classmethod
    def get_time_since_last_enter(cls, measure="day", user="current"):
        '''
        время с момента установки
        :param measure: мера min/sec/day (по умолчанию day)
        :param user: текущий или предыдущий пользователь
        :return: время с момента последнего входа
        '''
        if user == "current":
            new_user = cls.current_user
            new_datetime = cls.current_event.datetime
        else:
            new_user = cls.previous_user
            new_datetime = cls.previous_event.datetime

        return get_timediff(new_datetime, new_user.entries[-1 * min(2, len(new_user.entries))], measure=measure)

    @classmethod
    def skip_current_user(cls):
        '''
        Пропуск последующих событий текущего игрока
        '''
        if cls.event_data[OS.get_aid(cls.os)] != "":
            cls.user_skip_list.add(cls.event_data[OS.get_aid(cls.os)])
        if cls.event_data[OS.get_id(cls.os)] != "":
            cls.user_skip_list.add(cls.event_data[OS.get_id(cls.os)])
        cls.current_user.skipped = True

    @staticmethod
    def draw_plot(x, y_dict, xtick_steps=1, xticks_move=0, x_ticks_labels=list(),
                  title=None, folder="", show=False, format="png"):
        draw_plot(x, y_dict,
                  xtick_steps=xtick_steps, xticks_move=xticks_move, x_ticks_labels=x_ticks_labels,
                  title=title, folder=folder, show=show, format=format)

    @classmethod
    def get_installs(cls):
        return MySQLHandler.installs_list
