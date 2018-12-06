import inspect
import threading
import functools
import time
class Menu:
    bot = None
    reports = None

    @classmethod
    def menu_handsmode(cls, reports):
        """
        Создание меню по списку отчетов
        :param reports: словарь соответствия названия отчета и функции отчета
        :return:
        """
        cls.reports = reports
        while True:

            print(Menu.get_menu(reports))
            chosen_report = "999999"

            # Выбор отчета с клавиатуры
            while not chosen_report.isdigit() or int(chosen_report) > len(reports) or chosen_report == "0":
                chosen_report = input()
            chosen_report = int(chosen_report)
            # изменение стандартных значений исходных данных отчёта
            settings = Menu.get_settings_from_console(chosen_report)
            if settings is None:
                continue
            # try:
            reports[chosen_report - 1][1](*settings)
            # except Exception as error:
            # print(error)
            # print(error.args)
            # continue

    @classmethod
    def get_settings_from_console(cls, rep_num):
        """
        Получение списка исходных данных для отёчта и изменение этих данных вводом с клавиатуры
        :param f: отчёт (функция)
        :return:
        """
        # Получаем список аргументов функции
        args, defaults, types = Menu.get_defaults(cls.reports, rep_num)
        # запрос на изменение параметров по-умлчанию с клавиатуры
        while True:
            string = Menu.get_settings_str(cls.reports, rep_num, [args, defaults, types])

            # выбор параметра для изменения (учет неверных вариантов)
            input_args = input(string)
            while not input_args.isdigit() or int(input_args) > len(args)+1:
                input_args = input()
            if int(input_args) == len(args):
                return None
            # если последний вариант, то отправляем отчет на выполнение
            if int(input_args) == len(args)+1:
                return defaults

            # парсинг нового значения
            new_value = input(args[int(input_args)] + ": ")
            new_value = Menu.parse_value(new_value, defaults[int(input_args)], types[int(input_args)])

            # обновляем список исходных значений
            defaults[int(input_args)] = new_value

    @classmethod
    def get_menu(cls, reports):
        string = ""
        for rep_name in [rep[0] for rep in reports]:
            string += rep_name + "\n"
        string += "Выберите отчёт: "
        return string

    @classmethod
    def get_settings_str(cls, reports, rep_num, defaults=None):
        if defaults is None:
            args, defaults, types = cls.get_defaults(reports, rep_num)
        else:
            args, defaults, types = defaults[0], defaults[1], defaults[2]
        string = " ".join(reports[rep_num - 1][0].split(".")[1].split())+":\n\n"
        for arg, default, i in zip(args, defaults, range(len(args))):
            def_str = "" if default is None else str(default)
            string += str(i) + ". " + str(arg) + ": " + def_str + "\n"

        # последний вариант, когда ввод закончен
        string += str(len(args)) + ". НАЗАД.\n"
        string += str(len(args)+1) + ". ОТЧЁТ.\n"
        string += "Выбор: "
        return string

    @classmethod
    def get_defaults(cls, reports, rep_num):
        rep = reports[rep_num - 1][1]
        # Получаем список аргументов функции
        sig = inspect.signature(rep)
        args = list(sig.parameters.keys())
        defaults = []
        types = []
        # сохраняем список значений по-умолчанияю
        for arg in args:
            defaults.append(sig.parameters[arg].default)
            types.append(sig.parameters[arg].default.__class__)
        return args, defaults, types

    @classmethod
    def parse_value(cls, new_value, default, def_type):
        #print("start parse, new:", new_value,"def:", default,"type:", def_type, type(default))
        if new_value.lower() in ("null", "none"):
            new_value = None
        elif def_type is int:
            try:
                new_value = int(new_value)
            except:
                new_value = default
        elif def_type is bool:
            if str(new_value).lower() in ("true", "1", "yes"):
                new_value = True
            elif str(new_value).lower() in ("false", "0", "not"):
                new_value = False
            else:
                new_value = default
        elif def_type is list:
            try:
                new_value.replace("[","")
                new_value.replace("]","")
                new_value = new_value.replace(" ", "").split(",")
            except:
                new_value = default
        elif def_type is None:
            try:
                new_value = int(new_value)
            except:
                new_value = str(new_value)
        elif type(default) is str and len(default.split("-")[0])==4 and len(default.split("-")[1])==2 and len(default.split("-")[2])==2:
            if not (len(new_value.split("-")[0])==4 and len(new_value.split("-")[1])==2 and len(new_value.split("-")[2])==2):
                new_value=default
        elif new_value in ("", " "):
            new_value = default
        #print("end parse, new:", new_value,"def:", default,"type:", def_type)
        return new_value


    def synchronized(wrapped):
        lock = threading.Lock()
        #print(lock, id(lock))
        @functools.wraps(wrapped)
        def _wrap(*args, **kwargs):
            with lock:
                print("Calling '%s' with Lock %s from thread %s [%s]"
                      % (wrapped.__name__, id(lock),
                         threading.current_thread().name, time.time()))
                result = wrapped(*args, **kwargs)
                print("Done '%s' with Lock %s from thread %s [%s]"
                      % (wrapped.__name__, id(lock),
                         threading.current_thread().name, time.time()))
                return result

        return _wrap

    @classmethod
    @synchronized
    def execute_report(cls, reports, rep_num, settings):
        return reports[rep_num - 1][1](*settings)
