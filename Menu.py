import inspect


def menu(reports):
    """
    Создание меню по списку отчетов
    :param reports: словарь соответствия названия отчета и функции отчета
    :return:
    """
    while True:
        for rep in reports.keys():
            print(rep)
        print("Отчёт: ", end="")
        chosen_report = "999999"

        # Выбор отчета с клавиатуры
        while not chosen_report.isdigit() or int(chosen_report) > len(reports.keys()) or chosen_report == "0":
            chosen_report = input()

        # изменение стандартных значений исходных данных отчёта
        settings = _get_settings(list(reports.values())[int(chosen_report) - 1])
        # try:
        list(reports.values())[int(chosen_report) - 1](*settings)
        # except Exception as error:
        # print(error)
        # print(error.args)
        # continue


def _get_settings(f):
    """
    Получение списка исходных данных для отёчта и изменение этих данных вводом с клавиатуры
    :param f: отчёт (функция)
    :return:
    """
    # Получаем список аргументов функции
    sig = inspect.signature(f)
    args = list(sig.parameters.keys())
    defaults = []
    types = []
    # сохраняем список значений по-умолчанияю
    for arg in args:
        defaults.append(sig.parameters[arg].default)
        types.append(sig.parameters[arg].default.__class__)
    # запрос на изменение параметров по-умлчанию с клавиатуры
    while True:
        string = ""
        for arg, default, i in zip(args, defaults, range(len(args))):
            string += str(i) + ". " + str(arg) + ": " + str(default) + "\n"

        # последний вариант, когда ввод закончен
        string += str(len(args)) + ". Отчёт.\n"
        string += "Выбор: "

        # выбор параметра для изменения (учет неверных вариантов)
        input_args = input(string)
        while not input_args.isdigit() or int(input_args) > len(args):
            input_args = input()
        # если последний вариант, то отправляем отчет на выполнение
        if int(input_args) == len(args):
            return defaults

        # парсинг нового значения
        new_value = input(args[int(input_args)] + ": ")
        if types[int(input_args)] is int:
            new_value = int(new_value)
        elif types[int(input_args)] is bool:
            if new_value in ("True", "true", 1):
                new_value = True
            elif new_value in ("False", "false", 0):
                new_value = False
        elif types[int(input_args)] is list:
            new_value = new_value.replace(" ", "").split(",")
        elif new_value in ("", " "):
            new_value = defaults[int(input_args)]
        elif new_value in ("None", "none"):
            new_value = None

        # обновляем список исходных значений
        defaults[int(input_args)] = new_value
