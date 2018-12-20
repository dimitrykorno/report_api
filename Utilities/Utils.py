from matplotlib import pyplot as plt
import matplotlib
matplotlib.use('Agg')
import inspect
import time
from functools import wraps
import os
import math
from math import sqrt
import pandas as pd
import numpy as np
from math import log
from operator import mul, add, sub, pow
import datetime
from report_api.Classes.Events import Event


def time_count(fn):
    """
    Декоратор для подсчета времени выполнения функции
    :param fn: анализируемая функция
    :return: обернутая функция
    """

    @wraps(fn)
    def wrapped(*args, **kwargs):
        start_time = time.time()
        result = fn(*args, **kwargs)
        print("Время: %.5s сек.\n" % (time.time() - start_time))

        return result

    return wrapped


def time_medium(fn):
    """
    Декоратор для подсчета среднего времени выполнения часто исполняемой функции (например парсинга)
    :param fn:
    :return:
    """

    @wraps(fn)
    def wrapped(*args, **kwargs):
        start_time = time.perf_counter()
        result = fn(*args, **kwargs)
        Event.medium_time.append(round(float(time.perf_counter() - start_time), 8))

        return result

    return wrapped


def time_medium_2(fn):
    """
    Декоратор для подсчета среднего времени выполнения часто исполняемой функции (например парсинга)
    :param fn:
    :return:
    """

    @wraps(fn)
    def wrapped(*args, **kwargs):
        start_time = time.perf_counter()
        result = fn(*args, **kwargs)
        Event.medium_time_2.append(round(float(time.perf_counter() - start_time), 8))

        return result

    return wrapped


def get_medium_time():
    '''
    Получение результата расчетов среднего времени выполнения функции
    :return:
    '''
    for index, t in enumerate(Event.medium_time):
        if index < 100:
            print('{0:.8f}'.format(t), end=", ")
    print("")
    first = 0

    if len(Event.medium_time) > 0:
        first = sum(Event.medium_time) / len(Event.medium_time)
    return first


def get_medium_time_2():
    '''
    Получение результата расчетов среднего времени выполнения функции
    :return:
    '''
    # print(Event.medium_time_2)
    for index, t in enumerate(Event.medium_time_2):
        if index < 100:
            print('{0:.8f}'.format(t), end=", ")
    print("")
    second = 0

    if len(Event.medium_time_2) > 0:
        second = sum(Event.medium_time_2[1:]) / (len(Event.medium_time_2) - 1)
    return second


def get_timediff(datetime_1=None, datetime_2=None, measure="min"):
    '''
    Определение разницы во времени
    :param datetime_1: первое время
    :param datetime_2: второе время
    :param measure: мера min/sec/day (min по умолчанию)
    :return: разница во времени, по умолчанию - между текущим и предыдущим событием
    '''

    if measure in ("min", "m"):
        return abs(datetime_1.timestamp() - datetime_2.timestamp()) / 60
    elif measure in ("sec", "s"):
        return abs(datetime_1.timestamp() - datetime_2.timestamp())
    elif measure in ("day", "d"):
        if not type(datetime_1) is datetime.date:
            datetime_1 = datetime_1.date()
        if not type(datetime_2) is datetime.date:
            datetime_2 = datetime_2.date()
        return abs(datetime_1 - datetime_2).days
    else:
        print("Неверный промежуток времени")


def try_save_writer(wr, filename=""):
    while True:
        try:
            wr.save()
            break
        except PermissionError:
            print("!!! CLOSE FILE !!!", filename)
            time.sleep(2)


def sigma(raw_data, normal=False):
    """
    расчет сигмы
    :param raw_data: исходные данные
    :param normal: `True` нормальное распрелеление
    :return: возвращается сигма
    """
    if len(raw_data) > 5:
        data = outliers_iqr(raw_data, min_outliers=True, max_outliers=True, multiplier=3)
        if set(data) != set(raw_data):
            print("raw data:", raw_data)
            print("outliers grubbs:", set(raw_data) - set(data))
    else:
        data = raw_data
    # print("data:",data)

    if len(data) == 1:
        return 0
    data = sorted(list(data))
    average_data = sum(data) / len(data)
    # print("averange data: ",average_data)
    diffs = [0] * (len(data) - 1)
    for i in range(len(data) - 2):
        diffs[i] = data[i + 1] - data[i]
    avg = sum(diffs) / len(diffs)
    if avg == 0:
        return 0
    # print("avg diff:",avg)
    S = 0
    for x in data:
        S += pow((x - avg), 2)
    S = S / (len(data) - 1)
    S = sqrt(S)
    # print("sigma:", S)
    if not normal:
        S = abs(average_data - S)
    # print("sigma:", S, "\n")
    return S


def outliers_iqr(data_list, where=None, max_outliers=True, min_outliers=False, multiplier=1.5, print_excl=True,
                 adaptive=False):
    """
    IQR-анализ выбросов
    :param data_list: список данных
    :param where: имя, что рассчитывем для понятного вывода
    :param max_outliers: максимальные выбросы
    :param min_outliers: минимальные выбросы
    :param multiplier: множитель
    :param print_excl: печать удаленных данных
    :param adaptive: подстраивающийся под объем массива множитель
    :return:
    """

    data_without_outliers = data_list
    excluded = None
    if adaptive and len(data_list) < 15:
        multiplier = round(multiplier * 0.25, 1)
    elif adaptive and len(data_list) > 150:
        multiplier = round(multiplier * 3, 1)

    if len(data_list) > 3:
        data_series = pd.Series(data_list)

        q1 = int(data_series.quantile([0.25]))
        q3 = int(data_series.quantile([0.75]))
        iqr = multiplier * (q3 - q1)
        data_list = sorted(data_list, reverse=True)

        if q1 != q3:
            if max_outliers:
                data_without_outliers = list(x for x in data_list if x < q3 + iqr)
                data_without_outliers = sorted(data_without_outliers, reverse=True)
                excluded = list(x for x in data_list if x >= q3 + iqr)

            if min_outliers:
                data_without_outliers = list(x for x in data_list if x > q1 - iqr)
                data_without_outliers = sorted(data_without_outliers, reverse=True)
                excluded = list(x for x in data_list if x <= q1 - iqr)

        if print_excl and excluded and len(excluded) > 0:
            print(where, "изначально:", data_list)
            print(where, "исключили:", excluded, "Q1:", q1, "Q3:", q3, str(multiplier) + "*IQR", iqr)

    return data_without_outliers


test_devices_android = {
    "3454d501-c01d-4ac6-bca3-44609643b8af", "1f016d8f-4f55-46f4-9407-07b7aab1f270",
    "b859894f-1b46-4ab1-8ffd-d046d86774f2", "a60e32b4-34b5-40dc-8fcf-539e0fba6b76",
    "e827bd22-9608-4d74-b6d3-88462e5306a4", "a7061aeb-89b4-4223-b7dc-d9ced7e4f31b",
    "a25ee26d-bdfc-4b0c-aaaf-d6962f9c0f6d", "49f53d94-6609-4b2a-9b91-d9d9a37f3494",
    "4edf535e-2741-4b74-a7aa-f3014d2e6d67", "11cb8c43-6647-4aa9-903c-32297af8ff5e",
    "50d21e9d-da82-403f-92ff-4c4bad4e7161", "f357c7e2-33f2-46dc-bd94-a0a9d9b09501"
                                            "2383cd9a-3575-4d3b-9d40-cb92add470d8",
    "632bc0d9-f62d-40bb-96b2-4c0bb26949f0",
    "50d21e9d-da82-403f-92ff-4c4bad4e7161", "4a21a9ef-ddeb-4b66-8c27-5c5729894e94"
}
test_devices_ios = {
    "68653B88-C937-479D-8548-C3020DDEB3C2", "FB5A77D5-5067-4E16-9830-65492BA81174",
    "F0890F68-5D19-46A5-88D9-CA54243376F8", "16E124C9-EE34-4179-814A-BB511A0A9025",
    "47ADA9C1-DADD-4115-A332-52F3291FBD76", "1BF0CFFB-D192-430C-9B67-612845A8A430",
    "AE03F39E-F442-4F3B-BAE2-954D028099C5", "E7C12811-08FF-4A9E-83D5-44EE98B4F530",
    "51794505-B791-4BE9-B6AA-B4E718A69847", "DC416F15-B602-44E5-9AC3-B34D60284F0B",
    "00AB8828-4167-44C6-AEAC-A46558ECEBB1", "E9073059-D0A1-44F8-B206-72AE0D366381",
    "282AC74C-EE9E-418E-959D-423025E5A97D", "2AB947E4-B36A-44DA-9666-8456D11DF47B",
    "6AA563C5-65E0-416C-AC48-EBD79AB20D69", "2FA9246C-1CEB-4904-872D-F5B07371A571",
    "AD64F7A4-30CF-4C89-A33C-65AABE3D1061",
    "B4AB781D-654D-4AEB-AAE9-2F4ABF1BC92B",
    "AD249847-3E4A-41FA-A764-2CF7BFAC1D8C",
}


# подозрения:
# iphone 7+ 38473BC6-0D1B-4CDA-BB2C-DB757D09F9C7


def daterange(start_date, end_date):
    """
    Генератор дат от начальной до конечной даты по каждомы дню
    :param start_date:
    :param end_date:
    :return:
    """
    if not end_date:
        end_date = datetime.datetime.now().date()
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)


def week_of_month(dt):
    """
    Returns the week of the month for the specified date.
    """

    first_day = dt.replace(day=1)
    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(math.ceil(adjusted_dom / 7.0))


def draw_plot(x_par, y_dict,
              xtick_steps=1, xticks_move=0, x_ticks_labels=list(),
              title=None, folder="", format="png",
              plot_type="plot", colors=None,
              show=False,
              additional_labels=[],
              calculate_sum=True):
    """
    рисование графика, расситано на одновременное рисование множества графиков из словаря
    :param x_par: иксы
    :param y_dict: словарь y-ков
    :param xtick_steps: шаги иксов
    :param xticks_move: смещение иксов
    :param x_ticks_labels: лейблы иксов
    :param title: название графика
    :param folder: папка с графиком
    :param show: печать графика
    :param format: формат сохранения картинки
    :return:
    """
    result_files=[]
    if type(y_dict) is not dict:
        y_dict = {"Y": y_dict}
    # Цвета
    hsv = plt.get_cmap('hsv')
    if not colors:
        colors = list(hsv(np.linspace(0, 0.9, len(y_dict))))
    else:
        colors = colors + list(hsv(np.linspace(0, 0.9, min(0, len(y_dict) - len(colors)))))
    # Линии
    linestyles = ['-']
    if len(y_dict) > 15:
        linestyles += ['-.']
    elif len(y_dict) > 25:
        linestyles += ['--']
    elif len(y_dict) > 35:
        linestyles += [':']
    current_linestyle = 0

    # Рисование графика
    plt.figure(figsize=(17, 8))
    ax = plt.subplot(111)
    for index, y_key in enumerate(y_dict):
        y_values = y_dict[y_key]
        if type(y_values[0]) is list:
            if len(additional_labels) != len(y_values[0]):
                label = additional_labels + ["unknown"] * (min(0, len(y_values[0]) - len(additional_labels)))
            else:
                label = additional_labels
            if plot_type == "bar":
                for i in range(len(y_values[0])):
                    ax.bar([x + i / (len(y_values[0]) + 1) for x in x_par], [y[i] for y in y_values],
                           width=1 / (len(y_values[0]) + 1),
                           label=str(label[i]) + " " + str(sum([y[i] for y in y_values])) if calculate_sum else str(
                               label[i]),
                           color=colors[i],
                           linestyle=linestyles[current_linestyle]
                           )
            else:
                if plot_type != "plot":
                    print("Unknown plot type:", plot_type)
                for i in range(len(y_values[0])):
                    ax.plot(x_par, [y[i] for y in y_values],
                            label=str(label[i]) + " " + str(sum([y[i] for y in y_values])) if calculate_sum else str(
                                label[i]),
                            color=colors[i],
                            linestyle=linestyles[current_linestyle]
                            )
        else:
            if additional_labels:
                label = additional_labels[0] if type(additional_labels) is list else additional_labels
            else:
                label = y_key
            if plot_type == "bar":
                ax.bar(x_par, y_values,
                       label=str(label) + " " + str(sum(y_values)) if calculate_sum else str(label),
                       color=colors[index],
                       linestyle=linestyles[current_linestyle])
            else:
                if plot_type != "plot":
                    print("Unknown plot type:", plot_type)
                ax.plot(x_par, y_values,
                        label=str(label) + " " + str(sum(y_values)) if calculate_sum else str(label),
                        color=colors[index],
                        linestyle=linestyles[current_linestyle])
        # Выбор линии
        current_linestyle = (current_linestyle + 1) % len(linestyles)

    # Тики и Лейблы
    ax.set_title([key for key in y_dict])
    ax.set_xticks(list(range(min(x_par), max(x_par) + 1, xtick_steps)))
    if xticks_move != 0:
        ax.set_xticklabels(
            list(range(min(x_par) + xticks_move, max(x_par) + 1 + xticks_move, xtick_steps)))
    elif x_ticks_labels:
        ax.set_xticklabels(x_ticks_labels[::xtick_steps])
    # Наклон лейблов
    if xtick_steps <= 5 and (x_ticks_labels and max([len(str(t)) for t in x_ticks_labels]) > 4):
        for tick in ax.get_xticklabels():
            tick.set_rotation(45)

    lgd = ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fancybox=True, shadow=True, ncol=5)
    if not title:
        title = str(list(y_dict.keys()))
    filename=folder + title + "." + format
    plt.savefig( filename, bbox_extra_artists=(lgd,), bbox_inches='tight')
    result_files.append(os.path.abspath(filename))
    if show:
        plt.show()
    return result_files

def draw_subplot(x_par, y_dict,
                 xtick_steps=1, xticks_move=0, x_ticks_labels=list(),
                 title=None, folder="", format="png",
                 plot_type="plot", colors=None,
                 show=False,
                 additional_labels=[],
                 size=(1, 1)):
    """
    рисование графика, расситано на одновременное рисование множества графиков из словаря
    :param x: иксы
    :param y_dict: словарь y-ков
    :param xtick_steps: шаги иксов
    :param xticks_move: смещение иксов
    :param x_ticks_labels: лейблы иксов
    :param title: название графика
    :param folder: папка с графиком
    :param show: печать графика
    :param format: формат сохранения картинки
    :return:
    """
    result_files=[]
    if type(y_dict) is not dict:
        y_dict = {"Y": y_dict}

    # Строки и столбцы
    rows = size[0]
    columns = size[1]

    # Цвета
    hsv = plt.get_cmap('hsv')
    if not colors:
        colors = list(hsv(np.linspace(0, 0.9, len(y_dict))))
    else:
        colors = colors + list(hsv(np.linspace(0, 0.9, min(0, len(y_dict) - len(colors)))))

    # Линии
    linestyles = ['-']
    if len(y_dict) > 15:
        linestyles += ['-.']
    elif len(y_dict) > 25:
        linestyles += ['--']
    elif len(y_dict) > 35:
        linestyles += [':']
    current_linestyle = 0

    # Рисование графика
    plt.figure(figsize=(17, 8))
    ax = plt.subplot(1, 1, 1)
    plot_num = 0
    for index, y_key in enumerate(y_dict):
        plot_num += 1
        # Новый экран
        if index != 0 and plot_num % (rows * columns):
            plot_num = 1
            lgd = ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fancybox=True, shadow=True, ncol=5)
            if not title:
                title = str(list(y_dict.keys()))
            filename=folder + title +" "+str(index)+ "." + format
            plt.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')
            if show:
                plt.show()
            result_files.append(os.path.abspath(filename))
            plt.figure(figsize=(17, 8))

        ax = plt.subplot(rows, columns, plot_num)
        y_values = y_dict[y_key]

        if type(y_values[0]) is list:
            if len(additional_labels) != len(y_values[0]):
                label = additional_labels + ["unknown"] * (min(0, len(y_values[0]) - len(additional_labels)))
            else:
                label = additional_labels
            if plot_type == "bar":
                for i in range(len(y_values[0])):
                    ax.bar([x + i / (len(y_values[0]) + 1) for x in x_par], [y[i] for y in y_values],
                           width=1 / (len(y_values[0]) + 1),
                           label=str(label[i]) + " " + str(sum([y[i] for y in y_values])),
                           color=colors[i],
                           linestyle=linestyles[current_linestyle]
                           )
            else:
                if plot_type != "plot":
                    print("Unknown plot type:", plot_type)
                for i in range(len(y_values[0])):
                    ax.plot(x_par, [y[i] for y in y_values],
                            label=str(label[i]) + " " + str(sum([y[i] for y in y_values])),
                            color=colors[i],
                            linestyle=linestyles[current_linestyle]
                            )
        else:
            if additional_labels:
                label = additional_labels[0] if type(additional_labels) is list else additional_labels
            else:
                label = y_key
            if plot_type == "bar":
                ax.bar(x_par, y_values,
                       label=str(label) + " " + str(sum(y_values)),
                       color=colors[index],
                       linestyle=linestyles[current_linestyle])
            else:
                if plot_type != "plot":
                    print("Unknown plot type:", plot_type)
                ax.plot(x_par, y_values,
                        label=str(label) + " " + str(sum(y_values)),
                        color=colors[index],
                        linestyle=linestyles[current_linestyle])
        # Выбор линии
        current_linestyle = (current_linestyle + 1) % len(linestyles)
        # Тики и Лейблы
        ax.set_title(y_key)
        ax.set_xticks(list(range(min(x_par), max(x_par) + 1, xtick_steps)))
        if xticks_move != 0:
            ax.set_xticklabels(
                list(range(min(x_par) + xticks_move, max(x_par) + 1 + xticks_move, xtick_steps)))
        elif x_ticks_labels:
            ax.set_xticklabels(x_ticks_labels[::xtick_steps])
        # Наклон лейблов
        if xtick_steps <= 5 and (x_ticks_labels and max([len(str(t)) for t in x_ticks_labels]) > 4):
            for tick in ax.get_xticklabels():
                tick.set_rotation(45)
        lgd = ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.2), fancybox=True, shadow=True, ncol=5)

    if not title:
        title = str(list(y_dict.keys()))
    filename=folder + title +" "+str(index+1)+ "." + format
    plt.savefig(filename, bbox_extra_artists=(lgd,), bbox_inches='tight')
    result_files.append(os.path.abspath(filename))
    if show:
        plt.show()
    return result_files

def log_approximation(x, y):
    """
    Логарифмическая аппроксимация
    :param x: икс
    :param y: y
    :return:
    """

    def square(x):
        return x * x

    x = list(map(add, [1] * len(x), x))
    log_x = list(map(log, x))
    avg_x = sum(log_x) / len(log_x)
    avg_y = sum(y) / len(y)
    b = sum(list(map(mul, list(map(sub, log_x, [avg_x] * len(log_x))), list(map(sub, y, [avg_y] * len(y)))))) / sum(
        list(map(square, list(map(sub, log_x, [avg_x] * len(log_x)))))
    )
    a = avg_y - b * avg_x

    # print(a, b)

    def func(new_x):
        new_x = list(map(add, [1] * len(new_x), new_x))
        return list(map(add, [a] * len(new_x),
                        list(map(mul, [b] * len(new_x), list(map(log, list(map(add, [1] * len(new_x), new_x))))))))

    return func


def check_folder(folder_dest, additional_folders=[]):
    if folder_dest[-1] != "/":
        folder_dest += "/"
    for folder in additional_folders:
        folder_dest += folder + "/"
    if not os.path.exists(folder_dest):
        os.makedirs(folder_dest)


def check_arguments(args):
    errors = set()
    def check_app_version(value):
        return type(value) is not str or ("." not in value and "," not in value)
    def check_digit(value):
        return (type(value) is str and not value.isdigit()) and (not type(value) is int)
    # рассматриваем спсиок аргументов и значений
    for arg in args:
        value = args[arg]
        if value is None:
            continue
        if arg in ("period_start", "period_end"):
            try:
                datetime.datetime.strptime(value, "%Y-%m-%d").date()
            except:
                errors.add("Формат даты не соответствует YYYY-MM-DD.")
        elif arg in ("os_list", "countries_list","days","app_versions"):
            if not type(value) is list:
                errors.add("ОС и страны должны быть в формате списка")
            elif arg == "os_list":
                for v in value:
                    if v.lower() not in ("ios", "android", "amazon"):
                        errors.add("ОС должны быть из ('ios','android','amazon')")
                        break
            elif arg == "countries_list":
                for v in value:
                    if len(v) != 2:
                        errors.add("Страны должны быть в формате ISO (RU, US, UA..)")
            elif arg=="app_versions":
                for v in value:
                    if check_app_version(v):
                        errors.add("Версии приложения должны быть строкового вида с точкой (4.7 или 1.3.5)")
                        break
            elif arg=="days":
                for v in value:
                    if check_digit(v):
                        errors.add("Дни должны быть списком чисел [1,2,3,4..]")
                        break
        elif arg in ("min_version", "max_version"):
            if check_app_version(value):
                errors.add("Версии приложения должны быть строкового вида (4.7 или 1.3.5)")
        elif arg in ("start", "quantity", "days_left", "days_max", "days_since_install","users_limit","max_level"):
            if check_digit(value):
                errors.add("Параметр " + arg + " должен быть числом.")
        elif arg in ("game_point","level_num"):
            if type(value) is not str or type(value) is str and not( len(value) == 4 or len(value)==8):
                errors.add("Уровни должны иметь формат 0031, квесты loc03q02")

    return "\n".join(errors)
