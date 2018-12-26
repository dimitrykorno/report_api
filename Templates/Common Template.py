import pandas as pd
from datetime import datetime
import os
from report_api.Report import Report
from report_api.Utilities.Utils import time_count,  try_save_writer, check_folder, check_arguments

# ОПРЕДЕЛЯЕМ НАЗВАНИЕ ПРИЛОЖЕНИЯ (sop/kc) - важно стандартизировать название
app = "sop"


# ФУНКЦИЯ ОТЧЁТА (ВСЕ ОТЧЁТЫ ДОЛЖНЫ НАЗЫВАТЬСЯ new_report)
# СТАРНДАРТНЫЕ ВХОДНЫЕ ДАННЫЕ - СЕГМЕНТ ПОЛЬЗОВАТЕЛЕЙ -  СПИСОК ОС, ПЕРИОД УСТАНОВКИ, ВЕРСИЯ ПРИЛОЖЕНИЯ ПРИ УСТАНОВКЕ,
# СПИСОК СТРАН, ОТКУДА ПРОИЗОШЛА УСТАНОВКА
# НАЗВАНИЯ ПЕРЕМЕННЫХ ЛУЧШЕ НЕ МЕНЯТЬ, ДОБАВЛЯТЬ СВОИ МОЖНО.
# noinspection PyDefaultArgument,PyDefaultArgument
@time_count
def new_report(os_list=["iOS", "Android"],
               period_start="2018-11-01",
               period_end=None,
               min_version=None,
               max_version=None,
               countries_list=[]):
    # ПРОВЕРКА ФОРМАТА ДАННЫХ В АРГУМЕНТАХ. ДЛЯ КОРРЕКТНОЙ ПРОВЕРКИ НЕОБХОДИМО ИСПОЛЬЗОВАТЬ СТАНДАРТНЫЕ НАЗВАНИЯ ПЕРЕМЕННЫХ.
    errors = check_arguments(locals())
    # СПИСОК ВЫХОДНЫХ ФАЙЛОВ, ОТПРАВЛЯЕМЫХ БОТУ
    result_files = []
    # ПАПКА НАЗНАЧЕНИЯ ДЛЯ СОХРАНЕНИЯ ВЫХОДНЫХ ФАЙЛОВ
    folder_dest = "Results/Отчёт по качеству уровней/"
    # ПРИ ЗАПУСКЕ ИЗ БОТА СОХРАНЯЕМ В ОТДУЛЬНУЮ ПАПКУ ДЛЯ КАЖДОГО ПОЛЬЗОВАЕТЛЯ
    # ИМЯ ПОЛЬЗОВАТЕЛЯ - НЕ АРГУМЕНТ, А ПАРАМЕТР ФУНКЦИИ (ФУНКЦИЯ - ОБЪЕКТ)
    if hasattr(new_report, 'user'):
        folder_dest += str(new_report.user) + "/"
    # ПРОВЕРКА СУЩЕСТВОВАНИЯ ПАПКИ НАЗВАНИЧЕНИЯ. СОЗДАНИЕ ПАПКИ, ЕСЛИ ЕЁ НЕТ.
    check_folder(folder_dest)
    # ЕСЛИ ВОЗНИКЛИ ОШИБКИ ПРИ ПРОВЕРКЕ ВХОДНЫХ ДАННЫХ - ВОЗВРАЩАЕМ СПИСОК ОШИБОК.
    # (смотри функцию check_arguments)
    # print(errors)
    if errors:
        return errors, result_files

    # БЛОК ПРОВЕРКИ ЦЕЛОСТНОСТИ ВХОДНЫХ ДАННЫХ, ЧТОБЫ ИЗБЕЖАТЬ ОШИБОК ЗНАЧЕНИЯ И ТИПА
    # ОСОБЕННО ПРОВЕРИТЬ СОБСТВЕННЫЕ ВХОДНЫЕ ПАРАМЕТРЫ
    if isinstance(period_start, str):
        period_start = datetime.strptime(period_start, "%Y-%m-%d").date()
    if isinstance(period_end, str):
        period_end = datetime.strptime(period_end, "%Y-%m-%d").date()
    if min_version:
        min_version.replace(",", ".")
    if max_version:
        max_version.replace(",", ".")

    # НАЧАЛО ОТЧЁТА ПО КАЖДОЙ ОС
    for os_str in os_list:
        # БАЗА ДАННЫХ
        # !!!
        # Report - основной класс для отчёта. В него загружаются исходные данные, из него же получают события.
        # !!!
        # ПЕРЕДАЕМ В НЕГО ПАРСЕР И КЛАСС ПОЛЬЗОВАТЕЛЯ ИЗ НАШЕГО МОДУЛЯ, НУЖНУЮ ОС И ПРИЛОЖЕНИЕ
        Report.set_app_data(parser=Parse, user_class=User, os=os_str, app=app)
        # ПЕРЕДАЕМ ДАННЫЕ ДЛЯ ПОЛУЧЕНИЯ НУЖНОГО СПИСКА ПОЛЬЗОВАТЕЛЕЙ
        Report.set_installs_data(additional_parameters=[],
                                 period_start=period_start,
                                 period_end=period_end,
                                 min_version=min_version,
                                 max_version=max_version,
                                 countries_list=countries_list)
        # ДОБАВЛЕНИЕ ДАННЫХ ДЛЯ ПОЛУЧЕНИЯ НУЖНЫХ СОБЫТИЙ. КОНЕЦ ПЕРИОДА - НЕ ОПРЕДЕЛЕН, ЕСЛИ НУЖНЫ ВСЕ СОБЫТИЯ ВЫБРАННЫХ ПОЛЬЗОВАТЕЛЕЙ
        # СОБЫТИЯ ОПИСЫВАЮТСЯ В ДВУХ ВИДАХ ДЛЯ ПОДДЕРЖКИ АБ-ТЕСТОВ (СМОТРИ ОПИСАНИЕ СОБЫТИЙ С АБ-ТЕСТАМИ)
        # СОБЫТИЯ ПЕРЕДАЕЮТСЯ В ВИДЕ МАССИВА TUPLE, ГДЕ ПЕРВЫЙ ЭЛЕМЕТН TUPLE - НАЗВАНИЕ EVENT_NAME, А
        # ВТОРОЙ ЭЛЕМЕНТ TUPLE - ПАРАМЕТР EVENT_JSON
        # НАЗВАНИЯ МОЖНО ПЕРЕДАВАТЬ В [ ... ] - СПИСОК НАЗВАНИЙ С 'AND' МЕЖДУ НИМИ, { ... } - СПИСОК С 'IN ( )'
        Report.set_events_data(additional_parameters=[],
                               period_start=period_start,
                               period_end=None,
                               min_version=None,
                               max_version=None,
                               events_list=[("Match3Events",),
                                            ("", "%Match3Events%"),
                                            ("CityEvent", "%StartGame%"),
                                            ("", "%CityEvent%StartGame%")])

        # ОПРЕДЕЛЕНИЕ ИЗНАЧАЛЬНЫХ ПАРАМЕТРОВ:
        # СОБЫТИЯ СОРТИРУЮТСЯ ПО ПОЛЬЗОВАТЕЛЮ И ПО ДАТЕ/ВРЕМЕНИ
        # Т.О. ПОЛУЧАЕМ ВСЕ ДАННЫЕ ПО КАЖДОМУ ПОЛЬЗОВАТЕЛЮ ПО ПОРЯДКУ
        # ОБЫЧНО СНАЧАЛА РАССЧИТАННЫЕ ДАННЫЕ ПО ПОЛЬЗОВАТЕЛЮ ДОБАВЛЯЮТСЯ В СПЕЦИАЛЬНО ОПРЕДЕЛЕННУЮ ДЛЯ ЭТОГО СТРУКТУРУ ДАННЫХ (СПИСОК, СЛОВАРЬ...)
        parameters = ["1", "2", "3"]
        user_data = dict.fromkeys(parameters, 0)
        common_data = dict.fromkeys(parameters, 0)

        # ЗАТЕМ СЛИВАЮТСЯ В ОБЩУЮ СТРУКТУРУ ДАННЫХ (СПИСОК, СЛОВАРЬ..)
        # ДАННЫЕ НЕ ДОБАВЛЯЮТСЯ СРАЗУ В pandas.DataFrame, Т.К. ЭТА ОПЕРАЦИЯ СЛИШКОМ ДОРОГАЯ (ДОЛГАЯ), ИСПОЛЬЗУЮТСЯ БОЛЕЕ БЫСТРЫЕ ТИПЫ ДЛЯ ХРАНЕНИЯ ЗНАЧЕНИЙ.
        def flush_user_data():
            for par in user_data:
                common_data[par] += user_data[par]

        # ЦИКЛ С ПОЛУЧЕНИЕМ СЛЕДУЮЩЕГО СОБЫТИЯ (С ПЕРВЫМ ВЫЗОВОМ ЭТОЙ ФУНКЦИИ ОТПРАВЛЯЕТСЯ ЗАПРОС В БАЗУ И НАЧИНАЮТ ПОСТУПАТЬ СОБЫТИЯ С КАЖДЫМ ТАКТОМ ЭТОГО ЦИКЛА
        while Report.get_next_event():

            # КОГДА У НАС СМЕНЯЕТСЯ ПОЛЬЗОВАТЕЛЬ, МЫ СЛИВАЕМ ДАННЫЕ ПРЕДЫДУЩЕГО В ОБЩИЕ И !ОБНУЛЯЕМ ПЕРСОНАЛЬНЫЕ ДАННЫЕ!
            # В ЭТОТ МОМЕНТ МЫ НАХОДИМСЯ УЖЕ НА ПЕРВОМ СОБЫТИИ СЛЕДУЮЩЕГО ПОЛЬЗОВАТЕЛЯ
            # !!!!! ПОЭТОМУ ВАЖНО !!!!! ЕСЛИ В ФУНКЦИИ flush_user_data НАМ НУЖЕН ДОСТУП К ПОСЛЕДНЕМУ
            # СОБЫТИЮ ПРЕДЫДУЩЕГО ПОЛЬЗОВАТЕЛЯ (ДАННЫЕ КОТОРОГО СЛИВАЕМ), НЕОБХОДИМО ОБРАЩАТЬСЯ К
            # ПРЕДЫДУЩЕМУ СОБЫТИЮ Report.previous_event
            # !!!
            if Report.is_new_user():
                flush_user_data()
                user_data = dict.fromkeys(parameters, 0)

            # ОБРАБОТКА ПОЛУЧЕННЫХ СОБЫТИЙ, ОБНОВЛЕНИЕ ДАННЫХ ПОЛЬЗОВАЕТЛЯ
            if isinstance(Report.current_event, EventClass1):
                user_data["1"] += Report.current_event.event_value
            elif isinstance(Report.current_event, EventClass2):
                user_data["2"] = Report.current_event.event_value

        # СЛИВ ДАННЫХ ПОСЛЕДНЕГО ПОЛЬЗОВАТЕЛЯ
        flush_user_data()

        # СОСТАВЛЕНИЕ НАЗВАНИЯ ФАЙЛА
        file_name = folder_dest + "NAME " + os_str + ".xlsx"
        # ОТКРЫТИЕ ПОТОКА ЗАПИСИ В ФАЙЛ EXCEL
        writer = pd.ExcelWriter(file_name)

        # СОЗДАНИЕ ТАБЛИЦЫ С ВЫХОДНЫМИ ДАННЫМИ
        df = pd.DataFrame(index=[], columns=parameters)
        df = df.fillna(0)

        df.append({
            "1": common_data["1"],
            "2": common_data["2"],
            "3": common_data["3"]
        }, ignore_index=True)

        # запись
        df.to_excel(excel_writer=writer, sheet_name="sheet")
        # попытка сохранить файл
        try_save_writer(writer, file_name)
        # добавление файла в результирующие файлы.
        result_files.append(os.path.abspath(file_name))

    # отправка результатов и ошибок. ошибки могут появиться в ходе выполнения в виде замечаний и тд.
    # они будут отпарвлены в чат-бот вместе с файлами.
    return errors, result_files
