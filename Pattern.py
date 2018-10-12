import sys
from Classes.Events import *


class Pattern:
    """
    Паттерны пользовательског оповедения. Определяются последовательностью событий,
    событиями, прерывающими паттерн и отрезками событий, на которых ищется совпадение (сессия/день/все данные)
    """
    filename = "Гипотеза на основе паттерна"
    users_not_covered_with_pattern = set()

    def __init__(self, name="Pattern name", same_day_pattern=True, same_session_pattern=False, pattern_string=[],
                 pattern_parameters=[None],
                 completed_once=False, min_completions=0, max_completions=None):
        """
        Инициализация паттерна
        :param name: название паттерна
        :param same_day_pattern: поиск по каждому дню
        :param same_session_pattern: поиск по каждой сессии
        :param pattern_string: паттерн в тектовом виде
        :param pattern_parameters: параметры событий в паттерне
        :param completed_once: паттерн выполняется только единожды
        :param min_completions: минимальное кол-во выполнений паттерна
        :param max_completions: максимальное кол-во выполнений паттерна
        """
        self.name = name
        self.same_day_pattern = same_day_pattern
        self.same_session_pattern = same_session_pattern
        if self.same_session_pattern:
            self.same_day_pattern = True
        self.pattern = []
        for string in pattern_string:
            c = getattr(sys.modules[__name__], string)
            self.pattern.append(c)
        self.parameters = pattern_parameters
        self.parameters += [None] * (len(self.pattern) - len(self.parameters))
        self.completed_once = completed_once
        self.min_completions = min_completions if not completed_once else 1
        self.max_completions = max_completions if not completed_once else 1
        if self.max_completions and self.min_completions > self.max_completions:
            print("Pattern completion number error. Changed min completions to max.")
            self.min_completions = self.max_completions

        self.breakers = []
        self.breakers_parameters = [None]

    def set_breakers(self, breakers_string=[], breakers_parameters=[None]):
        """
        Добавляем события, прерывающие паттерн
        :param breakers_string: тектовый стисок
        :param breakers_parameters: параметры
        :return:
        """
        for string in breakers_string:
            c = getattr(sys.modules[__name__], string)
            self.breakers.append(c)
        self.breakers_parameters = breakers_parameters

    def is_followed(self, events_list):
        """
        Проверка следования паттерну
        :param events_list: список событий
        :return: количество выполнения паттерна
        """
        pattern_completion = [False] * len(self.pattern)
        completion_number = 0
        pattern_index = 0
        # print("events list", events_list)
        # print("pattern", self.pattern)
        # print("param",self.parameters)
        # проверяем каждое событие
        for user_event in events_list:
            # print("list",patterns_list, len(patterns_list), pattern_index)
            # print("completion",pattern_completion,len(pattern_completion), completion_number)

            # сопоставляем со следующим классом из паттерна и его параметрами
            if Pattern._is_matching_object(user_event, self.pattern[pattern_index], self.parameters[pattern_index]):
                # print("MATCH!")
                pattern_completion[pattern_index] = True
                pattern_index += 1
                # print("event", user_event)
                # print(pattern_index, pattern_completion)
            # если есть события, прерывающие паттерн, проверяем на них
            if self.breakers:
                for breaker_index, breaker in enumerate(self.breakers):
                    # print("breakers")
                    # сопоставляем с breaker'ом
                    if Pattern._is_matching_object(user_event, self.breakers[breaker_index],
                                                   self.breakers_parameters[breaker_index]):
                        # print("break")
                        pattern_completion = [False] * len(self.pattern)
                        pattern_index = 0
                        # print("break", user_event)
                        # print(pattern_index, pattern_completion)
                        # если breaker является так же первым элементом (вдруг)
                        if Pattern._is_matching_object(user_event, self.pattern[pattern_index],
                                                       self.parameters[pattern_index]):
                            pattern_completion[pattern_index] = True
                            pattern_index += 1

            # если паттерн совпал, увеличиваем кол-во выполнений
            if False not in pattern_completion:
                completion_number += 1
                pattern_completion = [False] * len(self.pattern)
                pattern_index = 0

        return completion_number

    @staticmethod
    def _is_matching_object(real_object, filler_class, filler_parameters=None):
        """
        Проверка совпадения события по классу и параметрам
        :param real_object: проверяемый объект
        :param filler_class: сверяемый класс
        :param filler_parameters: сверяемые параметры
        :return: совпадение True/False
        """
        match = False
        if isinstance(real_object, filler_class):
            match = True
            if filler_parameters:
                for attr in filler_parameters.keys():
                    if hasattr(real_object, attr):
                        if real_object.__getattribute__(attr) != filler_parameters[attr]:
                            return False
                    else:
                        print("Incorrect pattern. Object", real_object.__class__.__name__, "doesn't have", attr,
                              "attribute.")
        return match


