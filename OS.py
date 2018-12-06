from enum import Enum


class OS(Enum):
    """
    Список опреационных систем
    """
    android = 0
    ios = 1
    amazon = 2

    @staticmethod
    def get_id(os):
        """
        Вернуть id, соответствующий оси
        :param os: ос
        :return:
        """
        # if isinstance(os, str):
        #     os = OS.get_os(os)
        if os in (OS.android, OS.amazon) or (type(os) is str and os.lower() in ("android", "amazon")):
            return "android_id"
        elif os == OS.ios or (type(os) is str and os.lower() == "ios"):
            return "ios_ifv"

    @staticmethod
    def get_aid(os):
        """
        Вернуть aid, соответствующий оси
        :param os: ос
        :return:
        """
        # if isinstance(os, str):
        #     os = OS.get_os(os)
        if os in (OS.android, OS.amazon) or (type(os) is str and os.lower() in ("android", "amazon")):
            return "google_aid"
        elif os == OS.ios or (type(os) is str and os.lower() == "ios"):
            return "ios_ifa"

    @staticmethod
    def get_source(os):
        """
        Вернуть текстовый вид источника (стора), соответствующий оси
        :param os: ос
        :return:
        """
        if isinstance(os, str):
            os = OS.get_os(os)
        if os == OS.android:
            return "Google Play"
        elif os == OS.ios:
            return "AppStore"
        elif os == OS.amazon:
            return "Amazon"

    @staticmethod
    def get_os(os):
        """
        Вернуть объект списка, соответствующий текстовой оси
        :param os: ос
        :return:
        """
        if isinstance(os, OS):
            return os
        if os.lower() == "android":
            return OS.android
        elif os.lower() == "ios":
            return OS.ios
        elif os.lower() == "amazon":
            return OS.amazon
        else:
            print("Get os: Ошибка в названии ОС. Возвращаем Android.")
            return OS.android

    @staticmethod
    def get_os_string(os):
        """
        Вернуть текстовое название оси
        :param os: ос
        :return:
        """
        if isinstance(os, str):
            if os.lower() in ("android", "ios", "amazon"):
                return os
            else:
                print("Get_os_string: Неверное имя ОС. Возвращаем Android.")
                return "android"
        if os is OS.android:
            return "android"
        elif os is OS.ios:
            return "ios"
        elif os is OS.amazon:
            return "amazon"
