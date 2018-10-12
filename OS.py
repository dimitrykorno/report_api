from enum import Enum


class OS(Enum):
    android = 0
    ios = 1
    amazon = 2

    @staticmethod
    def get_id(os):
        if os == OS.android or os == OS.amazon:
            return "android_id"
        elif os == OS.ios:
            return "ios_ifv"

    @staticmethod
    def get_aid(os):
        if os == OS.android or os == OS.amazon:
            return "google_aid"
        elif os == OS.ios:
            return "ios_ifa"

    @staticmethod
    def get_source(os):
        if os == OS.android:
            return "Google Play"
        elif os == OS.ios:
            return "AppStore"
        elif os == OS.amazon:
            return "Amazon"

    @staticmethod
    def get_os(os):
        if os.lower() == "android":
            return OS.android
        elif os.lower() == "ios":
            return OS.ios
        elif os.lower() == "amazon":
            return OS.amazon
        else:
            print("Ошибка в названии ОС")

    @staticmethod
    def get_os_string(os):
        if os is OS.android :
            return "android"
        elif os is OS.ios:
            return "ios"
        elif os is OS.amazon:
            return "amazon"