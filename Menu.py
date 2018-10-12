import inspect


def menu(reports):
    while True:
        for rep in reports.keys():
            print(rep)
        print("Отчёт: ", end="")
        chosen_report = "999999"

        while not chosen_report.isdigit() or int(chosen_report) > len(reports.keys()) or chosen_report == "0":
            chosen_report = input()

        settings = _get_settings(list(reports.values())[int(chosen_report) - 1])
        # try:
        list(reports.values())[int(chosen_report) - 1](*settings)
        # except Exception as error:
        # print(error)
        # print(error.args)
        # continue


def _get_settings(f):
    sig = inspect.signature(f)
    args = list(sig.parameters.keys())
    defaults = []
    types = []
    for arg in args:
        defaults.append(sig.parameters[arg].default)
        types.append(sig.parameters[arg].default.__class__)
    while True:
        string = ""
        for arg, default, i in zip(args, defaults, range(len(args))):
            full_default = default
            string += str(i) + ". " + str(arg) + ": " + str(full_default) + "\n"

        string += str(len(args)) + ". Отчёт.\n"
        string += "Выбор: "

        input_args = input(string)
        while not input_args.isdigit() or int(input_args) > len(args):
            input_args = input()
        if int(input_args) == len(args):
            return defaults
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
        defaults[int(input_args)] = new_value
