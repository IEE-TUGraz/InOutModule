import datetime

import pyomo
from rich.console import Console
from rich.markup import escape


class Printer:
    """
    Singleton class for printing messages to the console and (potentially) logging them to a file.
    This class provides methods to print messages with different severity levels (e.g., error, warning, success, ...)
    and handles the formatting of the messages based on the console width for use with Rich.

    The class is designed to be used as a singleton (use 'getInstance()' to get the instance).
    """

    __instance = None

    @staticmethod
    def getInstance() -> "Printer":
        """
        Use this method to get the singleton instance of the Printer class.
        :return: Singleton instance of Printer
        """
        if Printer.__instance is None:
            Printer(Console(width=80))
        return Printer.__instance

    def __init__(self, console: Console):
        """
        Should normally not be called by users. Represents a singleton class Printer.

        Attributes
        ----------
        console : Console
            The console object to be used for operations.
        logfile : str or None
            Path to the log file. Defaults to None, indicating no log file is used.
        add_timestamp_to_logfile : bool
            A flag indicating whether timestamps should be added to the log file.
            Defaults to True, meaning timestamps will be added.

        Raises
        ------
        Exception
            If an attempt is made to create more than one instance of this singleton
            class.

        :param console: The console object to initialize the Printer instance with.
        """
        if Printer.__instance is not None:
            raise Exception("Printer is a singleton but got initialized twice")

        Printer.__instance = self
        self.console = console
        self.logfile = None
        self.add_timestamp_to_logfile = True

    def set_width(self, width: int) -> None:
        """
        Sets the width of the console output.
        :param width:
        :return: None
        """
        self.console.width = width
        return None

    def set_logfile(self, logfile: str) -> None:
        """
        Sets the log file that will be used for logging purposes.
        No log file will be used if this is set to None.

        :param logfile: Path to the log file to be set
        :return: None
        """
        self.logfile = logfile
        return None

    def get_logfile(self) -> str:
        """
        Returns the current log file path.
        :return: Path to the current log file
        """
        return self.logfile

    def handle_hard_wrap_chars(self, text: str, prefix: str, hard_wrap_chars: str) -> str:
        """
        Handles the hard wrap characters in the text. If the text exceeds the console width,
        it will be truncated and the hard wrap characters will be added at the end.
        If hard_wrap_chars is None, the text will be returned as is.

        Example:
        >>> printer = Printer.getInstance()
        >>> printer.set_width(16)
        >>> printer.handle_hard_wrap_chars("This is a test-text", "PREFIX ", "...")
        'This i...'

        :param text: Text to be printed
        :param prefix: Prefix (used to determine the length of the total text)
        :param hard_wrap_chars: Chars to be added at the end of the text if it exceeds the console width
        :return: Adjusted text
        """
        if hard_wrap_chars is not None and len(text) + len(prefix) > self.console.width:
            text = text[:self.console.width - len(prefix) - len(hard_wrap_chars)] + hard_wrap_chars
        return text

    def error(self, text: str, prefix: str = "Error: ", hard_wrap_chars: str = None) -> None:
        """
        Handles the error message. If the text exceeds the console width and hard_wrap_chars is set,
        it will be truncated and the hard wrap characters will be added at the end. The error message
        is printed in red. The message is also logged to the log file if one is set.

        :param text: Text to be printed
        :param prefix: Prefix (default: "Error: ")
        :param hard_wrap_chars: Chars to be added at the end of the text if it exceeds the console width,
        None if text should not be truncated
        :return: None
        """

        text = self.handle_hard_wrap_chars(text, prefix, hard_wrap_chars)
        if len(prefix) > 0:
            self.console.print(f"[red]{prefix}[/red]{text}")  # Only have prefix in color if it is set
        else:
            self.console.print(f"[red]{text}[/red]")
        self._log(f"{prefix}{text}")
        return None

    def warning(self, text: str, prefix: str = "Warning: ", hard_wrap_chars: str = None):
        """
        Handles the warning message. If the text exceeds the console width and hard_wrap_chars is set,
        it will be truncated and the hard wrap characters will be added at the end. The warning message
        is printed in yellow. The message is also logged to the log file if one is set.

        :param text: Text to be printed
        :param prefix: Prefix (default: "Warning: ")
        :param hard_wrap_chars: Chars to be added at the end of the text if it exceeds the console width,
        None if text should not be truncated
        :return: None
        """

        text = self.handle_hard_wrap_chars(text, prefix, hard_wrap_chars)
        if len(prefix) > 0:
            self.console.print(f"[yellow]{prefix}[/yellow]{text}")  # Only have prefix in color if it is set
        else:
            self.console.print(f"[yellow]{text}[/yellow]")
        self._log(f"{prefix}{text}")
        return None

    def success(self, text: str, prefix: str = "", hard_wrap_chars: str = None):
        """
        Handles the success message. If the text exceeds the console width and hard_wrap_chars is set,
        it will be truncated and the hard wrap characters will be added at the end. The success message
        is printed in green. The message is also logged to the log file if one is set.

        :param text: Text to be printed
        :param prefix: Prefix (default: No prefix)
        :param hard_wrap_chars: Chars to be added at the end of the text if it exceeds the console width,
        None if text should not be truncated
        :return: None
        """

        text = self.handle_hard_wrap_chars(text, prefix, hard_wrap_chars)
        if len(prefix) > 0:
            self.console.print(f"[green]{prefix}[/green]{text}")  # Only have prefix in color if it is set
        else:
            self.console.print(f"[green]{text}[/green]")
        self._log(f"{prefix}{text}")
        return None

    def information(self, text: str, prefix: str = "", hard_wrap_chars: str = None):
        """
        Handles the information message. If the text exceeds the console width and hard_wrap_chars is set,
        it will be truncated and the hard wrap characters will be added at the end. The information message
        is printed in white. The message is also logged to the log file if one is set.

        :param text: Text to be printed
        :param prefix: Prefix (default: No prefix)
        :param hard_wrap_chars: Chars to be added at the end of the text if it exceeds the console width,
        None if text should not be truncated
        :return: None
        """

        text = self.handle_hard_wrap_chars(text, prefix, hard_wrap_chars)
        self.console.print(f"{prefix}{text}")
        self._log(f"{prefix}{text}")
        return None

    def linear_expression(self, expr: pyomo.core.expr.numeric_expr.LinearExpression) -> None:
        """
        Pretty-prints a linear expression to the console and logs it to the logfile if one is set.

        :param expr: The linear expression to be printed
        :return: None
        """

        expr_str = " ".join([f"{"+" if coef >= 0 else ""}{coef:.1f}*{var}" for coef, var in zip(expr.linear_coefs, expr.linear_vars)])
        self.console.print(escape(expr_str))
        self._log(expr_str)
        return None

    def _log(self, text: str) -> None:
        """
        Logs the provided text message to a logfile (by appending) if one is specified.
        If no logfile is set, the function does nothing. If add_timestamp_to_logfile is
        set to True, adds a timestamp to the message before logging.

        :param text: The message to be logged
        :return: None
        """
        if self.logfile is not None:
            with open(self.logfile, "a") as f:
                for line in text.splitlines():
                    if self.add_timestamp_to_logfile:
                        f.write(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {line}\n")
                    else:
                        f.write(line + "\n")
        return None

    def separator(self) -> None:
        """
        Prints a separator line to the console. The line is made up of dashes and
        is as long as the console width. The line is printed in white.
        :return: None
        """
        self.console.print("-" * self.console.width, style="white")
        self._log("-" * self.console.width)
        pass


# Helper function to pretty-print the values of a Pyomo indexed variable within zone of interest
def pprint_zoi_var(var, zoi, index_positions: list = None, decimals: int = 2):
    """
    Pretty-print the values of a Pyomo indexed variable within a specified zone of interest (ZOI).
    This function formats the output to show the variable's name, lower and upper bounds, value,
    fixed status, stale status, and domain type for each index in the ZOI.
    The output is aligned in a tabular format for better readability.
    :param var: The Pyomo indexed variable to be printed.
    :param zoi: The zone of interest, which is a list of indices to be considered for printing.
    :param index_positions: A list of index positions to be printed. If None, all indices will be printed.
    :param decimals: The number of decimal places to display for the variable's value.
    :return: None
    """
    from pyomo import environ as pyo  # Import Pyomo environment for variable handling

    if index_positions is None:
        index_positions = [0]

    key_list = ["Key"]
    lower_list = ["Lower"]
    value_list = ["Value"]
    upper_list = ["Upper"]
    fixed_list = ["Fixed"]
    stale_list = ["Stale"]
    domain_list = ["Domain"]

    for index in var:
        # check if at least one index is in zone of interest
        if not any(i in zoi for i in index):
            continue
        key_list.append(str(index))
        lower_list.append(f"{var[index].lb:.2f}" if var[index].has_lb() else str(var[index].lb))
        value_list.append(f"{pyo.value(var[index]):.2f}" if not var[index].value is None else str(var[index].value))
        upper_list.append(f"{var[index].ub:.2f}" if var[index].has_ub() else str(var[index].ub))
        fixed_list.append(str(var[index].fixed))
        stale_list.append(str(var[index].stale))
        domain_list.append(str(var[index].domain.name))

    key_spacer = len(max(key_list, key=len))
    lower_spacer = len(max(lower_list, key=len))
    value_spacer = len(max(value_list, key=len))
    upper_spacer = len(max(upper_list, key=len))
    fixed_spacer = len(max(fixed_list, key=len))
    stale_spacer = len(max(stale_list, key=len))
    domain_spacer = len(max(domain_list, key=len))

    print(f"{var.name} : {var.doc}")
    print(f"    Size={len(var)}, In Zone of Interest={len(key_list) - 1}, Index={var.index_set()}")

    # Iterate over all lists and print the values
    for i in range(len(value_list)):
        print(f"    {key_list[i]:>{key_spacer}} : {lower_list[i]:>{lower_spacer}} : {value_list[i]:>{value_spacer}} : {upper_list[i]:>{upper_spacer}} : {fixed_list[i]:>{fixed_spacer}} : {stale_list[i]:>{stale_spacer}} : {domain_list[i]:>{domain_spacer}}")
