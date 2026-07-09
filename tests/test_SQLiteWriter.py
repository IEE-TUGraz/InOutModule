import sqlite3

import pyomo.environ as pyo
from InOutModule.printer import Printer

from SQLiteWriter import model_to_sqlite

printer = Printer.getInstance()


def create_simple_model(variable_names: list[str]) -> pyo.ConcreteModel:
    """
    Creates a simple pyomo model.
    :param variable_names: Names of the variables to add to the model.
    :return: The pyomo model.
    """
    model = pyo.ConcreteModel()

    for variable_name in variable_names:
        setattr(model, variable_name, pyo.Var(["index_a", "index_b"], domain=pyo.NonNegativeReals, initialize=0))
        variable = getattr(model, variable_name)
        variable["index_a"].set_value(1)
        variable["index_b"].set_value(0)

    variable_y = getattr(model, "variable_y")
    model.objective = pyo.Objective(expr=2 * variable_y["index_a"] + 3 * variable_y["index_b"])
    model.Constraint1 = pyo.Constraint(expr=3 * variable_y["index_a"] + 4 * variable_y["index_b"] >= 1)

    return model


def get_sqlite_table_names(filename):
    """
    Gets the names of all tables in a SQLite database.
    :param filename: Path to the SQLite database file.
    :return: Set of SQLite table names.
    """
    connection = sqlite3.connect(filename)

    cursor = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    )

    rows = cursor.fetchall()
    table_names = set()

    for row in rows:
        table_name = row[0]
        table_names.add(table_name)

    connection.close()

    return table_names


def test_model_to_sqlite_behaviour(tmp_path):
    """
    Tests if model_to_sqlite removes the tables from a previous model.
    :param tmp_path: Temporary path for the test (provided by pytest).
    :return: None
    """
    filename = tmp_path / "model.sqlite"
    printer.information(f"Writing SQLite test database to {filename}")

    first_variables = ["variable_x", "variable_y"]

    model_to_sqlite(create_simple_model(first_variables), str(filename))
    assert "variable_x" in get_sqlite_table_names(filename)
    assert "variable_y" in get_sqlite_table_names(filename)

    second_variables = ["variable_y"]

    model_to_sqlite(create_simple_model(second_variables), str(filename))
    assert "variable_x" not in get_sqlite_table_names(filename)
    assert "variable_y" in get_sqlite_table_names(filename)
