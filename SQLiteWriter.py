import os
import sqlite3

import pandas as pd
import pyomo.core.base.set
import pyomo.environ as pyo
import math
from InOutModule.printer import Printer

printer = Printer.getInstance()


def model_to_sqlite(model: pyo.base.Model, filename: str) -> None:
    """
    Save the model to a SQLite database.
    :param model: Pyomo model to save
    :param filename: Path to the SQLite database file
    :return: None
    """

    if os.path.dirname(filename) != "":
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    cnx = sqlite3.connect(filename)

    if filename == "Model_limits.sqlite":
        # RHS and Bounds only
        print("Check variable bounds for numerical issues")
        var_rows: List[Tuple[str, str, Optional[float], Optional[float]]] = []
        for o in model.component_objects():
            match type(o):
                case pyomo.core.base.var.IndexedVar:
                    indices = o.extract_values()
                    for key, value in indices.items():
                        lb = None if o[key].lb is None else float(o[key].lb)
                        ub = None if o[key].ub is None else float(o[key].ub)
                        var_rows.append((o.name, str(key), lb, ub))

                    df_var_bounds = pd.DataFrame(var_rows, columns=["var_name", "index", "lb", "ub"])
                    df_var_bounds.to_sql("var_bounds", cnx, if_exists="replace", index=False)


        if var_rows:
            # Collect all finite bound values with their names
            all_bounds = []

            for (name, idx, lb, ub) in var_rows:
                if lb is not None:
                    all_bounds.append((name, idx, "LB", lb))
                if ub is not None:
                    all_bounds.append((name, idx, "UB", ub))

            # Only absolute values
            abs_bounds = [(name, idx, b_type, abs(val), val)
                          for (name, idx, b_type, val) in all_bounds
                          if val != 0]  # skip zero bounds for the "lowest non-zero"

            if abs_bounds:
                # 1️ Largest absolute bound
                max_abs = max(abs_bounds, key=lambda x: x[3])
                # 2. Lowest non-zero absolute bound
                min_abs = min(abs_bounds, key=lambda x: x[3])
                max_value = max_abs[3]
                min_value = min_abs[3]
            if math.log10(max_value) - math.log10(min_value) >= 6:
                printer.warning("Bound range higher than 1e6, this may lead to numerical issues in the solver!")
                print(f"Highest ABS bound: {max_abs[0]}[{max_abs[1]}] ({max_abs[2]}) = {max_abs[3]}")
                print(f"Lowest non-zero ABS bound: {min_abs[0]}[{min_abs[1]}] ({min_abs[2]}) = {min_abs[4]}")
                print(f"Saving variable bounds for comparison to {filename}")
                cnx.commit()
            else:
                print("No finite non-zero bounds found.")


        cnx.close()

    else:
        for o in model.component_objects():
            match type(o):
                case pyomo.core.base.set.OrderedScalarSet:
                    df = pd.DataFrame(o.data())
                case pyomo.core.base.var.IndexedVar | pyomo.core.base.param.IndexedParam | pyomo.core.base.param.ScalarParam:
                    indices = [str(i) for i in o.index_set().subsets()]
                    df = pd.DataFrame(pd.Series(o.extract_values()), columns=['values'])
                    if len(indices) == len(df.index.names):
                        if len(indices) > 1:
                            df = df.reset_index().rename(columns={f"level_{i}": b for i, b in enumerate(indices)})
                        else:
                            df = df.reset_index().rename(columns={"index": indices[0]})
                        df = df.set_index(indices)
                case pyomo.core.base.objective.ScalarObjective:
                    df = pd.DataFrame([pyo.value(o)], columns=['values'])
                case pyomo.core.base.constraint.ConstraintList | pyomo.core.base.constraint.IndexedConstraint | pyomo.core.base.expression.IndexedExpression:  # Those will not be saved on purpose
                    continue
                case _:
                    printer.warning(f"Pyomo-Type {type(o)} not implemented, {o.name} will not be saved to SQLite")
                    continue
            df.to_sql(o.name, cnx, if_exists='replace')
            cnx.commit()
        cnx.close()
        pass
