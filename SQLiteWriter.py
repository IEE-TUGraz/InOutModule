import os
import sqlite3

import pandas as pd
import pyomo.core.base.set
import pyomo.environ as pyo
import math
from InOutModule.printer import Printer
import re
printer = Printer.getInstance()


def parse_mps_bounds_and_rhs(mps_path: str):
    """
    Parse BOUNDS and RHS with the corresponding variable and equation names from a Model MPS file.
    Returns:
      var_bounds: dict varname -> (lb: Optional[float], ub: Optional[float])
      rhs: dict rowname -> float
      all_vars: set of variable names observed anywhere in file (columns)
    Notes:
      - Follows common MPS conventions: default lower bound is 0, default upper bound is +inf.
      - BOUND types handled: LO, UP, FX, FR, MI, BV.
    """


    section = None
    var_bounds = {}  # var -> (lb, ub) (None means -inf/+inf or unknown)
    rhs = {}
    all_vars = set()

    # regex helpers
    tok_re = re.compile(r"\S+")

    with open(mps_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            if not line:
                continue
            # MPS uses marker lines like 'ENDATA'
            # Determine section
            # Some MPS files have free-format; detect section keywords in column 1
            first = line[:10].strip().upper()
            if first in ("NAME", "ROWS", "COLUMNS", "RHS", "RANGES", "BOUNDS", "ENDATA"):
                section = first
                # Keep going to next line (section header line usually has no data)
                if section == "ENDATA":
                    break
                continue

            if section == "COLUMNS":
                # Extract variable names that appear as columns; helpful to know all variables
                # COLUMNS lines : column_name  rowname  value  [rowname2 value2 ...]
                tokens = tok_re.findall(line)
                if len(tokens) >= 2:
                    colname = tokens[0]
                    all_vars.add(colname)
                # continue scanning - no further action needed here for bounds
            elif section == "BOUNDS":
                # BOUNDS lines typically: BOUND_TYPE  BOUND_SET  VAR_NAME  [VALUE]
                # Example: LO BND1  X1  0
                tokens = tok_re.findall(line)
                if not tokens:
                    continue
                # if token count < 3 it's malformed; skip
                # Typical tokens: [type, bset, var, value?]
                btype = tokens[0].upper()
                # Some MPS writers omit the bound-set name or use different spacing.
                # We'll try to find the variable name as the third token if present, else second.
                if len(tokens) >= 3:
                    var = tokens[2]
                    # possible that tokens[1] is the boundset name; if there are only 2 tokens var may be tokens[1]
                elif len(tokens) == 2:
                    var = tokens[1]
                else:
                    # can't find var name
                    continue

                all_vars.add(var)

                # value may be present; if not, some types imply values (e.g., BV -> 0/1)
                val = None
                if len(tokens) >= 4:
                    try:
                        val = float(tokens[3])
                    except Exception:
                        val = None

                # initialize if not present: default MPS is LB=0, UB=+inf (we encode as None for +inf)
                if var not in var_bounds:
                    default_lb = 0.0
                    default_ub = None  # None == +inf for our checks
                    var_bounds[var] = (default_lb, default_ub)

                lb, ub = var_bounds[var]

                if btype == "LO":
                    if val is not None:
                        lb = float(val)
                elif btype == "UP":
                    if val is not None:
                        ub = float(val)
                elif btype == "FX":
                    # fixed: both lb and ub equal value, default value is typically 0 if not present
                    v = float(val) if val is not None else 0.0
                    lb = v
                    ub = v
                elif btype == "FR":
                    # free variable: no bounds (both -inf and +inf)
                    lb = None
                    ub = None
                elif btype == "MI":
                    # minus infinity lower bound (no lower bound)
                    lb = None
                elif btype == "BV":
                    # binary - bounds 0..1
                    lb = 0.0
                    ub = 1.0
                else:
                    # unhandled bound type: leave as-is
                    pass

                var_bounds[var] = (lb, ub)

            elif section == "RHS":
                # RHS lines typically: RHS_NAME  ROWNAME  VALUE  [ROWNAME2 VALUE2 ...]
                tokens = tok_re.findall(line)
                if not tokens:
                    continue
                # tokens[0] might be RHS name; pairs follow
                # Find first token that looks like a row name + value pair: usually tokens[1:]
                pairs = tokens[1:]
                # consume in pairs
                it = iter(pairs)
                while True:
                    try:
                        row = next(it)
                        val_tok = next(it)
                    except StopIteration:
                        break
                    try:
                        val = float(val_tok)
                    except Exception:
                        continue
                    rhs[row] = val

            else:
                # other sections ignored for this task
                pass

    # Ensure all variables seen exist in var_bounds: if not, assign default (LB=0, UB=+inf)
    for v in all_vars:
        if v not in var_bounds:
            var_bounds[v] = (0.0, None)

    return var_bounds, rhs, all_vars

def model_to_sqlite(model: pyo.base.Model, filename: str, use_moving_window: bool = False) -> None:
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
        print("Check variable bounds and RHS values for numerical issues (reading from the Build_Model.mps)")

        mps_path = r"C:\GIT\LEGO-Pyomo\Build_Model.mps"
        try:
            var_bounds_dict, rhs_dict, all_vars = parse_mps_bounds_and_rhs(mps_path)
        except FileNotFoundError:
            raise RuntimeError(f"MPS file not found at {mps_path}. Make sure the file exists and path is correct.")

        # Convert dict to var_rows list like you used before: (var_name, index, lb, ub)
        var_rows: List[Tuple[str, str, Optional[float], Optional[float]]] = []
        for varname, (lb, ub) in sorted(var_bounds_dict.items()):
            # Store ABSOLUTE values in SQLite for easier queries
            lb_val = None if lb is None else abs(float(lb))
            ub_val = None if ub is None else abs(float(ub))
            var_rows.append((varname, "", lb_val, ub_val))

        # Save ABSOLUTE bounds to sqlite
        df_var_bounds = pd.DataFrame(var_rows, columns=["var_name", "index", "lb", "ub"])
        if not use_moving_window:
            df_var_bounds.to_sql("var_bounds", cnx, if_exists="replace", index=False)
        else:
            df_var_bounds.to_sql("var_bounds", cnx, if_exists="append", index=False)

        # Save RHS (store absolute values for easier threshold queries)
        if rhs_dict:
            df_rhs = pd.DataFrame(
                [(k, abs(v)) for k, v in rhs_dict.items()],
                columns=["row_name", "rhs_value"]
            )
            if not use_moving_window:
                df_rhs.to_sql("rhs", cnx, if_exists="replace", index=False)
            else:
                df_rhs.to_sql("rhs", cnx, if_exists="append", index=False)

        # -----------------------
        # Bounds numeric-range check
        # -----------------------
        if var_rows:
            all_bounds = []
            for (name, idx, lb, ub) in var_rows:
                if lb is not None:
                    all_bounds.append((name, idx, "LB", lb))
                if ub is not None:
                    all_bounds.append((name, idx, "UB", ub))

            abs_bounds = [(name, idx, b_type, val, val)
                          for (name, idx, b_type, val) in all_bounds
                          if val is not None and val != 0]

            if not abs_bounds:
                print("No finite non-zero bounds found in the MPS BOUNDS section.")
            else:
                max_abs = max(abs_bounds, key=lambda x: x[3])
                min_abs = min(abs_bounds, key=lambda x: x[3])
                max_value = max_abs[3]
                min_value = min_abs[3]

                if min_value <= 0:
                    print("Smallest non-zero absolute bound is non-positive or zero; skipping log-scale comparison for bounds.")
                else:
                    log_range = math.log10(max_value) - math.log10(min_value)
                    if log_range >= 6:
                        printer.warning("Bound range higher than 1e6, this may lead to numerical issues in the solver!")
                        print(f"Highest ABS bound: {max_abs[0]}[{max_abs[1]}] ({max_abs[2]}) = {max_abs[4]}")
                        print(f"Lowest non-zero ABS bound: {min_abs[0]}[{min_abs[1]}] ({min_abs[2]}) = {min_abs[4]}")
                        print(f"Saving variable bounds for comparison to {filename}")
                        cnx.commit()
                    else:
                        print("No problematic bound range detected (range < 1e6).")
        else:
            print("No variable bounds extracted from MPS.")

        # RHS numeric-range check
        if rhs_dict:
            # Collect absolute non-zero RHS values
            rhs_abs_list = [(row_name, abs(val)) for row_name, val in rhs_dict.items() if val is not None and val != 0]

            if not rhs_abs_list:
                print("No finite non-zero RHS values found in the MPS RHS section.")
            else:
                # find largest and smallest absolute RHS
                max_rhs = max(rhs_abs_list, key=lambda x: x[1])
                min_rhs = min(rhs_abs_list, key=lambda x: x[1])
                max_rhs_val = max_rhs[1]
                min_rhs_val = min_rhs[1]

                if min_rhs_val <= 0:
                    print("Smallest non-zero absolute RHS is non-positive or zero; skipping log-scale comparison for RHS.")
                else:
                    rhs_log_range = math.log10(max_rhs_val) - math.log10(min_rhs_val)
                    if rhs_log_range >= 6:
                        printer.warning("RHS range higher than 1e6, this may lead to numerical issues in the solver!")
                        print(f"Highest ABS RHS: {max_rhs[0]} = {max_rhs_val}")
                        print(f"Lowest non-zero ABS RHS: {min_rhs[0]} = {min_rhs_val}")
                        print(f"Saving RHS values for comparison to {filename}")
                        cnx.commit()
                    else:
                        print("No problematic RHS range detected (range < 1e6).")
        else:
            print("No RHS entries extracted from MPS.")

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
