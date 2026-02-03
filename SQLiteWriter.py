import os
import sqlite3

import pandas as pd
import pyomo.core.base.set
import pyomo.environ as pyo

from InOutModule.printer import Printer

printer = Printer.getInstance()


def model_to_sqlite(model: pyo.base.Model, filename: str) -> None:
    """
    Save the model to a SQLite database.
    Automatically includes objective decomposition and dual values.

    :param model: Pyomo model to save
    :param filename: Path to the SQLite database file
    :return: None
    """

    if os.path.dirname(filename) != "":
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    cnx = sqlite3.connect(filename)

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
            case pyomo.core.base.suffix.Suffix:
                if str(o) in ["_relaxed_integer_vars", "dual"]:
                    continue  # Not saved on purpose
                else:
                    printer.warning(f"Pyomo-Type {type(o)} not implemented, {o.name} will not be saved to SQLite")
                    continue
            case _:
                printer.warning(f"Pyomo-Type {type(o)} not implemented, {o.name} will not be saved to SQLite")
                continue
        df.to_sql(o.name, cnx, if_exists='replace')
        cnx.commit()
    cnx.close()

    # Automatically add objective decomposition and dual values
    add_objective_decomposition_to_sqlite(filename, model)
    add_dual_values_to_sqlite(filename, model)
    pass


def add_solver_statistics_to_sqlite(filename: str, results, work_units=None) -> None:
    """
    Add solver statistics (like Gurobi work-units) to an existing SQLite database.
    :param filename: Path to the SQLite database file
    :param results: Pyomo solver results object
    :param work_units: Optional work units value (from Gurobi solver)
    :return: None
    """
    cnx = sqlite3.connect(filename)

    # Extract solver statistics
    stats = {}

    try:
        # Add work units if provided
        if work_units is not None:
            stats['work_units'] = float(work_units)

        # Get basic solver info from solver[0]
        if hasattr(results, 'solver') and len(results.solver) > 0:
            solver_info = results.solver[0]

            # Status and termination
            if hasattr(solver_info, 'status'):
                stats['solver_status'] = str(solver_info.status)
            if hasattr(solver_info, 'termination_condition'):
                stats['termination_condition'] = str(solver_info.termination_condition)
            if hasattr(solver_info, 'time'):
                try:
                    time_val = solver_info.time
                    if time_val is not None and str(type(time_val)) != "<class 'pyomo.opt.results.container.UndefinedData'>":
                        stats['solver_time'] = float(time_val)
                except:
                    pass

        # Get problem statistics
        if hasattr(results, 'problem'):
            problem = results.problem
            for attr in ['lower_bound', 'upper_bound', 'number_of_constraints',
                         'number_of_variables', 'number_of_nonzeros']:
                if hasattr(problem, attr):
                    value = getattr(problem, attr)
                    if value is not None:
                        stats[attr] = float(value) if isinstance(value, (int, float)) else str(value)

        # Create a DataFrame with solver statistics
        if stats:
            df = pd.DataFrame([stats])
            df.to_sql('solver_statistics', cnx, if_exists='replace', index=False)
            cnx.commit()
            work_units_str = f"{stats['work_units']:.2f}" if 'work_units' in stats else 'N/A'
            printer.information(f"Added solver statistics to SQLite database (work_units: {work_units_str})")
        else:
            printer.warning("No solver statistics found in results object")

    except Exception as e:
        printer.error(f"Failed to add solver statistics: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cnx.close()


def add_run_parameters_to_sqlite(filename: str, **parameters) -> None:
    """
    Add run parameters to an existing SQLite database.
    Creates a table 'run_parameters' with parameter names and values.

    :param filename: Path to the SQLite database file
    :param parameters: Keyword arguments containing parameter names and values
    :return: None

    Example:
        add_run_parameters_to_sqlite('model.sqlite',
                                     zoi='R1',
                                     dc_buffer=2,
                                     tp_buffer=1,
                                     scale_demand=1.3,
                                     scale_pmax=1.0)
    """
    cnx = sqlite3.connect(filename)

    try:
        # Convert parameters to DataFrame
        params = {}
        for key, value in parameters.items():
            # Convert None to string 'None' for storage
            if value is None:
                params[key] = 'None'
            elif isinstance(value, (int, float)):
                params[key] = float(value)
            else:
                params[key] = str(value)

        if params:
            df = pd.DataFrame([params])
            df.to_sql('run_parameters', cnx, if_exists='replace', index=False)
            cnx.commit()
            printer.information(f"Added run parameters to SQLite database: {', '.join([f'{k}={v}' for k, v in params.items()])}")
        else:
            printer.warning("No run parameters provided")

    except Exception as e:
        printer.error(f"Failed to add run parameters: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cnx.close()


def add_objective_decomposition_to_sqlite(filename: str, model: pyo.ConcreteModel) -> None:
    """
    Add objective function decomposition to SQLite database.
    This enables recalculation of ZOI objectives without the full model.

    The objective is decomposed into:
    - objective_constant: Single row with the constant term
    - objective_terms: Variable names, indices, and their coefficients

    :param filename: Path to the SQLite database file
    :param model: Pyomo model with objective
    :return: None
    """
    from pyomo.repn import generate_standard_repn

    cnx = sqlite3.connect(filename)

    try:
        # Decompose objective into linear representation
        repn = generate_standard_repn(model.objective.expr, quadratic=False)

        # Store objective decomposition as separate tables
        # 1. Constant term
        df_constant = pd.DataFrame([{'constant': repn.constant if repn.constant else 0.0}])
        df_constant.to_sql('objective_constant', cnx, if_exists='replace', index=False)

        # 2. Variable names, indices, and coefficients
        var_names = [var.parent_component().name for var in repn.linear_vars]
        var_indices = [str(var.index()) for var in repn.linear_vars]
        coefs = list(repn.linear_coefs)
        df_terms = pd.DataFrame({
            'var_name': var_names,
            'var_index': var_indices,
            'coefficient': coefs
        })
        df_terms.to_sql('objective_terms', cnx, if_exists='replace', index=False)

        cnx.commit()
        printer.information(f"Added objective decomposition to SQLite ({len(var_indices)} terms)")

    except Exception as e:
        printer.error(f"Failed to add objective decomposition: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cnx.close()


def add_dual_values_to_sqlite(filename: str, model: pyo.ConcreteModel) -> None:
    """
    Add dual values (shadow prices) from model constraints to SQLite database.

    Dual values are stored in tables named 'dual_<constraint_name>' with:
    - Index columns for the constraint
    - 'dual_value' column containing the dual/shadow price

    :param filename: Path to the SQLite database file
    :param model: Solved Pyomo model with dual suffix
    :return: None
    """
    cnx = sqlite3.connect(filename)

    try:
        if not hasattr(model, 'dual'):
            printer.warning("Model does not have dual suffix - no dual values to save")
            return

        total_duals = 0
        total_constraints = 0

        # Iterate through all constraints and save their dual values
        for constraint in model.component_objects(pyo.Constraint, active=True):
            constraint_name = constraint.name
            dual_data = []

            # Get dual values for this constraint
            for index in constraint:
                try:
                    dual_value = model.dual[constraint[index]]
                    if dual_value is not None:
                        # Store index and dual value
                        if isinstance(index, tuple):
                            # Multi-indexed constraint
                            row_data = {str(i): val for i, val in enumerate(index)}
                            row_data['dual_value'] = float(dual_value)
                        else:
                            # Single-indexed or scalar constraint
                            row_data = {'0': index, 'dual_value': float(dual_value)}
                        dual_data.append(row_data)
                        total_duals += 1
                except (KeyError, AttributeError):
                    # Dual value not available for this constraint
                    pass

            total_constraints += 1

            # Save to database if we have dual values for this constraint
            if dual_data:
                df = pd.DataFrame(dual_data)
                table_name = f'dual_{constraint_name}'
                df.to_sql(table_name, cnx, if_exists='replace', index=False)

        cnx.commit()

        if total_duals > 0:
            printer.information(f"Added dual values to SQLite ({total_duals} duals from {total_constraints} constraints)")
        else:
            printer.warning(f"No dual values found in model (checked {total_constraints} constraints)")

    except Exception as e:
        printer.error(f"Failed to add dual values: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cnx.close()
