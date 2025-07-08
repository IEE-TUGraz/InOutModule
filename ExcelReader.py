import time

import openpyxl
import pandas as pd
from openpyxl import load_workbook

from printer import Printer

printer = Printer.getInstance()


def __check_LEGOExcel_version(excel_file_path: str, version_specifier: str):
    # Check if the file has the correct version specifier
    wb = openpyxl.load_workbook(excel_file_path)
    for sheet in wb.sheetnames:
        if wb[sheet].cell(row=2, column=3).value != version_specifier:
            printer.error(f"Excel file '{excel_file_path}' does not have the correct version specifier in sheet '{sheet}'. Expected '{version_specifier}' but got '{wb[sheet].cell(row=2, column=3).value}'.")
            printer.error(f"Trying to work with it any way, but this can have unintended consequences!")
    pass


# Function to read generator data
def __read_generator_data(file_path):
    d_generator = pd.read_excel(file_path, skiprows=[0, 1, 2, 4, 5, 6])
    d_generator = d_generator[d_generator["excl"].isnull()]  # Only keep rows that are not excluded (i.e., have no value in the "Excl." column)
    d_generator = d_generator[(d_generator["ExisUnits"] > 0) | (d_generator["EnableInvest"] > 0)]  # Filter out all generators that are not existing and not invest-able
    d_generator = d_generator.set_index('g')
    return d_generator


def __read_non_pivoted_file(excel_file_path: str, version_specifier: str, indices: list[str], has_excl_column: bool, keep_excl_columns: bool = False) -> pd.DataFrame:
    __check_LEGOExcel_version(excel_file_path, version_specifier)
    xls = pd.ExcelFile(excel_file_path)
    data = pd.DataFrame()

    for scenario in xls.sheet_names:  # Iterate through all sheets, i.e., through all scenarios
        df = pd.read_excel(excel_file_path, skiprows=[0, 1, 2, 4, 5, 6], sheet_name=scenario)
        if has_excl_column:
            if not keep_excl_columns:
                df = df[df["excl"].isnull()]  # Only keep rows that are not excluded (i.e., have no value in the "Excl." column)
        else:
            df = df.drop(df.columns[0], axis=1)  # Drop the first column (which is empty)
        df = df.set_index(indices) if len(indices) > 0 else df
        df["scenario"] = scenario

        data = pd.concat([data, df], ignore_index=False)  # Append the DataFrame to the main DataFrame

    return data


def __read_pivoted_file(excel_file_path: str, version_specifier: str, indices: list[str], pivoted_variable_name: str, melt_indices: list[str], has_excl_column: bool, keep_excluded_columns: bool = False) -> pd.DataFrame:
    df = __read_non_pivoted_file(excel_file_path, version_specifier, [], has_excl_column, keep_excluded_columns)

    df = df.melt(id_vars=melt_indices + ["scenario"], var_name=pivoted_variable_name, value_name="value")
    df = df.set_index(indices)
    return df


def get_dGlobal_Scenarios(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dGlobal_Scenarios data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dGlobal_Scenarios
    """
    dGlobal_Scenarios = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["scenarioID"], True, keep_excluded_entries)

    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dGlobal_Scenarios', although no values are converted anyway - please check if this is intended.")

    # Check that there is only one sheet with the name 'Scenario'
    check = dGlobal_Scenarios["scenario"].to_numpy()
    if not (check[0] == check).all():
        raise ValueError(f"There are multiple or falsely named sheets for '{excel_file_path}'. There should only be one sheet with the name 'Scenario', please check the Excel file.")

    return dGlobal_Scenarios


def get_dPower_Hindex(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_Hindex data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_Hindex
    """
    dPower_Hindex = __read_non_pivoted_file(excel_file_path, "v0.1.2", ["p", "rp", "k"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_Hindex', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_Hindex', although no values are converted anyway - please check if this is intended.")

    return dPower_Hindex


def get_dPower_WeightsRP(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_WeightsRP data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_WeightsRP
    """
    dPower_WeightsRP = __read_non_pivoted_file(excel_file_path, "v0.1.3", ["rp"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_WeightsRP', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_WeightsRP', although no values are converted anyway - please check if this is intended.")

    return dPower_WeightsRP


def get_dPower_WeightsK(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_WeightsK data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_WeightsK
    """
    dPower_WeightsK = __read_non_pivoted_file(excel_file_path, "v0.1.3", ["k"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_WeightsK', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_WeightsK', although no values are converted anyway - please check if this is intended.")

    return dPower_WeightsK


def get_dPower_BusInfo(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_BusInfo data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Do not exclude any entries which are marked to be excluded in the Excel file
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_BusInfo
    """
    dPower_BusInfo = __read_non_pivoted_file(excel_file_path, "v0.1.2", ["i"], True, keep_excluded_entries)

    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_BusInfo', although no values are converted anyway - please check if this is intended.")

    return dPower_BusInfo


def get_dPower_Network(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_Network data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Do not exclude any entries which are marked to be excluded in the Excel file
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_Network
    """
    dPower_Network = __read_non_pivoted_file(excel_file_path, "v0.1.1", ["i", "j", "c"], True, keep_excluded_entries)

    return dPower_Network


def get_dPower_Demand(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_Demand data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Skip the conversion of values
    :return: dPower_Demand
    """

    dPower_Demand = __read_pivoted_file(excel_file_path, "v0.1.2", ['rp', 'k', 'i'], 'k', ['rp', 'i', 'dataPackage', 'dataSource', 'id'], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_Demand', although nothing is excluded anyway - please check if this is intended.")

    return dPower_Demand


def get_dPower_ThermalGen(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_ThermalGen data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Do not exclude any entries which are marked to be excluded in the Excel file
    :param do_not_convert_values: Skip the conversion of values
    :return: dPower_thermalGen
    """
    dPower_ThermalGen = __read_non_pivoted_file(excel_file_path, "v0.1.1", ["g"], True, keep_excluded_entries)

    return dPower_ThermalGen


def get_dPower_VRES(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_VRES data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Do not exclude any entries which are marked to be excluded in the Excel file
    :param do_not_convert_values: Skip the conversion of values
    :return: dPower_VRES
    """
    dPower_VRES = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["g"], True, keep_excluded_entries)

    return dPower_VRES


def get_dPower_VRESProfiles(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dPower_VRESProfiles data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dPower_VRES
    """
    dPower_VRESProfiles = __read_pivoted_file(excel_file_path, "v0.1.0", ['rp', 'k', 'g'], 'k', ['rp', 'g', 'dataPackage', 'dataSource', 'id'], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_WeightsK', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_WeightsK', although no values are converted anyway - please check if this is intended.")

    return dPower_VRESProfiles


def get_dData_Sources(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dData_Sources data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dData_Sources
    """
    dData_Sources = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["dataSource"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dData_Sources', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dData_Sources', although no values are converted anyway - please check if this is intended.")

    return dData_Sources


def get_dData_Packages(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False) -> pd.DataFrame:
    """
    Read the dData_Packages data from the Excel file.
    :param excel_file_path: Path to the Excel file
    :param keep_excluded_entries: Unused but kept for compatibility with other functions
    :param do_not_convert_values: Unused but kept for compatibility with other functions
    :return: dData_Packages
    """
    dData_Packages = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["dataPackage"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dData_Packages', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dData_Packages', although no values are converted anyway - please check if this is intended.")

    return dData_Packages


def compare_Excels(source_path: str, target_path: str, dont_check_formatting: bool = False) -> bool:
    """
    Compare two Excel files for differences in formatting and values.
    :param source_path: Path to the source Excel file
    :param target_path: Path to the target Excel file
    :param dont_check_formatting: If True, skip formatting checks
    :return: True if the files are equal, False otherwise
    """
    start_time = time.time()
    source = load_workbook(source_path)
    target = load_workbook(target_path)

    equal = True

    for sheet in source.sheetnames:
        source_sheet = source[sheet]
        if sheet not in target.sheetnames:
            printer.error(f"Sheet '{sheet}' not found in target file '{target_path}'")
            equal = False
            continue
        target_sheet = target[sheet]

        for row in range(1, source_sheet.max_row + 1):
            if not dont_check_formatting:
                if source_sheet.row_dimensions[row].height != target_sheet.row_dimensions[row].height:
                    printer.error(f"Mismatch in row height at {sheet}/row {row}: {source_sheet.row_dimensions[row].height} != {target_sheet.row_dimensions[row].height}")
                    equal = False

            for col in range(1, source_sheet.max_column + 1):
                source_cell = source_sheet.cell(row=row, column=col)
                target_cell = target_sheet.cell(row=row, column=col)

                # Value
                if source_cell.value != target_cell.value:
                    source_value = str(source_cell.value).replace("[", r"\[")  # Required to prevent rich from interpreting brackets as style definitions
                    target_value = str(target_cell.value).replace("[", r"\[")
                    printer.error(f"Mismatch in value at {sheet}/{source_cell.coordinate}: {source_value} != {target_value}")
                    equal = False

                if not dont_check_formatting:
                    # Font
                    for k, v in source_cell.font.__dict__.items():
                        if k == "color" and v is not None:
                            for k2, v2 in v.__dict__.items():
                                if v2 != getattr(target_cell.font.color, k2):
                                    printer.error(f"Mismatch in font color at {sheet}/{source_cell.coordinate}: {v2} != {getattr(target_cell.font.color, k2)}")
                                    equal = False
                        elif getattr(target_cell.font, k) != v:
                            printer.error(f"Mismatch in font property '{k}' at {sheet}/{source_cell.coordinate}: {getattr(target_cell.font, k)} != {v}")
                            equal = False

                    # Fill
                    for k, v in source_cell.fill.__dict__.items():
                        if k == "color" and v is not None:
                            for k2, v2 in v.__dict__.items():
                                if v2 != getattr(target_cell.fill.color, k2):
                                    printer.error(f"Mismatch in fill color at {sheet}/{source_cell.coordinate}: {v2} != {getattr(target_cell.fill.color, k2)}")
                                    equal = False
                        elif getattr(target_cell.fill, k) != v:
                            printer.error(f"Mismatch in fill property '{k}' at {sheet}/{source_cell.coordinate}: {getattr(target_cell.fill, k)} != {v}")
                            equal = False

                    # Number format
                    if source_cell.number_format != target_cell.number_format:
                        printer.error(f"Mismatch in number format at {sheet}/{source_cell.coordinate}: {source_cell.number_format} != {target_cell.number_format}")
                        equal = False

                    # Alignment
                    for k, v in source_cell.alignment.__dict__.items():
                        if getattr(target_cell.alignment, k) != v:
                            printer.error(f"Mismatch in alignment property '{k}' at {sheet}/{source_cell.coordinate}: {getattr(target_cell.alignment, k)} != {v}")
                            equal = False

                    # Comment
                    if source_cell.comment != target_cell.comment:
                        printer.error(f"Mismatch in comment at {sheet}/{source_cell.coordinate}: {source_cell.comment} != {target_cell.comment}")
                        equal = False

                    # Column width
                    if row == 1:  # Only need to check column width for the first row
                        if source_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width != target_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width:
                            printer.error(f"Mismatch in column width at {sheet}/column {col}: {source_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width} != {target_sheet.column_dimensions[openpyxl.utils.get_column_letter(col)].width}")
                            equal = False

    printer.information(f"Compared Excel file '{source_path}' to '{target_path}' in {time.time() - start_time:.2f} seconds")
    return equal
