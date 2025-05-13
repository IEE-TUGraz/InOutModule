import numpy as np
import openpyxl
import pandas as pd

from InOutModule.printer import Printer

printer = Printer.getInstance()


def __check_LEGOExcel_version(excel_file_path: str, version_specifier: str):
    # Check if the file has the correct version specifier
    wb = openpyxl.load_workbook(excel_file_path)
    for sheet in wb.sheetnames:
        if wb[sheet].cell(row=2, column=3).value != version_specifier:
            raise ValueError(f"Excel file '{excel_file_path}' does not have the correct version specifier in sheet '{sheet}'. Expected '{version_specifier}' but got '{wb[sheet].cell(row=2, column=3).value}'.")
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


def get_dPower_Hindex(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False):
    dPower_Hindex = __read_non_pivoted_file(excel_file_path, "v0.1.1", ["p", "rp", "k"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_Hindex', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_Hindex', although no values are converted anyway - please check if this is intended.")

    return dPower_Hindex


def get_dPower_WeightsRP(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False):
    dPower_WeightsRP = __read_non_pivoted_file(excel_file_path, "v0.1.1", ["rp"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_WeightsRP', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_WeightsRP', although no values are converted anyway - please check if this is intended.")

    return dPower_WeightsRP


def get_dPower_WeightsK(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False):
    dPower_WeightsK = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["k"], False, False)

    if keep_excluded_entries:
        printer.warning("'keep_excluded_entries' is set for 'get_dPower_WeightsK', although nothing is excluded anyway - please check if this is intended.")
    if do_not_convert_values:
        printer.warning("'do_not_convert_values' is set for 'get_dPower_WeightsK', although no values are converted anyway - please check if this is intended.")

    return dPower_WeightsK


def get_dPower_BusInfo(excel_file_path: str):
    dPower_BusInfo = __read_non_pivoted_file(excel_file_path, "v0.1.0", ["i"], True)
    return dPower_BusInfo


def get_dPower_Network(excel_file_path: str):
    __check_LEGOExcel_version(excel_file_path, "v0.0.4r")
    dPower_Network = pd.read_excel(excel_file_path, skiprows=[0, 1, 2, 4, 5, 6])
    dPower_Network = dPower_Network[dPower_Network["excl"].isnull()]  # Only keep rows that are not excluded (i.e., have no value in the "Excl." column)

    dPower_Network["pInvestCost"] = dPower_Network["pInvestCost"].fillna(0)
    dPower_Network["pPmax"] *= 1e-3

    dPower_Network = dPower_Network.set_index(['i', 'j', 'c'])
    return dPower_Network


def get_dPower_Demand(excel_file_path: str, keep_excluded_entries: bool = False, do_not_convert_values: bool = False):
    dPower_Demand = __read_pivoted_file(excel_file_path, "v0.1.0", ['rp', 'k', 'i'], 'k', ['rp', 'i', 'dataPackage', 'dataSource', 'id'], False, keep_excluded_entries)

    if not do_not_convert_values:
        dPower_Demand["value"] = dPower_Demand["value"] * 1e-3

    return dPower_Demand


def get_dPower_ThermalGen(excel_file_path: str):
    __check_LEGOExcel_version(excel_file_path, "v0.0.4r")
    dPower_ThermalGen = pd.read_excel(excel_file_path, skiprows=[0, 1, 2, 4, 5, 6])
    dPower_ThermalGen = dPower_ThermalGen[dPower_ThermalGen["excl"].isnull()]  # Only keep rows that are not excluded (i.e., have no value in the "Excl." column)
    dPower_ThermalGen = dPower_ThermalGen.set_index('g')
    dPower_ThermalGen = dPower_ThermalGen[(dPower_ThermalGen["ExisUnits"] > 0) | (dPower_ThermalGen["EnableInvest"] > 0)]  # Filter out all generators that are not existing and not investable

    dPower_ThermalGen['pSlopeVarCostEUR'] = (dPower_ThermalGen['OMVarCost'] * 1e-3 +
                                             dPower_ThermalGen['FuelCost']) / dPower_ThermalGen['Efficiency'] * 1e-3

    dPower_ThermalGen['pInterVarCostEUR'] = dPower_ThermalGen['CommitConsumption'] * 1e-6 * dPower_ThermalGen['FuelCost']
    dPower_ThermalGen['pStartupCostEUR'] = dPower_ThermalGen['StartupConsumption'] * 1e-6 * dPower_ThermalGen['FuelCost']
    dPower_ThermalGen['MaxInvest'] = dPower_ThermalGen.apply(lambda x: 1 if x['EnableInvest'] == 1 and x['ExisUnits'] == 0 else 0, axis=1)
    dPower_ThermalGen['RampUp'] *= 1e-3
    dPower_ThermalGen['RampDw'] *= 1e-3
    dPower_ThermalGen['MaxProd'] *= 1e-3  # TODO: Include EFOR here
    dPower_ThermalGen['MinProd'] *= 1e-3
    dPower_ThermalGen['InvestCostEUR'] = dPower_ThermalGen['InvestCost'] * 1e-3 * dPower_ThermalGen['MaxProd']  # InvestCost is scaled here (1e-3), scaling of MaxProd happens above

    # Fill NaN values with 0 for MinUpTime and MinDownTime
    dPower_ThermalGen['MinUpTime'] = dPower_ThermalGen['MinUpTime'].fillna(0)
    dPower_ThermalGen['MinDownTime'] = dPower_ThermalGen['MinDownTime'].fillna(0)

    # Check that both MinUpTime and MinDownTime are integers and raise error if not
    if not dPower_ThermalGen.MinUpTime.dtype == np.int64:
        raise ValueError("MinUpTime must be an integer for all entries.")
    if not dPower_ThermalGen.MinDownTime.dtype == np.int64:
        raise ValueError("MinDownTime must be an integer for all entries.")
    dPower_ThermalGen['MinUpTime'] = dPower_ThermalGen['MinUpTime'].astype(int)
    dPower_ThermalGen['MinDownTime'] = dPower_ThermalGen['MinDownTime'].astype(int)

    return dPower_ThermalGen


def get_dPower_VRES(excel_file_path: str):
    __check_LEGOExcel_version(excel_file_path, "v0.0.4r")
    dPower_VRES = __read_generator_data(excel_file_path)
    if "MinProd" not in dPower_VRES.columns:
        dPower_VRES['MinProd'] = 0

    dPower_VRES['InvestCostEUR'] = dPower_VRES['InvestCost'] * 1e-3 * dPower_VRES['MaxProd'] * 1e-3
    dPower_VRES['MaxProd'] *= 1e-3
    dPower_VRES['OMVarCost'] *= 1e-3
    return dPower_VRES


def get_dPower_VRESProfiles(excel_file_path: str):
    __check_LEGOExcel_version(excel_file_path, "v0.0.3")
    dPower_VRESProfiles = pd.read_excel(excel_file_path, skiprows=[0, 1, 2, 4, 5, 6])
    dPower_VRESProfiles = dPower_VRESProfiles.drop(dPower_VRESProfiles.columns[0], axis=1)  # Drop the first column (which is empty)
    dPower_VRESProfiles = dPower_VRESProfiles.melt(id_vars=['id', 'rp', 'g', 'dataPackage', 'dataSource'], var_name='k', value_name='Capacity')
    dPower_VRESProfiles = dPower_VRESProfiles.set_index(['rp', 'k', 'g'])
    return dPower_VRESProfiles
