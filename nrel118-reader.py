import os

import pandas as pd

from printer import Printer

printer = Printer.getInstance()


def read_Power_Inflows(folder_hourly_inflows: str, file_hydro_nondispatchable: str, plexos_export_file: str, maximum_k: str = None) -> pd.DataFrame:
    """
    Reads data from NREL-118 and returns a DataFrame in the LEGO-format.
    :param folder_hourly_inflows: Folder containing hourly inflow data files.
    :param file_hydro_nondispatchable: File containing non-dispatchable hydro inflow data.
    :param plexos_export_file: File containing monthly inflow budgets from Plexos.
    :param maximum_k: Optional maximum k value to read (e.g., "k0240" to read only first 240 time steps).
    :return: DataFrame with Power_Inflows data.
    """

    inflows = pd.DataFrame()

    # Define mapping from monthly timeslices to hourly k values
    days_jan = 31
    days_feb = days_jan + 29  # Leap year
    days_mar = days_feb + 31
    days_apr = days_mar + 30
    days_may = days_apr + 31
    days_jun = days_may + 30
    days_jul = days_jun + 31
    days_aug = days_jul + 31
    days_sep = days_aug + 30
    days_oct = days_sep + 31
    days_nov = days_oct + 30
    days_dec = days_nov + 32  # Includes an additional day

    month_timeslice_to_k = {
        "M1": [f"k{i:>04}" for i in range(1, 24 * days_jan + 1)],  # January
        "M2": [f"k{i:>04}" for i in range(24 * days_jan + 1, 24 * days_feb + 1)],  # February
        "M3": [f"k{i:>04}" for i in range(24 * days_feb + 1, 24 * days_mar + 1)],  # March
        "M4": [f"k{i:>04}" for i in range(24 * days_mar + 1, 24 * days_apr + 1)],  # April
        "M5": [f"k{i:>04}" for i in range(24 * days_apr + 1, 24 * days_may + 1)],  # May
        "M6": [f"k{i:>04}" for i in range(24 * days_may + 1, 24 * days_jun + 1)],  # June
        "M7": [f"k{i:>04}" for i in range(24 * days_jun + 1, 24 * days_jul + 1)],  # July
        "M8": [f"k{i:>04}" for i in range(24 * days_jul + 1, 24 * days_aug + 1)],  # August
        "M9": [f"k{i:>04}" for i in range(24 * days_aug + 1, 24 * days_sep + 1)],  # September
        "M10": [f"k{i:>04}" for i in range(24 * days_sep + 1, 24 * days_oct + 1)],  # October
        "M11": [f"k{i:>04}" for i in range(24 * days_oct + 1, 24 * days_nov + 1)],  # November
        "M12": [f"k{i:>04}" for i in range(24 * days_nov + 1, 24 * days_dec + 1)],  # December
    }

    printer.information(f"Reading hourly inflows from folder: {folder_hourly_inflows}")
    for hourly_inflow_file in os.listdir(folder_hourly_inflows):
        if hourly_inflow_file.endswith(".csv"):
            generator_name = hourly_inflow_file[:-4]  # Remove .csv extension
            file_path = os.path.join(folder_hourly_inflows, hourly_inflow_file)
            df = pd.read_csv(file_path)
            df.rename(columns={"Value": "value", "Values": "value"}, inplace=True)
            df['k'] = [f"k{i:>04}" for i in range(1, len(df) + 1)]  # Get Ks from time steps
            df["g"] = generator_name
            df["dataSource"] = "hydro-hourly-inflows"
            inflows = pd.concat([inflows, df[['k', 'g', 'value', "dataSource"]]], ignore_index=True)

    printer.information(f"Reading monthly fixed hydro inflows from file: {file_hydro_nondispatchable}")
    df_hydro = pd.read_csv(file_hydro_nondispatchable)

    for _, row in df_hydro.iterrows():
        if row["Timeslice"] in month_timeslice_to_k:
            df = pd.DataFrame({
                "g": row["Generator"],
                "k": month_timeslice_to_k[row["Timeslice"]],
                "value": row["Value"]
            })
            df["dataSource"] = "hydro-monthly-fixed-inflows"
            inflows = pd.concat([inflows, df], ignore_index=True)

    printer.information(f"Reading monthly maximum energy budgets from Plexos export file: {plexos_export_file}")
    df_plexos = pd.read_excel(plexos_export_file, sheet_name="Properties")
    df_plexos = df_plexos[df_plexos["property"] == "Max Energy Month"]
    for _, row in df_plexos.iterrows():
        if row["pattern"] in month_timeslice_to_k:
            df = pd.DataFrame({
                "g": row["child_object"],
                "k": month_timeslice_to_k[row["pattern"]],
                "value": [row["value"]] + [0] * (len(month_timeslice_to_k[row["pattern"]]) - 1)
            })
            df["dataSource"] = "plexos-monthly-budgets"
            inflows = pd.concat([inflows, df], ignore_index=True)

    inflows["rp"] = "rp01"  # Single representative period
    inflows["dataPackage"] = "NREL-118-mod"  # Data from NREL-118
    inflows["scenario"] = "ScenarioA"  # Single scenario
    inflows["id"] = None  # Empty ID column
    inflows.set_index(["rp", "k", "g"], inplace=True)

    if maximum_k is not None:
        printer.information(f"Filtering inflows to only include up to k = {maximum_k}.")
        inflows = inflows[inflows.index.get_level_values('k') <= maximum_k]

    printer.success("Done reading inflows.")

    return inflows


def read_Power_VRESProfiles(folder_hourly_solar: str, folder_hourly_wind: str, generator_information_file: str, clip_to_max_1: bool = False, clip_to_min_0: bool = False, maximum_k: str = None) -> pd.DataFrame:
    """
    Reads VRES profile data from NREL-118 and returns a DataFrame in the LEGO-format.
    :param folder_hourly_solar: Folder containing hourly solar profile data files.
    :param folder_hourly_wind: Folder containing hourly wind profile data files.
    :param generator_information_file: File containing generator information (especially capacity factors).
    :param clip_to_max_1: Whether to clip values greater than 1 to 1 (100% capacity factor).
    :param clip_to_min_0: Whether to clip values less than 0 to 0.
    :param maximum_k: Optional maximum k value to read (e.g., "k0240" to read only first 240 time steps).
    :return: DataFrame with Power_VRESProfiles data.
    """

    printer.information(f"Reading generator information from file: {generator_information_file}")
    generatorInfo = pd.read_csv(generator_information_file, usecols=["Generator Name", "Max Capacity (MW)"], sep=";")
    generatorInfo.set_index("Generator Name", inplace=True)

    profiles = pd.DataFrame()

    printer.information(f"Reading solar profiles from folder: {folder_hourly_solar}")
    for solar_file in os.listdir(folder_hourly_solar):
        if solar_file.endswith(".csv"):
            vres_name = solar_file[0:5] + " " + solar_file[5:][:-6].zfill(2)  # Remove "RT.csv" extension, add space between name and number
            file_path = os.path.join(folder_hourly_solar, solar_file)
            df = pd.read_csv(file_path)
            df.rename(columns={"Value": "value", "Values": "value"}, inplace=True)
            df['value'] = df['value'] / float(generatorInfo.at[vres_name, "Max Capacity (MW)"].replace(",", "."))  # Calculate capacity factor
            df['k'] = [f"k{i:>04}" for i in range(1, len(df) + 1)]  # Get Ks from time steps
            df["g"] = vres_name
            df["dataSource"] = "vres-profiles"
            profiles = pd.concat([profiles, df[['k', 'g', 'value', "dataSource"]]], ignore_index=True)

    printer.information(f"Reading wind profiles from folder: {folder_hourly_wind}")
    for wind_file in os.listdir(folder_hourly_wind):
        if wind_file.endswith(".csv"):
            vres_name = wind_file[0:4] + " " + wind_file[4:][:-6].zfill(2)  # Remove "RT.csv" extension, add space between name and number
            file_path = os.path.join(folder_hourly_wind, wind_file)
            df = pd.read_csv(file_path)
            df.rename(columns={"Value": "value", "Values": "value"}, inplace=True)
            df['value'] = df['value'] / float(generatorInfo.at[vres_name, "Max Capacity (MW)"].replace(",", "."))  # Calculate capacity factor
            df['k'] = [f"k{i:>04}" for i in range(1, len(df) + 1)]  # Get Ks from time steps
            df["g"] = vres_name
            df["dataSource"] = "vres-profiles"
            profiles = pd.concat([profiles, df[['k', 'g', 'value', "dataSource"]]], ignore_index=True)

    profiles["rp"] = "rp01"  # Single representative period
    profiles["dataPackage"] = "NREL-118-mod"  # Data from NREL-118
    profiles["scenario"] = "ScenarioA"  # Single scenario
    profiles["id"] = None  # Empty ID column
    profiles.set_index(["rp", "k", "g"], inplace=True)

    # Find values greater than 1 and print a warning
    invalid_values = profiles[profiles['value'] > 1]
    if not invalid_values.empty:
        printer.warning(f"{invalid_values.shape[0]} VRES profiles have values greater than 1 (capacity factor > 100%), printing the biggest 5:")
        invalid_values = invalid_values.sort_values(by='value', ascending=False)
        for index, row in invalid_values.head(5).iterrows():
            printer.warning(f"  rp: {index[0]}, k: {index[1]}, g: {index[2]}, value: {row['value']}")

    if clip_to_max_1:
        printer.information("Clipping VRES profile values to a maximum of 1 (100% capacity factor).")
        profiles['value'] = profiles['value'].clip(upper=1.0)
    else:
        printer.warning("VRES profile values are not clipped to a maximum of 1 (100% capacity factor). This may lead to issues in the model.")

    # Find values less than 0 and print a warning
    invalid_values = profiles[profiles['value'] < 0]
    if not invalid_values.empty:
        printer.warning(f"{invalid_values.shape[0]} VRES profiles have values less than 0 (negative capacity factor), printing the top 5:")
        invalid_values = invalid_values.sort_values(by='value')
        for index, row in invalid_values.head(5).iterrows():
            printer.warning(f"  rp: {index[0]}, k: {index[1]}, g: {index[2]}, value: {row['value']}")

    if clip_to_min_0:
        printer.information("Clipping VRES profile values to a minimum of 0.")
        profiles['value'] = profiles['value'].clip(lower=0.0)
    else:
        printer.warning("VRES profile values are not clipped to a minimum of 0. This may lead to issues in the model.")

    if maximum_k is not None:
        printer.information(f"Filtering VRES profiles to only include up to k = {maximum_k}.")
        profiles = profiles[profiles.index.get_level_values('k') <= maximum_k]

    printer.success("Done reading VRES profiles.")

    return profiles

# ew = ExcelWriter()

# inflows = read_Power_Inflows("../../LEGO-Pyomo2/data/NREL-118/input-files/Hydro", "../../LEGO-Pyomo2/data/NREL-118/additional-files-mti-118/Hydro_nondipatchable.csv", "../../LEGO-Pyomo2/data/NREL-118/plexos-export.xls", "k8784")
# ew.write_Power_Inflows(inflows, ".")

# vres_profiles = read_Power_VRESProfiles("../../LEGO-Pyomo2/data/NREL-118/input-files/RT/Solar", "../../LEGO-Pyomo2/data/NREL-118/input-files/RT/Wind", "../../LEGO-Pyomo2/data/NREL-118/additional-files-mti-118/Generators.csv", True, True, "k8784")
# ew.write_Power_VRESProfiles(vres_profiles, ".")
