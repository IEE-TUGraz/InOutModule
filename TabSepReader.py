import pandas as pd
import yaml
import os
import numpy as np

# read the yaml file with data settings and return the settings as a dictionary
def read_data_settings(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def read_tab_separated_file(file_path: str) -> pd.DataFrame:
    """
    Reads a tab-separated file and returns a DataFrame. 
    
    :param file_path: Path to the tab-separated file.
    :return: DataFrame containing the data from the file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    try:
        data = pd.read_csv(file_path, sep='\t', header=[0], skiprows=[1], encoding='latin1')
    # Drop the last column if it is completely empty or unnamed
        if data.columns[-1][0].startswith('Unnamed'):
            data = data.iloc[:, :-1]

    except Exception as e:
        raise ValueError(f"Error reading the file: {e}")
    
    return data

def build_dummy_df(settings: dict, num_elements: int, ig: str, ig_name: str) -> pd.DataFrame:
    """
    Builds a dummy DataFrame for testing purposes.
    """

    # generate the data fame layout
    # --- Define variables ---
    dataPackage_default = "TestPackage1"
    dataSource_default = "TestSource1"
    id_default = "NaN"
    scenario_default = "ScenarioA"

    # --- Build MultiIndex ---
    rp = ["rp01"] * num_elements
    k = [f"k{str(i).zfill(4)}" for i in range(1, num_elements + 1)]
    g = [ig_name] * num_elements

    multi_index = pd.MultiIndex.from_tuples(
        list(zip(rp, k, g)), names=["rp", "k", ig]
    )

    # --- Initialize DataFrame ---
    df_dummy = pd.DataFrame({
        "dataPackage": dataPackage_default,
        "dataSource": dataSource_default,
        "id": id_default,
        "scenario": scenario_default,
        "value": [None] * len(multi_index)  # empty values
    }, index=multi_index)

    return df_dummy


def get_VRES_profiles(file_path: str, settings: dict) -> pd.DataFrame:
    """
    Reads the VRES profiles from a tab-separated file based on settings.
    
    :param file_path: Path to the tab-separated file.
    :param settings: Dictionary containing settings for reading the file.
    :return: DataFrame containing the VRES profiles.
    """
    df_raw = read_tab_separated_file(os.path.join(file_path, settings["VRES_profiles"]["filename"]))

    df_PV = df_raw[[settings["VRES_profiles"]["column"]]].copy()

    # rename the columns
    df_PV.columns = ["value"]

    # normalise by the module power
    nominal_power = settings["VRES_profiles"]["nominal_power"]
    df_PV["value"] = df_PV["value"] / nominal_power

    # get the minimum value
    #print(df_PV["value"].min())

    # set negative values to zero
    df_PV["value"] = df_PV["value"].clip(lower=0)

    df_PV_sum = aggregate_TS(settings, df_PV, "mean")

    # get the number of elements
    num_elements = df_PV_sum["value"].count()

    df_VRES_profiles = build_dummy_df(settings, num_elements, "g", "PV_rooftop")

    # copy data from df_PV_sum into the values column
    df_VRES_profiles.loc[:, "value"] = df_PV_sum["value"].values

    return df_VRES_profiles

def aggregate_TS(settings, df_PV, type: str = "mean"):
    if settings["aggregation"]["enabled"]:
        # aggregate each steps
        df_PV["invervall_group"] = df_PV.index // settings["aggregation"]["intervall"]

        if type == "mean":
            df_PV_sum = df_PV.groupby("invervall_group", as_index=False)["value"].mean()
        elif type == "sum":
            df_PV_sum = df_PV.groupby("invervall_group", as_index=False)["value"].sum()
        else:
            raise ValueError(f"Unknown aggregation type: {type}")

        # delete last element
        df_PV_sum = df_PV_sum.iloc[:-1]
    else:
        df_PV_sum = df_PV.copy()
    return df_PV_sum

def get_dPower_Demand(file_path: str, settings: dict) -> pd.DataFrame:
    df_raw = read_tab_separated_file(os.path.join(file_path,settings["power_demand"]["filename"]))

    df_demand = df_raw[[settings["power_demand"]["column"]]].copy()
    df_demand.columns = ["value"]

    # calc total demand 
    total_demand = df_demand["value"].sum() / 60 / 1000  # in MWh
    print(f"Total demand in the raw data: {total_demand} MWh")

    df_demand_sum = aggregate_TS(settings, df_demand, "mean")

    num_elements = df_demand_sum["value"].count()

    df_power_demand = build_dummy_df(settings, num_elements, "i", "Node_1")

    df_power_demand.loc[:, "value"] = df_demand_sum["value"].values * 1e-3      # calculate the demand in MW (raw data in kW)

    return df_power_demand

def create_imp_exp_data(length: int) -> pd.DataFrame:
    # generate the data fame layout
    # --- Define variables ---
    scenario_default = "ScenarioA"

    # --- Build MultiIndex ---
    hub = ["External_Grid"] * length
    rp = ["rp01"] * length
    k = [f"k{str(i).zfill(4)}" for i in range(1, length + 1)]
    
    multi_index = pd.MultiIndex.from_tuples(
        list(zip(hub, rp, k)), names=["hub", "rp", "k"]
    )

    # --- Initialize DataFrame ---
    df_imp_exp = pd.DataFrame({
        "ImpExp": 0.8,
        "Price": 40,
        "scenario": scenario_default,
    }, index=multi_index)

    #df_imp_exp.loc[("External_Grid", "rp01", "k0013"), "ImpExp"] = -0.8
    
    return df_imp_exp

def create_consecutive_hindex(length):
      # --- Define variables ---
    scenario_default = "ScenarioA"
    dataPackage = "TestPackage1"
    dataSource = "TestSource1"
    id = "NaN"

    # --- Build MultiIndex ---
    p = [f"h{str(i).zfill(4)}" for i in range(1, length + 1)]
    rp = ["rp01"] * length
    k = [f"k{str(i).zfill(4)}" for i in range(1, length + 1)]
    
    multi_index = pd.MultiIndex.from_tuples(
        list(zip(p, rp, k)), names=["p", "rp", "k"]
    )

    # --- Initialize DataFrame ---
    df_imp_exp = pd.DataFrame({
        "id": id,
        "dataPackage": dataPackage,
        "dataSource": dataSource,
        "scenario": scenario_default,
    }, index=multi_index)

    return df_imp_exp

def create_kWeights(length):
    scenario_default = "ScenarioA"
    dataPackage = "TestPackage1"
    dataSource = "TestSource1"
    id = "NaN"

    # --- Build MultiIndex ---
    k = [f"k{str(i).zfill(4)}" for i in range(1, length + 1)]
    
    multi_index = pd.MultiIndex.from_tuples(
        list(zip(k)), names=["k"]
    )

    # --- Initialize DataFrame ---
    df_k_weights = pd.DataFrame({
        "id": id,
        "pWeight_k": 8760/length,
        "dataPackage": dataPackage,
        "dataSource": dataSource,
        "scenario": scenario_default,
    }, index=multi_index)

    return df_k_weights

# Example usage
if __name__ == "__main__":

    data_folder = os.path.join("data", "rings")
    settings = read_data_settings(os.path.join(data_folder, "DataSettings.yaml"))
    #print(settings)

    df_demand = get_dPower_Demand(data_folder, settings)
    #print(df_demand.head(35))

    #import ExcelReader

    #path = os.path.join("data", "rings_base_example")
    #df_k = ExcelReader.get_dPower_WeightsK(os.path.join(path, "Power_WeightsK.xlsx"))
    #df_hindex = ExcelReader.get_dPower_Hindex(os.path.join(path, "Power_Hindex.xlsx"))

    #print(df_k.head(24))

    #df_test = create_kWeights(24)
    #print(df_test.head(24))
