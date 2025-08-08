import pandas as pd
import yaml
import os

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


def get_VRES_profiles(file_path: str, settings: dict) -> pd.DataFrame:
    """
    Reads the VRES profiles from a tab-separated file based on settings.
    
    :param file_path: Path to the tab-separated file.
    :param settings: Dictionary containing settings for reading the file.
    :return: DataFrame containing the VRES profiles.
    """
    df_raw = read_tab_separated_file(os.path.join(data_folder, settings["VRES_profiles"]["filename"]))

    # todos: add the time column,
    # adapt to correct LEGO format
    # include the temporal aggregation

    df_PV = df_raw[[settings["VRES_profiles"]["column"]]].copy()

    # rename the columns
    df_PV.columns = ["vres"]


    if settings["aggregation"]["enabled"]:
        # aggregate each steps
        df_PV["invervall_group"] = df_PV.index // settings["aggregation"]["intervall"]
        df_PV_sum = df_PV.groupby("invervall_group", as_index=False)["vres"].sum()
    else:
        df_PV_sum = df_PV.copy()

    return df_PV_sum

# Example usage
if __name__ == "__main__":

    data_folder = os.path.join("data", "rings")

    settings = read_data_settings(os.path.join(data_folder, "DataSettings.yaml"))
    #print(settings)

    df_PV = get_VRES_profiles(data_folder, settings)

    print(df_PV.head(150))

