import pypsa
import pandas as pd
import pypsa_helper as h
import numpy as np
import os

class NetworkDataExtractor:
    def __init__(self, network: pypsa.Network):
        self.network = network

        self.component_definitions = {
            "dPower_BusInfo": {
                "source": lambda net: net.buses[net.buses["carrier"] == "AC"],
                "id": lambda Buses: Buses.index,
                "z": lambda Buses: Buses["country"] ,
                "pBusBaseV": lambda Buses: Buses["v_nom"],
                "pBusMaxV": lambda Buses: 1.1,
                "pBusMinV": lambda Buses: 0.9,
                "lat": lambda Buses: Buses["y"],
                "lon": lambda Buses: Buses["x"],
                # You can add more column definitions here
            },
            "dPower_Network": {
                "source": lambda net: pd.concat([h.prepare_ac_lines(net),
                                                 h.prepare_dc_links(net)
                                                 ], ignore_index=True),
                "index": lambda df: pd.MultiIndex.from_frame(
                    df[["bus0", "bus1", "name"]].rename(columns={"bus0": "i", "bus1": "j", "name": "c"})
                ).set_names(["i", "j", "c"]),
                "pRline": lambda df: df["r"],
                "pXline": lambda df: df["x"],
                "pBcline": lambda df: df["b"],
                "pMax": lambda df: df["pmax"] 
                
            },
            "dPower_ThermalGen": {
                "source": lambda net: h.prepare_thermal_generators(net),
                "index": lambda df: df["id"],
                "tec": lambda df: df["carrier"],
                "i": lambda df: df["bus"],
                "MaxProd": lambda df: df["max_prod"],
                "MinProd": lambda df: df["min_prod"],
                "RampUp": lambda df: df["ramp_up"],
                "RampDown": lambda df: df["ramp_down"],
                "pStartupCostEUR": lambda df: df["start_up_cost"],
                "EnableInvest": lambda df: df["enable_invest"],
                "InvestCost": lambda df: df["capital_cost"],
                "OMVarCost": lambda df: df["marginal_cost"],
            },
            "dPower_VRESProfiles": {
                "source": lambda net: h.prepare_renewable_profiles(net),
                "Capacity": lambda df: df["Capacity"],
                "index": lambda df: pd.MultiIndex.from_frame(
                    df[["generator_id", "snapshot"]].rename(columns={"generator_id": "g", "snapshot": "k"}))
            },


        }

        self.dataframes = self._extract_dataframes()

    def _extract_dataframes(self):
        df_dict = {}

        for name, config in self.component_definitions.items():
            source_df = config["source"](self.network)

            column_data = {
                column_name: transform(source_df)
                for column_name, transform in config.items()
                if column_name not in ("source", "index")
            }

            df = pd.DataFrame(column_data)

            # Set custom index if defined
            if "index" in config:
                index_values = config["index"](source_df)
                df.index = index_values
                df.index.name = None  # Optional: prevent index name from showing up

            else:
                df = df.reset_index(drop=True)

            df_dict[name] = df

        return df_dict



    def get_dataframes(self):
        return self.dataframes

filepath = os.path.join(os.path.dirname(__file__), "..", "pypsa-eur/resources/test/networks/base_s_39_elec_1year.nc")
net = pypsa.Network(filepath)
extractor = NetworkDataExtractor(net)
dfs = extractor.get_dataframes()
print(dfs["dPower_VRESProfiles"].head())
