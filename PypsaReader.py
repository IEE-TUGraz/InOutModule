import pypsa
import pandas as pd
import pypsa_helper as h
import numpy as np
import os

class NetworkDataExtractor:
    def __init__(self, network: pypsa.Network):
        self.network = network
        self.columns = {

            "dPower_BusInfo": ['excl', 'id', 'z', 'pBusBaseV', 'pBusMaxV', 'pBusMinV', 'pBusB',
                                'pBusG', 'pBus_pf', 'YearCom', 'YearDecom', 'lat', 'lon', 'zoi',
                                'dataPackage', 'dataSource'],
            "dPower_Network": ['excl', 'id', 'pRline', 'pXline', 'pBcline', 'pAngle', 'pRatio',
                                'pPmax', 'pEnableInvest', 'pFOMCost', 'pInvestCost', 'pTecRepr',
                                'YearCom', 'YearDecom', 'dataPackage', 'dataSource'],
            "dPower_ThermalGen": ['excl', 'id', 'tec', 'i', 'ExisUnits', 'MaxProd', 'MinProd', 'RampUp',
                                    'RampDw', 'MinUpTime', 'MinDownTime', 'Qmax', 'Qmin', 'InertiaConst',
                                    'FuelCost', 'Efficiency', 'CommitConsumption', 'OMVarCost',
                                    'StartupConsumption', 'EFOR', 'EnableInvest', 'InvestCost',
                                    'FirmCapCoef', 'CO2Emis', 'YearCom', 'YearDecom', 'lat', 'long',
                                    'dataPackage', 'dataSource', 'pSlopeVarCostEUR', 'pInterVarCostEUR',
                                    'pStartupCostEUR', 'MaxInvest', 'InvestCostEUR'],
            "dPower_VRESProfiles": ['Capacity'],
            "dPower_VRES": ['excl', 'id', 'tec', 'i', 'ExisUnits', 'MaxProd', 'EnableInvest',
                            'MaxInvest', 'InvestCost', 'OMVarCost', 'FirmCapCoef', 'Qmax', 'Qmin',
                            'InertiaConst', 'YearCom', 'YearDecom', 'lat', 'lon', 'dataPackage',
                            'dataSource', 'MinProd', 'InvestCostEUR'],
            "dPower_Storage": ['tec', 'i', 'ExisUnits', 'MaxProd', 'MinProd', 'MaxCons', 'DisEffic',
                                'ChEffic', 'Qmax', 'Qmin', 'InertiaConst', 'MinReserve', 'IniReserve',
                                'IsHydro', 'OMVarCost', 'EnableInvest', 'MaxInvest', 'InvestCostPerMW',
                                'InvestCostPerMWh', 'Ene2PowRatio', 'ReplaceCost', 'ShelfLife',
                                'FirmCapCoef', 'CDSF_alpha', 'CDSF_beta', 'PPName', 'YearCom',
                                'YearDecom', 'lat', 'long', 'pOMVarCostEUR', 'InvestCostEUR'], 
            "dPower_RoR": ['tec', 'i', 'ExisUnits', 'MaxProd', 'MinProd', 'MaxCons', 'DisEffic',
                            'ChEffic', 'Qmax', 'Qmin', 'InertiaConst', 'MinReserve', 'IniReserve',
                            'IsHydro', 'OMVarCost', 'EnableInvest', 'MaxInvest', 'InvestCostPerMW',
                            'InvestCostPerMWh', 'Ene2PowRatio', 'ReplaceCost', 'ShelfLife',
                            'FirmCapCoef', 'CDSF_alpha', 'CDSF_beta', 'PPName', 'YearCom',
                            'YearDecom', 'lat', 'long', 'InvestCostEUR'],             
            "dPower_Demand": ['Capacity'],
            "dPower_Inflows": ['Inflow'],
        }


        self.component_definitions = {
            "dPower_BusInfo": {
                "source": lambda net: net.buses[net.buses["carrier"] == "AC"],
                "index": lambda Buses: Buses.index.rename("i"),
                "z": lambda Buses: Buses["country"] ,
                "pBusBaseV": lambda Buses: Buses["v_nom"],
                "pBusMaxV": lambda Buses: 1.1,
                "pBusMinV": lambda Buses: 0.9,
                "lat": lambda Buses: Buses["y"],
                "lon": lambda Buses: Buses["x"],
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
                "index": lambda df: df["id"].rename("g"),
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
            "dPower_VRES": {
                "source": lambda net: h.prepare_renewable_generators(net),
                "index": lambda df: df["id"].rename("g"),
                "tec": lambda df: df["carrier"],
                "i": lambda df: df["bus"],
                "MaxProd": lambda df: df["max_prod"],
                "enableinvest": lambda df: df["enable_invest"],
                "MaxInvest": lambda df: df["p_nom_max"],
                "InvestCost": lambda df: df["capital_cost"],
                "OMVarCost": lambda df: df["marginal_cost"]
            },
            "dPower_RoR": {
                "source": lambda net: h.prepare_ror_generators(net),
                "tec": lambda df: df["carrier"],
                "index": lambda df: df["id"].rename("g"),
                "i": lambda df: df["bus"],
                "MaxProd": lambda df: df["max_prod"],
                "MinProd": lambda df: df["min_prod"],
                "DisEffic": lambda df: df["discharge"],
                "IsHydro": lambda df: df["is_hydro"],
                "OMVarCost": lambda df: df["marginal_cost"],
                "EnableInvest": lambda df: df["enable_invest"],
                "MaxInvest": lambda df: df["p_nom_max"],
                "InvestCostPerMW": lambda df: df["capital_cost"]
            },
            "dPower_Storage": {
                "source": lambda net: h.prepare_storage_units(net),
                "index": lambda df: df["id"].rename("g"),
                "tec": lambda df: df["carrier"],
                "i": lambda df: df["bus"],
                "MaxProd": lambda df: df["max_prod"],
                "MinProd": lambda df: df["min_prod"],
                "DisEffic": lambda df: df["discharge"],
                "ChEffic": lambda df: df["charge"],
                "IniReserve": lambda df: df["ini_reserve"],
                "IsHydro": lambda df: df["is_hydro"],
                "OMVarCost": lambda df: df["marginal_cost"],
                "EnableInvest": lambda df: df["enable_invest"],
                "MaxInvest": lambda df: df["p_nom_max"],
                "InvestCostPerMWh": lambda df: df["capital_cost"],
                "Ene2PowRatio": lambda df: df["max_hours"],
                "ShelfLife": lambda df: df["lifetime"]
            },
            "dPower_Inflows": {
                "source": lambda net: h.prepare_inflow_profiles(net),
                "rp": lambda df: df["rp"], 
                "g": lambda df: df["g"],
                "k": lambda df: df["k"],
                "Inflow": lambda df: df["Inflow"],
                "index": lambda df: pd.MultiIndex.from_frame(df[["rp","k", "g"]])
            },
            "dPower_Demand": {
                "source": lambda net: h.prepare_demand_profiles(net),
                "rp": lambda df: df["rp"], 
                "g": lambda df: df["g"],
                "k": lambda df: df["k"],
                "Demand": lambda df: df["Demand"],
                "index": lambda df: pd.MultiIndex.from_frame(df[["rp", "k", "g"]])
            }
        }

        self.dataframes = self._extract_dataframes()
        # add empty columns
        self.dataframes = self._add_empty_columns()
        # reorder columns
        self.dataframes = self._reorder_columns()
       
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

                # Drop the columns used in the index if they exist in the DataFrame
                if isinstance(index_values, pd.MultiIndex):
                    df = df.drop(columns=[col for col in index_values.names if col in df.columns], errors="ignore")
                elif isinstance(index_values, pd.Index):
                    if index_values.name in df.columns:
                        df = df.drop(columns=[index_values.name], errors="ignore")

                # df.index.name = None  # Optional: remove index name


            else:
                df = df.reset_index(drop=True)

            df_dict[name] = df

        return df_dict

    def _add_empty_columns(self):
        for name, df in self.dataframes.items():
            for col in self.columns[name]:
                if col not in df.columns:
                    df[col] = np.nan
        return self.dataframes
    
    def _reorder_columns(self):
        for name, df in self.dataframes.items():
            if name in self.columns:
                cols = self.columns[name]
                # Reorder the DataFrame columns
                df = df.reindex(columns=cols)
                # Update the DataFrame in the dictionary
                self.dataframes[name] = df
        return self.dataframes
    
    def get_dataframes(self):
        return self.dataframes

filepath = os.path.join(os.path.dirname(__file__), "..", "pypsa-eur/resources/test/networks/base_s_39_elec_1year.nc")
net = pypsa.Network(filepath)
extractor = NetworkDataExtractor(net)
dfs = extractor.get_dataframes()
for name, df in dfs.items():
    print(f"DataFrame: {name}")
    print(df.head())
    print("\n")
