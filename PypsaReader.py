import pypsa
import pandas as pd
import pypsa_helper as h
import numpy as np
import os
import yaml
import inspect

class Conversions:
    """Registry of conversion functions for unit transformations."""
    @staticmethod
    def EUR_to_MEUR(val, row=None):
        return val * 1e-6

    @staticmethod
    def MEUR_to_EUR(val, row=None):
        return val * 1e6

    @staticmethod
    def MW_to_kW(val, row=None):
        return val * 1e3

    @staticmethod
    def V_to_kV(val, row=None):
        return val * 1e-3

    @staticmethod
    def EUR_per_MVA_to_MEUR(val, df):
        """Calculates total cost in MEUR: (EUR/unit) * capacity * 1e-6."""
        capacity = df.s_nom
        return val * capacity * 1e-6

    @staticmethod
    def bool_to_binary(val):
        """Converts boolean values to binary (0/1) integers."""
        return val.astype(int)

    @staticmethod
    def year_and_lifetime_to_year_decom(val, df):
        """Calculates decommissioning year based on commissioning year and lifetime."""
        # Only return a decom year if build_year and lifetime is available; otherwise return NaN
        if df.build_year.isnull().all() and df.lifetiem.isnull().all():
            return np.nan
        else:
            return df.build_year + df.lifetime

    @staticmethod
    def line_carrier_to_tec_repr(val, df):
        if df.carrier.isnull().all():
            return 'DC-OPF'
        else:
            return df.carrier.map({'AC': 'DC-OPF', 'DC': 'TP'})


class NetworkDataExtractor:
    def __init__(self, network: pypsa.Network, config_path: str = None):
        self.network = network
        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "mapping_config.yaml")
        
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # The expected columns for each table (used for reordering and filling empties)
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

        self.dataframes = self._extract_dataframes()
        # add empty columns
        self.dataframes = self._add_empty_columns()
        # reorder columns
        self.dataframes = self._reorder_columns()

    def _get_unit_factor(self, pypsa_unit: str, lego_unit: str) -> float:
        """Calculates conversion factor based on metric prefixes (e.g., MW to kW)."""
        if not pypsa_unit or not lego_unit or pypsa_unit == lego_unit:
            return 1.0
        
        # Power of 10 mapping for metric prefixes
        prefixes = {'T': 12, 'G': 9, 'M': 6, 'k': 3, '': 0, 'm': -3, 'u': -6, 'n': -9}
        
        def split_unit(u):
            if len(u) > 1 and u[0] in prefixes and (u[1:] in ['W', 'V', 'EUR', 'Wh', 'g', 'l']):
                return u[0], u[1:]
            return '', u

        p_pre, p_base = split_unit(pypsa_unit)
        l_pre, l_base = split_unit(lego_unit)

        # Specific handling for EUR/MEUR if they are treated as base units
        if pypsa_unit == "MEUR" and lego_unit == "EUR": return 1e6
        if pypsa_unit == "EUR" and lego_unit == "MEUR": return 1e-6

        if p_base != l_base:
            return 1.0

        return 10** (prefixes[p_pre] - prefixes[l_pre])
       
    def _extract_dataframes(self):
        df_dict = {}

        for table_name, cfg in self.config.items():
            # 1. Get Source Data
            src = cfg['source']
            if src['type'] == 'attribute':
                source_df = getattr(self.network, src['name'])
                if 'filter' in src:
                    source_df = source_df.query(src['filter'])
            elif src['type'] == 'helper':
                source_df = getattr(h, src['name'])(self.network)
            else:
                continue

            # 2. Process Mapping
            column_data = {}
            for lego_col, mapping in cfg['mapping'].items():
                if isinstance(mapping, dict):
                    # Attribute mapping with potential unit conversion or transformation function
                    attr = mapping.get('attr')
                    if attr and attr in source_df.columns:
                        val = source_df[attr]
                    else:
                        # Try to get value as series of NaNs or fixed value
                        val = pd.Series(mapping.get('value', np.nan), index=source_df.index)
                    
                    # Priority 1: Named conversion function
                    if 'conversion' in mapping:
                        conv_name = mapping['conversion'].replace('()', '')
                        if hasattr(Conversions, conv_name):
                            conv_func = getattr(Conversions, conv_name)
                            # Check function signature
                            sig = inspect.signature(conv_func)
                            params = list(sig.parameters.values())
                            
                            if len(params) >= 2:
                                # Vectorized call: (Series, DataFrame)
                                val = conv_func(val, source_df)
                            else:
                                # Vectorized call: (Series)
                                val = conv_func(val)
                        else:
                            print(f"Warning: Conversion function '{conv_name}' not found in Conversions class.")
                    
                    # Priority 2: Explicit unit strings
                    elif 'pypsa_unit' in mapping and 'lego_unit' in mapping:
                        factor = self._get_unit_factor(mapping['pypsa_unit'], mapping['lego_unit'])
                        val = val * factor
                    
                    # Priority 3: Simple multiplier factor
                    elif 'factor' in mapping:
                        val = val * mapping['factor']
                        
                    column_data[lego_col] = val
                elif isinstance(mapping, (int, float)):
                    # Static value
                    column_data[lego_col] = mapping
                else:
                    # Direct attribute string mapping
                    if mapping in source_df.columns:
                        column_data[lego_col] = source_df[mapping]
                    else:
                        column_data[lego_col] = np.nan

                # Todo: Add warning if the column defined in mapping is not available in the LEGO columns!!!

            df = pd.DataFrame(column_data)

            # 3. Handle Indexing
            if 'index' in cfg:
                # Logic from original PypsaReader for indexing
                if table_name == "dPower_BusInfo":
                    df.index = source_df.index.rename("i")
                elif table_name == "dPower_Network":
                    df.index = pd.MultiIndex.from_frame(
                        source_df[["bus0", "bus1", "name"]].rename(columns={"bus0": "i", "bus1": "j", "name": "c"})
                    ).set_names(["i", "j", "c"])
                elif table_name == "dPower_ThermalGen":
                    df.index = source_df["id"].rename("g")
                elif table_name == "dPower_VRESProfiles":
                    df.index = pd.MultiIndex.from_frame(
                        source_df[["generator_id", "k"]].rename(columns={"generator_id": "g"})
                    ).set_names(["g", "k"])
                elif table_name in ["dPower_VRES", "dPower_RoR", "dPower_Storage"]:
                    df.index = source_df["id"].rename("g")
                elif table_name in ["dPower_Inflows", "dPower_Demand"]:
                    df.index = pd.MultiIndex.from_frame(source_df[["rp", "k", "g"]])

            df_dict[table_name] = df

        return df_dict

    def _add_empty_columns(self):
        for name, df in self.dataframes.items():
            if name in self.columns:
                for col in self.columns[name]:
                    if col not in df.columns:
                        df[col] = np.nan
        return self.dataframes
    
    def _reorder_columns(self):
        for name, df in self.dataframes.items():
            if name in self.columns:
                cols = self.columns[name]
                df = df.reindex(columns=cols)
                self.dataframes[name] = df
        return self.dataframes
    
    def get_dataframes(self):
        return self.dataframes


if __name__ == "__main__":
    filepath = os.path.join(os.path.dirname(__file__), "..", "pypsa-eur/resources/test/networks/base_s_39_elec_1year.nc")
    if os.path.exists(filepath):
        net = pypsa.Network(filepath)
        extractor = NetworkDataExtractor(net)
        dfs = extractor.get_dataframes()
        for name, df in dfs.items():
            print(f"DataFrame: {name}")
            print(df.head())
            print("\n")
    else:
        print(f"Test file not found: {filepath}")
