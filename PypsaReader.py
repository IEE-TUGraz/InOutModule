import inspect
import os

import numpy as np
import pandas as pd
import pypsa
import yaml

import pypsa_helper as h


class Conversions:
    """Registry of conversion functions for unit transformations."""

    @staticmethod
    def EUR_to_MEUR(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        """Converts € to Mio.€"""
        return val * 1e-6

    @staticmethod
    def MEUR_to_EUR(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        """Converts Mio.€ to € """
        return val * 1e6

    @staticmethod
    def MW_to_kW(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        return val * 1e3

    @staticmethod
    def V_to_kV(val, df: pd.DataFrame = None) -> pd.Series:
        return val * 1e-3

    @staticmethod
    def EUR_per_MVA_to_MEUR(val, df: pd.DataFrame = None) -> pd.Series:
        """Calculates total cost in MEUR: (EUR/unit) * capacity * 1e-6."""
        capacity = df.s_nom
        return val * capacity * 1e-6

    @staticmethod
    def pu_to_absolute(val, df: pd.DataFrame) -> pd.Series:
        """Converts per-unit values to absolute values using the base value from the DataFrame."""
        return val * df.p_nom

    @staticmethod
    def bool_to_binary(val, df: pd.DataFrame = None) -> pd.Series:
        """Converts boolean values to binary (0/1) integers."""
        if val.isnull().all():
            return 0
        else:
            # Use infer_objects to explicitly handle the type conversion from object to bool
            val = val.fillna(False).infer_objects(copy=False)
            return val.astype(int)

    @staticmethod
    def year_and_lifetime_to_year_decom(val, df: pd.DataFrame) -> pd.Series:
        """Calculates decommissioning year based on commissioning year and lifetime."""
        # Only return a decom year if build_year and lifetime is available; otherwise return NaN
        if df.build_year.isnull().all() and df.lifetime.isnull().all():
            return np.nan
        else:
            return df.build_year + df.lifetime

    @staticmethod
    def line_carrier_to_tec_repr(val, df: pd.DataFrame) -> pd.Series:
        if df.carrier.isnull().all():
            return 'DC-OPF'
        else:
            return df.carrier.map({'AC': 'DC-OPF', 'DC': 'TP'})

    @staticmethod
    def total_capacity_to_number_of_units(val, df: pd.DataFrame) -> pd.Series:
        """Calculates the number of units based on total capacity and nominal capacity per unit."""
        # Check if the input value is all NaN or all infinite, and return a default of 100 in that case
        if val.isnull().all() or np.isinf(val).all():
            return 100
        else:
            val = val.fillna(0).replace(np.inf, 1000)  # Treat NaN as 0 for investment calculation
            p_nom = df.p_nom.fillna(1).replace(np.inf, 100).replace(0, 1)  # Avoid division by zero and treat NaN as 1 for unit calculation
            return (np.ceil(val / p_nom)).astype(int)

    @staticmethod
    def _get_fuel_costs(df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Helper to get fuel costs for each row in the DataFrame based on carrier."""
        fuel_mapping = {}
        meta_config = metadata.get('Metadata', {})
        for key, fuel_info in meta_config.items():
            if isinstance(fuel_info, dict) and 'filter' in fuel_info and 'cost' in fuel_info:
                for carrier in fuel_info['filter']:
                    fuel_mapping[carrier] = fuel_info['cost']

        # Map carriers to costs; default to NaN if not found
        fuel_costs = df['carrier'].map(fuel_mapping)

        # Performance check: only check for missing carriers if DataFrame isn't empty
        if not fuel_costs.empty:
            missing_carriers = df.loc[fuel_costs.isnull(), 'carrier'].unique()
            if len(missing_carriers) > 0:
                print(f"Warning: No fuel cost defined in Metadata for carrier(s): {missing_carriers.tolist()}")

        return fuel_costs

    @staticmethod
    def EUR_per_hour_to_MWh_per_hour(val: pd.Series, df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Converts costs per hour of thermal generation (e.g. stand_by_cost) to costs per MWh based on the fuel cost specified in the metadata."""
        fuel_costs = Conversions._get_fuel_costs(df, metadata)
        # Result is MWh/h = (EUR/h) / (EUR/MWh)
        # Avoid division by zero, handle NaN/Inf
        with np.errstate(divide='ignore', invalid='ignore'):
            res = val / fuel_costs
        return res.replace([np.inf, -np.inf], 0).fillna(0)

    @staticmethod
    def EUR_to_MWh(val: pd.Series, df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Converts costs to costs per MWh based on the fuel cost specified in the metadata."""
        fuel_costs = Conversions._get_fuel_costs(df, metadata)
        # Result is MWh = EUR / (EUR/MWh)
        with np.errstate(divide='ignore', invalid='ignore'):
            res = val / fuel_costs
        return res.replace([np.inf, -np.inf], 0).fillna(0)


class NetworkDataExtractor:
    def __init__(self, network: pypsa.Network, config_path: str = None):
        self.network = network
        self._conv_params_cache = {}  # Performance: Cache function signatures
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
            "dPower_VRESProfiles": ['value'],
            "dPower_VRES": ['excl', 'id', 'tec', 'i', 'ExisUnits', 'MaxProd', 'EnableInvest',
                            'MaxInvest', 'InvestCost', 'OMVarCost', 'FirmCapCoef', 'Qmax', 'Qmin',
                            'InertiaConst', 'YearCom', 'YearDecom', 'lat', 'lon', 'dataPackage',
                            'dataSource', 'MinProd', 'InvestCostEUR'],
            "dPower_Storage": ['tec', 'i', 'ExisUnits', 'MaxProd', 'MinProd', 'MaxCons', 'DisEffic',
                               'ChEffic', 'Qmax', 'Qmin', 'InertiaConst', 'MinReserve', 'IniReserve',
                               'IsHydro', 'OMVarCost', 'EnableInvest', 'MaxInvest', 'InvestCostPerMW',
                               'InvestCostPerMWh', 'Ene2PowRatio', 'ReplaceCost', 'ShelfLife',
                               'FirmCapCoef', 'CDSF_alpha', 'CDSF_beta', 'PPName', 'YearCom',
                               'YearDecom', 'lat', 'long', 'pOMVarCostEUR', 'InvestCostEUR', 'dataPackage', 'dataSource'],
            "dPower_Demand": ['value'],
            "dPower_Inflows": ['value'],
        }

        self.dataframes = self._extract_dataframes()
        # add empty columns
        self.dataframes = self._add_empty_columns()
        # reorder columns
        self.dataframes = self._reorder_columns()


    def _extract_dataframes(self):
        """Extracts and transforms data from the PyPSA network into LEGO DataFrames."""
        df_dict = {}

        for table_name, cfg in self.config.items():
            if table_name == "Metadata":
                continue

            # 1. Resolve Filters from Category
            self._resolve_category_filters(cfg)

            # 2. Get Source Data
            source_df = self._get_source_df(cfg)
            if source_df is None:
                continue

            # 3. Process Column Mapping
            df = self._map_columns(source_df, cfg)

            # 4. Handle Indexing
            if 'index' in cfg:
                self._apply_indexing(df, source_df, cfg['index'])

            # 5. Normalize for LEGO Format
            df = self._add_scenario_columns(df)

            df_dict[table_name] = df

        return df_dict


    def _resolve_category_filters(self, cfg):
        """Resolves technology filters from Metadata if a category is defined."""
        if 'category' not in cfg:
            return

        category = cfg['category']
        meta_data = self.config.get('Metadata', {})
        meta_cat = meta_data.get(category, {})

        # Combine filters from listed technologies or use direct filter
        cat_filter = meta_cat.get('filter', [])
        if isinstance(cat_filter, (str, int, float)):
            cat_filter = [cat_filter]
        else:
            cat_filter = list(cat_filter)

        for tech in meta_cat.get('technologies', []):
            tech_filter = meta_data.get(tech, {}).get('filter', [])
            if isinstance(tech_filter, list):
                cat_filter.extend(tech_filter)
            else:
                cat_filter.append(tech_filter)

        if cat_filter:
            if 'source' not in cfg:
                cfg['source'] = {}
            # Ensure uniqueness and format as query string
            unique_filter = list(set(cat_filter))
            cfg['source']['filter'] = f"carrier in {unique_filter}"


    def _get_source_df(self, cfg):
        """Retrieves the source DataFrame based on the configuration."""
        src = cfg.get('source')
        if not src:
            return None

        if src['type'] == 'attribute':
            source_df = getattr(self.network, src['name'])
            if 'filter' in src:
                source_df = source_df.query(src['filter'])
            return source_df
        elif src['type'] == 'helper':
            return getattr(h, src['name'])(self.network, cfg)
        return None


    def _map_columns(self, source_df, cfg):
        """Maps PyPSA attributes to LEGO columns using the mapping configuration."""
        column_data = {}
        for lego_col, mapping in cfg.get('mapping', {}).items():
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

                        # Performance Improvement: Cache function parameter counts
                        if conv_name not in self._conv_params_cache:
                            sig = inspect.signature(conv_func)
                            self._conv_params_cache[conv_name] = len(sig.parameters)

                        num_params = self._conv_params_cache[conv_name]

                        try:
                            if num_params == 3:
                                # Vectorized call: (Series, DataFrame, config)
                                val = conv_func(val, source_df, self.config)
                            elif num_params == 2:
                                # Vectorized call: (Series, DataFrame)
                                val = conv_func(val, source_df)
                            else:
                                # Vectorized call: (Series)
                                val = conv_func(val)
                        except Exception as e:
                            raise ValueError(f"Error applying conversion '{conv_name}' to column '{lego_col}': {e}")
                    else:
                        print(f"Warning: Conversion function '{conv_name}' not found in Conversions class.")

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
        return pd.DataFrame(column_data)

    @staticmethod
    def _apply_indexing(df, source_df, idx_cfg):
        """Applies the indexing logic to the DataFrame."""
        if isinstance(idx_cfg, dict):
            # MultiIndex from specified columns/attributes
            index_data = {}
            for lego_idx, pypsa_source in idx_cfg.items():
                if pypsa_source in source_df.columns:
                    index_data[lego_idx] = source_df[pypsa_source]
                elif pypsa_source == "index":
                    index_data[lego_idx] = source_df.index
                else:
                    index_data[lego_idx] = np.nan
            df.index = pd.MultiIndex.from_frame(pd.DataFrame(index_data, index=source_df.index))
        elif isinstance(idx_cfg, str):
            # Simple index renaming
            df.index = source_df.index.rename(idx_cfg)
        elif isinstance(idx_cfg, list):
            # Fallback for current list style: assumes columns match LEGO names
            df.index = pd.MultiIndex.from_frame(source_df[idx_cfg]).set_names(idx_cfg)


    def _add_scenario_columns(self, df) -> pd.DataFrame:
        """Adds dataPackage and dataSource columns based on config or defaults."""
        meta = self.config.get('Metadata', {})
        df['dataPackage'] = meta.get('dataPackage', 'default-package')
        df['dataSource'] = meta.get('dataSource', 'default-source')
        return df


    def _add_empty_columns(self):
        for name, df in self.dataframes.items():
            # Add scenario column
            if 'scenario' not in df.columns:
                df['scenario'] = 'ScenarioA'

            # Add id column if missing
            if 'id' not in df.columns:
                df['id'] = np.nan

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
    filepath = os.path.join(r"C:\BeSt\PyPSA-LEGO-Translator\scigrid-de.nc")
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
