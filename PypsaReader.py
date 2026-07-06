import inspect
import os

import numpy as np
import pandas as pd
import pypsa
import yaml

import pypsa_helper as h
from ExcelWriter import ExcelWriter


class Conversions:
    """Registry of conversion functions for unit transformations."""

    @staticmethod
    def EUR_to_MEUR(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        """Converts € to Mio.€"""
        return val * 1e-6

    @staticmethod
    def MEUR_to_EUR(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        """Converts Mio.€ to €"""
        return val * 1e6

    @staticmethod
    def MW_to_kW(val: pd.Series, df: pd.DataFrame = None) -> pd.Series:
        """Converts MW to kW."""
        return val * 1e3

    @staticmethod
    def V_to_kV(val, df: pd.DataFrame = None) -> pd.Series:
        """Converts V to kV."""
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
        # Only return a decom year if build_year and lifetime is available and build_year is not 0; otherwise return NaN
        mask = df.build_year.notnull() & (df.build_year != 0) & df.lifetime.notnull()
        res = pd.Series(np.nan, index=df.index)
        res[mask] = df.loc[mask, "build_year"] + df.loc[mask, "lifetime"]
        return res

    @staticmethod
    def is_ldes(val: pd.Series, df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Sets binary to 1 if energy-to-power ratio (max_hours) is greater than the threshold in metadata."""
        threshold = metadata.get("Metadata", {}).get("LDES_threshold", 2160)
        return (val > threshold).astype(int)

    @staticmethod
    def line_carrier_to_tec_repr(val, df: pd.DataFrame) -> pd.Series:
        """Maps line carriers to LEGO's technology representations (e.g., DC-OPF for AC lines)."""
        if df.carrier.isnull().all():
            return "DC-OPF"
        else:
            return df.carrier.map({"AC": "DC-OPF", "DC": "TP"})

    @staticmethod
    def total_capacity_to_number_of_units(val, df: pd.DataFrame) -> pd.Series:
        """Calculates the number of units based on total capacity and nominal capacity per unit."""
        # Check if the input value is all NaN or all infinite, and return a default of 100 in that case
        if val.isnull().all() or np.isinf(val).all():
            return 100
        else:
            val = val.fillna(0).replace(
                np.inf, 1000
            )  # Treat NaN as 0 for investment calculation
            p_nom = (
                df.p_nom.fillna(1).replace(np.inf, 100).replace(0, 1)
            )  # Avoid division by zero and treat NaN as 1 for unit calculation
            return (np.ceil(val / p_nom)).astype(int)

    @staticmethod
    def _get_fuel_costs(df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Helper to get fuel costs for each row in the DataFrame based on carrier."""
        fuel_mapping = {}
        meta_config = metadata.get("Metadata", {})
        for key, fuel_info in meta_config.items():
            if (
                isinstance(fuel_info, dict)
                and "filter" in fuel_info
                and "cost" in fuel_info
            ):
                for carrier in fuel_info["filter"]:
                    fuel_mapping[carrier] = fuel_info["cost"]

        # Map carriers to costs; default to NaN if not found
        fuel_costs = df["carrier"].map(fuel_mapping)

        # Performance check: only check for missing carriers if DataFrame isn't empty
        if not fuel_costs.empty:
            missing_carriers = df.loc[fuel_costs.isnull(), "carrier"].unique()
            if len(missing_carriers) > 0:
                print(
                    f"Warning: No fuel cost defined in Metadata for carrier(s): {missing_carriers.tolist()}"
                )

        return fuel_costs

    @staticmethod
    def get_fuel_cost_from_metadata(
        val: pd.Series, df: pd.DataFrame, metadata: dict
    ) -> pd.Series:
        """Retrieves fuel costs based on the carrier and Metadata configuration."""
        return Conversions._get_fuel_costs(df, metadata)

    @staticmethod
    def _get_storage_capacity_costs(df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Helper to get storage capacity costs for each row in the DataFrame based on carrier."""
        cost_mapping = {}
        meta_config = metadata.get("Metadata", {})
        for key, info in meta_config.items():
            if (
                isinstance(info, dict)
                and "filter" in info
                and "default_storage_capacity_cost" in info
            ):
                for carrier in info["filter"]:
                    cost_mapping[carrier] = info["default_storage_capacity_cost"]

        # Map carriers to costs; default to NaN if not found
        costs = df["carrier"].map(cost_mapping)

        return costs

    @staticmethod
    def get_storage_capacity_cost_from_metadata(
        val: pd.Series, df: pd.DataFrame, metadata: dict
    ) -> pd.Series:
        """Retrieves storage capacity costs based on the carrier and Metadata configuration."""
        return Conversions._get_storage_capacity_costs(df, metadata)

    @staticmethod
    def EUR_per_hour_to_MWh_per_hour(
        val: pd.Series, df: pd.DataFrame, metadata: dict
    ) -> pd.Series:
        """Converts costs per hour of thermal generation (e.g. stand_by_cost) to costs per MWh based on the fuel cost specified in the metadata."""
        fuel_costs = Conversions._get_fuel_costs(df, metadata)
        # Result is MWh/h = (EUR/h) / (EUR/MWh)
        # Avoid division by zero, handle NaN/Inf
        with np.errstate(divide="ignore", invalid="ignore"):
            res = val / fuel_costs
        return res.replace([np.inf, -np.inf], 0).fillna(0)

    @staticmethod
    def EUR_to_MWh(val: pd.Series, df: pd.DataFrame, metadata: dict) -> pd.Series:
        """Converts costs to costs per MWh based on the fuel cost specified in the metadata."""
        fuel_costs = Conversions._get_fuel_costs(df, metadata)
        # Result is MWh = EUR / (EUR/MWh)
        with np.errstate(divide="ignore", invalid="ignore"):
            res = val / fuel_costs
        return res.replace([np.inf, -np.inf], 0).fillna(0)

    @staticmethod
    def greater_or_equal_to_one(val: pd.Series) -> pd.Series:
        """Ensures that all values are greater or equal to 1, replacing values less than 1 with 1."""
        return val.apply(lambda x: max(x, 1) if pd.notnull(x) else x)

    @staticmethod
    def pu_to_absolute_and_p_nom_if_nan_or_zero(
        val: pd.Series, df: pd.DataFrame
    ) -> pd.Series:
        val = val * df.p_nom
        return val.where((val.notnull() & (val != 0)), df.p_nom)

    @staticmethod
    def one_if_nan_or_zero(
        val: pd.Series,
    ) -> pd.Series:
        """Replaces NaN or zero values with 1, keeping other values unchanged."""
        return val.where((val.notnull() & (val != 0)), 1)

    @staticmethod
    def bool_to_binary_zero_if_p_nom_is_zero(
        val: pd.Series, df: pd.DataFrame
    ) -> pd.Series:
        """Converts boolean values to binary (0/1) integers and sets to 0, if p_nom is zero."""
        if val.isnull().all():
            return 0
        else:
            # Use infer_objects to explicitly handle the type conversion from object to bool
            val = val.fillna(False).infer_objects(copy=False)
            val = val.where(df.p_nom != 0, 0)  # set to 0 if p_nom is zero
            return val.astype(int)


class NetworkDataExtractor:
    def __init__(
        self,
        network: pypsa.Network,
        config_path: str = None,
        table_definitions_path: str = None,
    ):
        """
        Initializes the extractor with a PyPSA network and configuration files.

        :param network: The PyPSA network instance to extract data from.
        :param config_path: Path to the mapping configuration YAML file.
        :param table_definitions_path: Path to the TableDefinitions XML file.
        """
        self.network = network
        self._conv_params_cache = {}  # Performance: Cache function signatures

        if config_path is None:
            config_path = os.path.join(os.path.dirname(__file__), "pypsa_lego_mapping_config.yaml")

        if table_definitions_path is None:
            table_definitions_path = os.path.join(
                os.path.dirname(__file__), "TableDefinitions.xml"
            )

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        # Initialize ExcelWriter to get definitions for checking/normalization
        self.writer = ExcelWriter(table_definitions_path)
        self.excel_definitions = self.writer.excel_definitions

        self.dataframes = self._extract_dataframes()
        # Normalize and check dataframes (moving checking functionality from testing script)
        self.dataframes = self._normalize_dataframes()

    def _normalize_dataframes(self):
        """Checks and normalizes dataframes to ensure they are ready for LEGO output."""
        normalized_dfs = {}
        for name, df in self.dataframes.items():
            # Only write tables that are defined in TableDefinitions.xml
            table_id = name[1:] if name.startswith("d") else name

            if table_id not in self.excel_definitions:
                print(f"  Skipping {name} (not defined in TableDefinitions.xml)")
                continue

            # Add mandatory 'scenario' column for LEGO format if missing
            if "scenario" not in df.columns:
                df["scenario"] = "ScenarioA"

            # Add 'id' column if missing
            if "id" not in df.columns:
                df["id"] = np.nan

            # Ensure all columns from TableDefinition are present (at least as NaN)
            definition = self.excel_definitions[table_id]
            for col_def in definition.columns:
                if col_def.db_name not in df.columns and col_def.db_name != "NOEXCL":
                    df[col_def.db_name] = np.nan

            normalized_dfs[name] = df

        return normalized_dfs

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
            if "index" in cfg:
                self._apply_indexing(df, source_df, cfg["index"])

            # 5. Normalize for LEGO Format
            # Drop columns that are now in the index to avoid "already exists" error on reset_index
            for idx_name in df.index.names:
                if idx_name in df.columns:
                    df = df.drop(columns=[idx_name])

            # Reduces length of index to less than 450 characters
            # This is only implemented because the LEGO database currently cannot handle more than 450 characters in the index!
            if isinstance(df.index, pd.MultiIndex):
                df.index = pd.MultiIndex.from_frame(
                    df.index.to_frame().astype(str).apply(lambda x: x.str[:449])
                )
            else:
                df.index = df.index.astype(str).str[:449]

            df = df.reset_index()
            df = self._add_scenario_columns(df)

            df_dict[table_name] = df

        return df_dict

    def _resolve_category_filters(self, cfg):
        """Resolves technology filters from Metadata if a category is defined."""
        if "category" not in cfg:
            return

        category = cfg["category"]
        meta_data = self.config.get("Metadata", {})
        meta_cat = meta_data.get(category, {})

        # Combine filters from listed technologies or use direct filter
        cat_filter = meta_cat.get("filter", [])
        if isinstance(cat_filter, (str, int, float)):
            cat_filter = [cat_filter]
        else:
            cat_filter = list(cat_filter)

        for tech in meta_cat.get("technologies", []):
            tech_filter = meta_data.get(tech, {}).get("filter", [])
            if isinstance(tech_filter, list):
                cat_filter.extend(tech_filter)
            else:
                cat_filter.append(tech_filter)

        if cat_filter:
            if "source" not in cfg:
                cfg["source"] = {}
            # Ensure uniqueness and format as query string
            unique_filter = list(set(cat_filter))
            cfg["source"]["filter"] = f"carrier in {unique_filter}"

    def _get_source_df(self, cfg):
        """Retrieves the source DataFrame based on the configuration."""
        src = cfg.get("source")
        if not src:
            return None

        if src["type"] == "attribute":
            source_df = getattr(self.network, src["name"])
            if "filter" in src:
                source_df = source_df.query(src["filter"])
            return source_df
        elif src["type"] == "helper":
            return getattr(h, src["name"])(self.network, cfg)
        return None

    def _map_columns(self, source_df, cfg):
        """Maps PyPSA attributes to LEGO columns using the mapping configuration."""
        column_data = {}
        for lego_col, mapping in cfg.get("mapping", {}).items():
            if isinstance(mapping, dict):
                # Attribute mapping with potential unit conversion or transformation function
                attr = mapping.get("attr")
                if attr and attr in source_df.columns:
                    val = source_df[attr]
                else:
                    # Try to get value as series of NaNs or fixed value
                    val = pd.Series(mapping.get("value", np.nan), index=source_df.index)

                # Priority 1: Named conversion function
                if "conversion" in mapping:
                    conv_name = mapping["conversion"].replace("()", "")
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
                            raise ValueError(
                                f"Error applying conversion '{conv_name}' to column '{lego_col}': {e}"
                            )
                    else:
                        print(
                            f"Warning: Conversion function '{conv_name}' not found in Conversions class."
                        )

                # Priority 3: Simple multiplier factor
                elif "factor" in mapping:
                    val = val * mapping["factor"]

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
            df.index = pd.MultiIndex.from_frame(
                pd.DataFrame(index_data, index=source_df.index)
            )
        elif isinstance(idx_cfg, str):
            # Simple index renaming
            df.index = source_df.index.rename(idx_cfg)
        elif isinstance(idx_cfg, list):
            # Fallback for current list style: assumes columns match LEGO names
            df.index = pd.MultiIndex.from_frame(source_df[idx_cfg]).set_names(idx_cfg)

    def _add_scenario_columns(self, df) -> pd.DataFrame:
        """Adds dataPackage and dataSource columns based on config or defaults."""
        meta = self.config.get("Metadata", {})
        df["dataPackage"] = meta.get("dataPackage", "default-package")
        df["dataSource"] = meta.get("dataSource", "default-source")
        return df

    def get_dataframes(self):
        """Returns the dictionary of extracted and normalized DataFrames."""
        return self.dataframes


def translate_pypsa_to_lego(
    input_directory: str = r"./input_data",
    input_file: str = r"pypsa-model.nc",
    output_directory: str = r"./output_data",
    output_folder_name: str = "LEGO-Model",
):
    """
    Main execution block for converting a PyPSA network to LEGO-formatted Excel files.

    To use this:
    1. Point 'input_directory' and 'input_file' to your .nc PyPSA network.
    2. Set 'output_directory' and 'output_folder_name' for the resulting Excel files.
    3. Run the script with command-line arguments. See: `python PypsaReader.py --help`
    """
    # Define the path to the PyPSA network (similar than implemented in PyPSA-LEGO-Translator_testing.py)
    filepath = os.path.join(input_directory, input_file)

    if not os.path.exists(filepath):
        print(f"Error: File not found at {filepath}")
    else:
        # Load the network
        print(f"Loading PyPSA network from {filepath}...")
        try:
            net = pypsa.Network(filepath)
        except Exception as e:
            print(f"Could not load network: {e}")
            exit(1)

        # Extract data using NetworkDataExtractor
        print("Extracting data into LEGO format...")
        extractor = NetworkDataExtractor(net)
        dfs = extractor.get_dataframes()

        # Initialize ExcelWriter
        writer = ExcelWriter()

        # Define output directory
        output_dir = os.path.join(output_directory, output_folder_name)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # Writing Excel files (filtering/checking functionality is now inside NetworkDataExtractor)
        print("Writing Excel files...")
        for name, df in dfs.items():
            table_id = name[1:] if name.startswith("d") else name

            print(f"  Writing {name} to {output_dir}...")
            try:
                writer._write_Excel_from_definition(df, output_dir, table_id)
            except Exception as e:
                print(f"  Error writing {name}: {e}")

        print("\nConversion complete. Output files are in:", output_dir)


if __name__ == "__main__":
    import argparse
    from rich_argparse import RichHelpFormatter

    parser = argparse.ArgumentParser(
        description="Convert a PyPSA network file to LEGO formatted Excel files.",
        formatter_class=RichHelpFormatter,
    )
    parser.add_argument(
        "inputDirectory",
        type=str,
        help="Path to the folder containing the PyPSA .nc network file.",
    )
    parser.add_argument(
        "inputFile",
        type=str,
        help="Name of the PyPSA .nc network file.",
    )
    parser.add_argument(
        "outputDirectory",
        type=str,
        help="Path to the folder where the LEGO output folder should be created.",
    )
    parser.add_argument(
        "outputFolderName",
        type=str,
        help="Name of the output folder for the generated LEGO Excel files.",
    )
    args = parser.parse_args()

    translate_pypsa_to_lego(
        input_directory=args.inputDirectory,
        input_file=args.inputFile,
        output_directory=args.outputDirectory,
        output_folder_name=args.outputFolderName,
    )
