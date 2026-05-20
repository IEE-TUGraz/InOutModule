import concurrent.futures
import copy
import os
import typing
import warnings
from pathlib import Path
from typing import Optional, Self, Literal

import numpy as np
import pandas as pd
import tsam.timeseriesaggregation as tsam

import ExcelReader
from InOutModule import Utilities
from printer import Printer

printer = Printer.getInstance()


class CaseStudy:
    # Lists of dataframes based on their dependencies - every table should only be present in one of these lists
    rpk_dependent_dataframes: list[str] = ["dPower_Demand",
                                           "dPower_Hindex",
                                           "dPower_ImportExport",
                                           "dPower_Inflows",
                                           "dPower_VRESProfiles"]
    rp_only_dependent_dataframes: list[str] = ["dPower_WeightsRP"]
    k_only_dependent_dataframes: list[str] = ["dPower_WeightsK"]
    non_time_dependent_dataframes: list[str] = ["dPower_BusInfo",
                                                "dPower_Network",
                                                "dPower_Storage",
                                                "dPower_ThermalGen",
                                                "dPower_VRES"]
    non_dependent_dataframes: list[str] = ["dGlobal_Parameters",
                                           "dGlobal_Scenarios",
                                           "dPower_Parameters"]

    # Subsets and supersets of the above lists
    rp_dependent_dataframes: list[str] = rpk_dependent_dataframes + rp_only_dependent_dataframes
    k_dependent_dataframes: list[str] = rpk_dependent_dataframes + k_only_dependent_dataframes
    scenario_dependent_dataframes: list[str] = rpk_dependent_dataframes + rp_only_dependent_dataframes + k_only_dependent_dataframes + non_time_dependent_dataframes

    def __init__(self,
                 data_folder: str | Path,
                 do_not_scale_units: bool = False,
                 do_not_merge_single_node_buses: bool = False,
                 do_not_filter_unused_scenarios: bool = False,
                 parallel_read: bool = True,
                 n_jobs: int = 4,
                 global_parameters_file: str = "Global_Parameters.xlsx", dGlobal_Parameters: dict = None,
                 global_scenarios_file: str = "Global_Scenarios.xlsx", dGlobal_Scenarios: pd.DataFrame = None,
                 power_parameters_file: str = "Power_Parameters.xlsx", dPower_Parameters: dict = None,
                 power_businfo_file: str = "Power_BusInfo.xlsx", dPower_BusInfo: pd.DataFrame = None,
                 power_network_file: str = "Power_Network.xlsx", dPower_Network: pd.DataFrame = None,
                 power_thermalgen_file: str = "Power_ThermalGen.xlsx", dPower_ThermalGen: pd.DataFrame = None,
                 power_vres_file: str = "Power_VRES.xlsx", dPower_VRES: pd.DataFrame = None,
                 power_demand_file: str = "Power_Demand.xlsx", dPower_Demand: pd.DataFrame = None,
                 power_inflows_file: str = "Power_Inflows.xlsx", dPower_Inflows: pd.DataFrame = None,
                 power_vresprofiles_file: str = "Power_VRESProfiles.xlsx", dPower_VRESProfiles: pd.DataFrame = None,
                 power_storage_file: str = "Power_Storage.xlsx", dPower_Storage: pd.DataFrame = None,
                 power_weightsrp_file: str = "Power_WeightsRP.xlsx", dPower_WeightsRP: pd.DataFrame = None,
                 power_weightsk_file: str = "Power_WeightsK.xlsx", dPower_WeightsK: pd.DataFrame = None,
                 power_hindex_file: str = "Power_Hindex.xlsx", dPower_Hindex: pd.DataFrame = None,
                 power_importexport_file: str = "Power_ImportExport.xlsx", dPower_ImportExport: pd.DataFrame = None,
                 clip_method: str = "none", clip_value: float = 0):
        self.data_folder = str(data_folder) if str(data_folder).endswith("/") else str(data_folder) + "/"
        self.do_not_scale_units = do_not_scale_units
        self.do_not_merge_single_node_buses = do_not_merge_single_node_buses
        self.do_not_filter_unused_scenarios = do_not_filter_unused_scenarios

        # === SEQUENTIAL READS ===
        if dGlobal_Parameters is not None:
            self.dGlobal_Parameters = dGlobal_Parameters
        else:
            self.global_parameters_file = global_parameters_file
            self.dGlobal_Parameters = self.get_dGlobal_Parameters()

        if dGlobal_Scenarios is not None:
            self.dGlobal_Scenarios = dGlobal_Scenarios
        else:
            self.global_scenarios_file = global_scenarios_file
            if not os.path.exists(self.data_folder + self.global_scenarios_file):
                printer.warning(f"Executing without 'Global_Scenarios' (since no file was found at '{self.data_folder + self.global_scenarios_file}').")

                # Create dataframe for only one Scenario
                dGlobal_Scenarios = pd.DataFrame({"excl": np.nan, "id": np.nan, "scenarioID": ["ScenarioA"], "relativeWeight": [1], "comments": np.nan, "scenario": ["Scenarios"]})
                dGlobal_Scenarios = dGlobal_Scenarios.set_index("scenarioID")

                self.dGlobal_Scenarios = dGlobal_Scenarios
            else:
                self.dGlobal_Scenarios = ExcelReader.get_Global_Scenarios(self.data_folder + self.global_scenarios_file)

        if dPower_Parameters is not None:
            self.dPower_Parameters = dPower_Parameters
        else:
            self.power_parameters_file = power_parameters_file
            self.dPower_Parameters = self.get_dPower_Parameters()

        # === PARALLEL READS ===
        tasks = []  # List of (attribute_name, function, args_tuple)

        # Define file paths
        self.power_businfo_file = power_businfo_file
        self.power_network_file = power_network_file
        self.power_demand_file = power_demand_file
        self.power_hindex_file = power_hindex_file
        self.power_weightsk_file = power_weightsk_file

        # Add independent tasks
        if dPower_BusInfo is None:
            tasks.append(("dPower_BusInfo", ExcelReader.get_Power_BusInfo, (self.data_folder + self.power_businfo_file,)))
        else:
            self.dPower_BusInfo = dPower_BusInfo

        if dPower_Network is None:
            tasks.append(("dPower_Network", ExcelReader.get_Power_Network, (self.data_folder + self.power_network_file,)))
        else:
            self.dPower_Network = dPower_Network

        if dPower_Demand is None:
            tasks.append(("dPower_Demand", ExcelReader.get_Power_Demand, (self.data_folder + self.power_demand_file,)))
        else:
            self.dPower_Demand = dPower_Demand

        if dPower_Hindex is None:
            tasks.append(("dPower_Hindex", ExcelReader.get_Power_Hindex, (self.data_folder + self.power_hindex_file,)))
        else:
            self.dPower_Hindex = dPower_Hindex

        if dPower_WeightsK is None:
            tasks.append(("dPower_WeightsK", ExcelReader.get_Power_WeightsK, (self.data_folder + self.power_weightsk_file,)))
        else:
            self.dPower_WeightsK = dPower_WeightsK

        # Add conditional tasks (dependent on dPower_Parameters)
        if self.dPower_Parameters["pEnableThermalGen"]:
            self.power_thermalgen_file = power_thermalgen_file
            if dPower_ThermalGen is None:
                tasks.append(("dPower_ThermalGen", ExcelReader.get_Power_ThermalGen, (self.data_folder + self.power_thermalgen_file,)))
            else:
                self.dPower_ThermalGen = dPower_ThermalGen

        if self.dPower_Parameters["pEnableVRES"]:
            self.power_vres_file = power_vres_file
            if dPower_VRES is None:
                tasks.append(("dPower_VRES", ExcelReader.get_Power_VRES, (self.data_folder + self.power_vres_file,)))
            else:
                self.dPower_VRES = dPower_VRES

            if dPower_VRESProfiles is None and os.path.isfile(self.data_folder + power_vresprofiles_file):
                self.power_vresprofiles_file = power_vresprofiles_file
                tasks.append(("dPower_VRESProfiles", ExcelReader.get_Power_VRESProfiles, (self.data_folder + self.power_vresprofiles_file,)))
            else:
                self.dPower_VRESProfiles = dPower_VRESProfiles

        if self.dPower_Parameters["pEnableStorage"]:
            self.power_storage_file = power_storage_file
            if dPower_Storage is None:
                tasks.append(("dPower_Storage", ExcelReader.get_Power_Storage, (self.data_folder + self.power_storage_file,)))
            else:
                self.dPower_Storage = dPower_Storage

        if self.dPower_Parameters["pEnableVRES"] or self.dPower_Parameters["pEnableStorage"]:
            if dPower_Inflows is None and os.path.isfile(self.data_folder + power_inflows_file):
                self.power_inflows_file = power_inflows_file
                tasks.append(("dPower_Inflows", ExcelReader.get_Power_Inflows, (self.data_folder + self.power_inflows_file,)))
            else:
                self.dPower_Inflows = dPower_Inflows

        if self.dPower_Parameters["pEnablePowerImportExport"]:
            self.power_importexport_file = power_importexport_file
            if dPower_ImportExport is None:
                tasks.append(("dPower_ImportExport", ExcelReader.get_Power_ImportExport, (self.data_folder + self.power_importexport_file,)))
            else:
                self.dPower_ImportExport = dPower_ImportExport
        else:
            self.dPower_ImportExport = None

        # --- Execute Tasks (Parallel or Sequential) ---
        if parallel_read and len(tasks) > 0:
            num_workers = min(n_jobs, len(tasks))
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                future_to_attr = {executor.submit(task[1], *task[2]): task[0] for task in tasks}

                for future in concurrent.futures.as_completed(future_to_attr):
                    attr_name = future_to_attr[future]
                    try:
                        result_df = future.result()
                        setattr(self, attr_name, result_df)
                    except Exception as exc:
                        printer.error(f"Error reading for '{attr_name}': {exc}")
                        raise exc
        else:
            for attr_name, func, args in tasks:
                try:
                    setattr(self, attr_name, func(*args))
                except Exception as exc:
                    printer.error(f"Error reading for '{attr_name}': {exc}")
                    raise exc

        # === SEQUENTIAL DEPENDENTS ===
        if dPower_WeightsRP is not None:
            self.dPower_WeightsRP = dPower_WeightsRP
        else:
            self.power_weightsrp_file = power_weightsrp_file
            # Calculate dPower_WeightsRP from Hindex
            dPower_WeightsRPs = []
            for scenario in self.dPower_Hindex['scenario'].unique().tolist():
                # Count occurences of each value in column 'rp' of dPower_Hindex
                dPower_WeightsRP_scenario = pd.DataFrame(self.dPower_Hindex[self.dPower_Hindex['scenario'] == scenario].reset_index()['rp'].value_counts().sort_index())
                dPower_WeightsRP_scenario = dPower_WeightsRP_scenario.rename(columns={'count': 'pWeight_rp'})
                dPower_WeightsRP_scenario['scenario'] = scenario  # Add scenario ID

                # Add other columns with default values
                dPower_WeightsRP_scenario['id'] = np.nan
                dPower_WeightsRP_scenario['dataPackage'] = np.nan
                dPower_WeightsRP_scenario['dataSource'] = np.nan

                dPower_WeightsRPs.append(dPower_WeightsRP_scenario)

            dPower_WeightsRP = pd.concat(dPower_WeightsRPs, ignore_index=False)

            if os.path.exists(self.data_folder + self.power_weightsrp_file):  # Compare with given file if it exists
                self.dPower_WeightsRP = ExcelReader.get_Power_WeightsRP(self.data_folder + self.power_weightsrp_file)

                calculated = dPower_WeightsRP.reset_index().set_index(["rp", "scenario"])
                fromFile = self.dPower_WeightsRP.reset_index().set_index(["rp", "scenario"])

                # Normalize both to sum to 1 for comparison
                calc_norm = calculated['pWeight_rp'] / calculated['pWeight_rp'].sum()
                file_norm = fromFile['pWeight_rp'] / fromFile['pWeight_rp'].sum()
                # Align indices and fill missing with 0 for comparison
                combined = pd.concat([calc_norm, file_norm], axis=1, keys=['calculated', 'fromFile']).fillna(0)
                diff_mask = ~np.isclose(combined['calculated'], combined['fromFile'])
                if diff_mask.any():
                    printer.warning(f"Values for 'pWeight_rp' in `{self.data_folder + self.power_weightsrp_file}` do not match the calculated values based on `{self.power_hindex_file}`. Please check if this is intended, using the file `{self.data_folder + self.power_weightsrp_file}` instead of the calculated values.")
                    # Print all differing lines
                    diffs = combined[diff_mask]
                    printer.warning("Differing entries (index -> calculated | fromFile):\n" + diffs.to_string())
            else:  # Use calculated dPower_WeightsRP otherwise
                printer.warning(f"Executing without 'Power_WeightsRP' (since no file was found at '{self.data_folder + self.power_weightsrp_file}').")
                self.dPower_WeightsRP = dPower_WeightsRP

        if not self.do_not_filter_unused_scenarios:
            self.dGlobal_Scenarios = self.dGlobal_Scenarios[self.dGlobal_Scenarios['relativeWeight'] != 0]  # Drop rows in dGlobal_Scenarios where relativeWeight is 0
            if len(self.dGlobal_Scenarios) == 0:
                raise ValueError("No scenarios are present in the 'Global_Scenarios' table. Please check if the file exists and contains valid data.")
            elif len(self.dGlobal_Scenarios) == 1:
                self.filter_scenario(self.dGlobal_Scenarios.index[0], inplace=True)  # Filter case study to only include actually present scenarios

        self.rpTransitionMatrixAbsolute, self.rpTransitionMatrixRelativeTo, self.rpTransitionMatrixRelativeFrom = self.get_rpTransitionMatrices(clip_method=clip_method, clip_value=clip_value)

        if not do_not_merge_single_node_buses:
            self.merge_single_node_buses()

        self.power_scaling_factor = self.dGlobal_Parameters["pPowerScalingFactor"]
        self.cost_scaling_factor = self.dGlobal_Parameters["pCostScalingFactor"]
        self.reactive_power_scaling_factor = 1e-3  # MVar to kVar conversion factor
        self.angle_to_rad_scaling_factor = np.pi / 180

        if not do_not_scale_units:
            self.scale_CaseStudy()

    def copy(self):
        new_self = copy.deepcopy(self)
        return new_self

    def equal_to(self, cs: typing.Self) -> bool:
        """
        Check if this CaseStudy is equal to another CaseStudy, checking all dataframes for equality.
        :param cs: Other CaseStudy to compare to
        :return: True if equal, False otherwise
        """
        all_equal = True
        for df in (self.rpk_dependent_dataframes + self.rp_only_dependent_dataframes + self.k_only_dependent_dataframes + self.non_time_dependent_dataframes + self.non_dependent_dataframes):
            if hasattr(self, df) and hasattr(cs, df):
                self_df = getattr(self, df)
                cs_df = getattr(cs, df)

                if type(self_df) is pd.DataFrame and type(cs_df) is pd.DataFrame:
                    if not self_df.equals(cs_df):
                        printer.error(f"DataFrame '{df}' is not equal.")
                        all_equal = False
                else:
                    if self_df != cs_df:
                        printer.error(f"Attribute '{df}' is not equal.")
                        all_equal = False
            else:
                printer.error(f"Attribute '{df}' is missing in one of the CaseStudies.")
                all_equal = False

        return all_equal

    def scale_CaseStudy(self):
        self.scale_dPower_Parameters()
        self.scale_dPower_Network()
        self.scale_dPower_Demand()

        if self.dPower_Parameters["pEnableThermalGen"]:
            self.scale_dPower_ThermalGen()

        if hasattr(self, "dPower_Inflows") and self.dPower_Inflows is not None:
            self.scale_dPower_Inflows()

        if hasattr(self, "dPower_VRESProfiles") and self.dPower_VRESProfiles is not None:
            self.scale_dPower_VRESProfiles()

        if self.dPower_Parameters["pEnableVRES"]:
            self.scale_dPower_VRES()

        if self.dPower_Parameters["pEnableStorage"]:
            self.scale_dPower_Storage()

        if self.dPower_Parameters["pEnablePowerImportExport"]:
            self.scale_dPower_ImportExport()

    def remove_scaling(self):
        self.power_scaling_factor = 1 / self.power_scaling_factor
        self.cost_scaling_factor = 1 / self.cost_scaling_factor
        self.angle_to_rad_scaling_factor = 1 / self.angle_to_rad_scaling_factor

        self.scale_CaseStudy()

        self.power_scaling_factor = 1 / self.power_scaling_factor
        self.cost_scaling_factor = 1 / self.cost_scaling_factor
        self.angle_to_rad_scaling_factor = 1 / self.angle_to_rad_scaling_factor

    def scale_dPower_Parameters(self):
        self.dPower_Parameters["pSBase"] *= self.power_scaling_factor
        self.dPower_Parameters["pENSCost"] *= self.cost_scaling_factor / self.power_scaling_factor
        self.dPower_Parameters["pLOLCost"] *= self.cost_scaling_factor / self.power_scaling_factor

        self.dPower_Parameters["pMaxAngleDCOPF"] *= self.angle_to_rad_scaling_factor  # Convert angle from degrees to radians

    def scale_dPower_Network(self):
        self.dPower_Network["pInvestCost"] = self.dPower_Network["pInvestCost"].fillna(0)
        self.dPower_Network["pPmax"] *= self.power_scaling_factor

    def scale_dPower_Demand(self):
        self.dPower_Demand["value"] *= self.power_scaling_factor

    def scale_dPower_ThermalGen(self):
        self.dPower_ThermalGen['EFOR'] = self.dPower_ThermalGen['EFOR'].fillna(0)  # Fill NaN values with 0 for EFOR

        # Only FuelCost is adjusted by efficiency (OMVarCost is not), then both are scaled by the cost_scaling_factor / power_scaling_factor
        self.dPower_ThermalGen['pSlopeVarCostEUR'] = (self.dPower_ThermalGen['OMVarCost'] + self.dPower_ThermalGen['FuelCost'] / self.dPower_ThermalGen['Efficiency']) * (self.cost_scaling_factor / self.power_scaling_factor)

        # Calculate interVar- and startup-costs in EUR, and then scale by cost_scaling_factor
        self.dPower_ThermalGen['pInterVarCostEUR'] = self.dPower_ThermalGen['CommitConsumption'] * self.dPower_ThermalGen['FuelCost'] * self.cost_scaling_factor
        self.dPower_ThermalGen['pStartupCostEUR'] = self.dPower_ThermalGen['StartupConsumption'] * self.dPower_ThermalGen['FuelCost'] * self.cost_scaling_factor

        self.dPower_ThermalGen['MaxInvest'] = self.dPower_ThermalGen.apply(lambda x: 1 if x['EnableInvest'] == 1 and x['ExisUnits'] == 0 else 0, axis=1)
        self.dPower_ThermalGen['RampUp'] *= self.power_scaling_factor
        self.dPower_ThermalGen['RampDw'] *= self.power_scaling_factor
        self.dPower_ThermalGen['MaxProd'] *= self.power_scaling_factor * (1 - self.dPower_ThermalGen['EFOR'])
        self.dPower_ThermalGen['MinProd'] *= self.power_scaling_factor * (1 - self.dPower_ThermalGen['EFOR'])
        self.dPower_ThermalGen['InvestCostEUR'] = self.dPower_ThermalGen['InvestCost'] * (self.cost_scaling_factor / self.power_scaling_factor) * self.dPower_ThermalGen['MaxProd']  # InvestCost is scaled here, scaling of MaxProd happens above

        # Fill NaN values with 0 for MinUpTime and MinDownTime
        self.dPower_ThermalGen['MinUpTime'] = self.dPower_ThermalGen['MinUpTime'].fillna(0)
        self.dPower_ThermalGen['MinDownTime'] = self.dPower_ThermalGen['MinDownTime'].fillna(0)

        # Check that both MinUpTime and MinDownTime are integers and raise error if not
        if not self.dPower_ThermalGen.MinUpTime.dtype == np.int64:
            raise ValueError("MinUpTime must be an integer for all entries.")
        if not self.dPower_ThermalGen.MinDownTime.dtype == np.int64:
            raise ValueError("MinDownTime must be an integer for all entries.")
        self.dPower_ThermalGen['MinUpTime'] = self.dPower_ThermalGen['MinUpTime'].astype('int64')
        self.dPower_ThermalGen['MinDownTime'] = self.dPower_ThermalGen['MinDownTime'].astype('int64')

        self.dPower_ThermalGen['Qmin'] = self.dPower_ThermalGen['Qmin'].fillna(0) * self.reactive_power_scaling_factor
        self.dPower_ThermalGen['Qmax'] = self.dPower_ThermalGen['Qmax'].fillna(0) * self.reactive_power_scaling_factor

    def scale_dPower_Inflows(self):
        # Allow only positive inflows
        if (self.dPower_Inflows["value"] < 0).any():
            negative_values = self.dPower_Inflows[self.dPower_Inflows["value"] < 0]
            raise ValueError(f"Inflows contains negative values:\n{negative_values}")

        self.dPower_Inflows["value"] *= self.power_scaling_factor

    def scale_dPower_VRESProfiles(self):
        # Allow only positive capacity factors
        if (self.dPower_VRESProfiles["value"] < 0).any():
            negative_values = self.dPower_VRESProfiles[self.dPower_VRESProfiles["value"] < 0]
            raise ValueError(f"VRES_Profiles contains negative values:\n{negative_values}")

    def scale_dPower_VRES(self):
        if "MinProd" not in self.dPower_VRES.columns:
            self.dPower_VRES['MinProd'] = 0

        self.dPower_VRES['InvestCostEUR'] = self.dPower_VRES['InvestCost'] * (self.cost_scaling_factor / self.power_scaling_factor) * self.dPower_VRES['MaxProd'] * self.power_scaling_factor
        self.dPower_VRES['MaxProd'] *= self.power_scaling_factor
        self.dPower_VRES['OMVarCost'] *= (self.cost_scaling_factor / self.power_scaling_factor)

        self.dPower_VRES['Qmin'] = self.dPower_VRES['Qmin'].fillna(0) * self.reactive_power_scaling_factor
        self.dPower_VRES['Qmax'] = self.dPower_VRES['Qmax'].fillna(0) * self.reactive_power_scaling_factor

    def scale_dPower_Storage(self):
        self.dPower_Storage['IniReserve'] = self.dPower_Storage['IniReserve'].fillna(0)
        self.dPower_Storage['MinReserve'] = self.dPower_Storage['MinReserve'].fillna(0)
        self.dPower_Storage['MinProd'] = self.dPower_Storage["MinProd"].fillna(0)
        self.dPower_Storage['pOMVarCostEUR'] = self.dPower_Storage['OMVarCost'] * (self.cost_scaling_factor / self.power_scaling_factor)
        self.dPower_Storage['InvestCostEUR'] = self.dPower_Storage['MaxProd'] * self.power_scaling_factor * (self.dPower_Storage['InvestCostPerMW'] + self.dPower_Storage['InvestCostPerMWh'] * self.dPower_Storage['Ene2PowRatio']) * (self.cost_scaling_factor / self.power_scaling_factor)
        self.dPower_Storage['MaxProd'] *= self.power_scaling_factor
        self.dPower_Storage['MaxCons'] *= self.power_scaling_factor

        self.dPower_Storage['Qmin'] = self.dPower_Storage['Qmin'].fillna(0) * self.reactive_power_scaling_factor
        self.dPower_Storage['Qmax'] = self.dPower_Storage['Qmax'].fillna(0) * self.reactive_power_scaling_factor

        # Check if any DisEffic or ChEffic is nan, if so, raise an error
        if self.dPower_Storage['DisEffic'].isna().any() or self.dPower_Storage['ChEffic'].isna().any():
            raise ValueError("DisEffic and ChEffic in 'Power_Storage.xlsx' must not contain NaN values. Please check the data.")

    def scale_dPower_ImportExport(self):
        self.dPower_ImportExport["ImpExpMinimum"] *= self.power_scaling_factor
        self.dPower_ImportExport["ImpExpMaximum"] *= self.power_scaling_factor
        self.dPower_ImportExport["ImpExpPrice"] *= self.cost_scaling_factor / self.power_scaling_factor

    def get_dGlobal_Parameters(self):
        file_path = self.data_folder + self.global_parameters_file
        version_spec = "v0.1.0"
        fail_on_wrong_version = False

        try:
            xls = pd.ExcelFile(file_path, engine="calamine")
        except FileNotFoundError:
            printer.error(f"File not found: {file_path}")
            raise

        # Check all sheets for version
        for sheet in xls.sheet_names:
            if sheet.startswith("~"):
                continue
            ExcelReader.check_LEGOExcel_version(xls, sheet, version_spec, file_path, fail_on_wrong_version)

        # Read global parameters from Excel
        dGlobal_Parameters = pd.read_excel(xls, skiprows=[0, 1])
        dGlobal_Parameters = dGlobal_Parameters.drop(dGlobal_Parameters.columns[0], axis=1)
        dGlobal_Parameters = dGlobal_Parameters.set_index('Solver Options')

        self.yesNo_to_bool(dGlobal_Parameters, ['pEnableRMIP'])

        # Transform to make it easier to access values
        dGlobal_Parameters = dGlobal_Parameters.drop(dGlobal_Parameters.columns[1:], axis=1)  # Drop all columns but "Value" (rest is just for information in the Excel)
        dGlobal_Parameters = dict({(parameter_name, parameter_value["Value"]) for parameter_name, parameter_value in dGlobal_Parameters.iterrows()})  # Transform into dictionary

        return dGlobal_Parameters

    def get_dPower_Parameters(self):
        file_path = self.data_folder + self.power_parameters_file
        version_spec = "v0.2.0"
        fail_on_wrong_version = False

        try:
            xls = pd.ExcelFile(file_path, engine="calamine")
        except FileNotFoundError:
            printer.error(f"File not found: {file_path}")
            raise

        # Check all sheets for version
        for sheet in xls.sheet_names:
            if sheet.startswith("~"):
                continue
            ExcelReader.check_LEGOExcel_version(xls, sheet, version_spec, file_path, fail_on_wrong_version)

        dPower_Parameters = pd.read_excel(xls, skiprows=[0, 1])
        dPower_Parameters = dPower_Parameters.drop(dPower_Parameters.columns[0], axis=1)
        dPower_Parameters = dPower_Parameters.dropna(how="all")
        dPower_Parameters = dPower_Parameters.set_index('General')

        self.yesNo_to_bool(dPower_Parameters, ['pEnableChDisPower', 'pFixStInterResToIniReserve', 'pEnableSoftLineLoadLimits', 'pEnableThermalGen', 'pEnableVRES', 'pEnableStorage', 'pEnablePowerImportExport', 'pEnableSOCP'])

        # Transform to make it easier to access values
        dPower_Parameters = dPower_Parameters.drop(dPower_Parameters.columns[1:], axis=1)  # Drop all columns but "Value" (rest is just for information in the Excel)
        dPower_Parameters = dict({(parameter_name, parameter_value["Value"]) for parameter_name, parameter_value in dPower_Parameters.iterrows()})  # Transform into dictionary

        return dPower_Parameters

    @staticmethod
    def yesNo_to_bool(df: pd.DataFrame, columns_to_be_changed: list[str]):
        for column in columns_to_be_changed:
            match df.loc[column, "Value"]:
                case "Yes":
                    df.loc[column, "Value"] = 1
                case "No":
                    df.loc[column, "Value"] = 0
                case _:
                    raise ValueError(f"Value for {column} must be either 'Yes' or 'No'.")
        return df

    @staticmethod
    def get_connected_buses(connection_matrix, bus: str):
        connected_buses = []
        stack = [bus]
        while stack:
            current_bus = stack.pop()
            connected_buses.append(current_bus)

            connected_to_current_bus = [multiindex[0] for multiindex in connection_matrix.loc[current_bus][connection_matrix.loc[current_bus] == True].index.tolist()]
            for node in connected_to_current_bus:
                if node not in connected_buses and node not in stack:
                    stack.append(node)

        connected_buses.sort()
        return connected_buses

    def merge_single_node_buses(self, inplace: bool = True) -> typing.Optional[typing.Self]:
        """
        Merge all buses that are only connected via single-node connections (i.e., "SN" technical representation) into one bus.
        :param inplace: Whether to perform the operation inplace or return a new CaseStudy object.
        :return: The modified CaseStudy object if inplace is False, otherwise None.
        """
        if inplace:
            cs = self
        else:
            cs = self.copy()

        # Create a connection matrix
        connectionMatrix = pd.DataFrame(index=cs.dPower_BusInfo.index, columns=[cs.dPower_BusInfo.index], data=False)
        for index, entry in cs.dPower_Network.iterrows():
            if entry["pTecRepr"] == "SN":
                connectionMatrix.loc[index[0], index[1]] = True
                connectionMatrix.loc[index[1], index[0]] = True

        # Merge buses based on connection matrix
        merged_buses = set()  # Set of buses that have been merged already
        for index, entry in connectionMatrix.iterrows():
            if index in merged_buses or not entry.any():  # Skip if bus has already been merged or has no connections
                continue

            connected_buses = cs.get_connected_buses(connectionMatrix, str(index))

            for bus in connected_buses:
                merged_buses.add(bus)

            new_bus_name = "merged-" + "-".join(connected_buses)

            ### Adapt dPower_BusInfo
            dPower_BusInfo_entry = cs.dPower_BusInfo.loc[connected_buses]  # Entry for the new bus
            zoneOfInterest = 1 if any(dPower_BusInfo_entry['zoi'] == 1) else 0
            zone_values = sorted(set(dPower_BusInfo_entry['z'].dropna().unique()))
            zone_name = '_'.join(str(v) for v in zone_values)
            aggregation_methods_for_columns = {
                # 'System': 'max',
                # 'BaseVolt': 'mean',
                # 'maxVolt': 'max',
                # 'minVolt': 'min',
                # 'Bs': 'mean',
                # 'Gs': 'mean',
                # 'PowerFactor': 'mean',
                'YearCom': 'mean',
                'YearDecom': 'mean',
                'lat': 'mean',
                'lon': 'mean'
            }
            dPower_BusInfo_entry = dPower_BusInfo_entry.agg(aggregation_methods_for_columns)
            dPower_BusInfo_entry['zoi'] = zoneOfInterest
            dPower_BusInfo_entry['z'] = zone_name
            dPower_BusInfo_entry = dPower_BusInfo_entry.to_frame().T
            dPower_BusInfo_entry.index = [new_bus_name]

            cs.dPower_BusInfo = cs.dPower_BusInfo.drop(connected_buses)
            with warnings.catch_warnings():  # Suppressing FutureWarning because some entries might include NaN values
                warnings.simplefilter(action='ignore', category=FutureWarning)
                cs.dPower_BusInfo = pd.concat([cs.dPower_BusInfo, dPower_BusInfo_entry])

            ### Adapt dPower_Network
            cs.dPower_Network = cs.dPower_Network.reset_index()
            rows_to_drop = []
            for i, row in cs.dPower_Network.iterrows():
                if row['i'] in connected_buses and row['j'] in connected_buses:
                    rows_to_drop.append(i)
                elif row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    cs.dPower_Network.iloc[i] = row
                elif row['j'] in connected_buses:
                    row['j'] = new_bus_name
                    cs.dPower_Network.iloc[i] = row
            cs.dPower_Network = cs.dPower_Network.drop(rows_to_drop)

            # Always put new_bus_name to 'j' (handles case where e.g. 2->3 and 4->2 would lead to 2->34 and 34->2 (because 3 and 4 are merged))
            for i, row in cs.dPower_Network.iterrows():
                if row['i'] == new_bus_name:
                    row['i'] = row['j']
                    row['j'] = new_bus_name
                    cs.dPower_Network.loc[i] = row

            # Handle case where e.g. 2->3 and 2->4 would lead to 2->34 and 2->34 (because 3 and 4 are merged); also incl. handling 2->3 and 4->2
            cs.dPower_Network['pTecRepr'] = cs.dPower_Network.groupby(['i', 'j'])['pTecRepr'].transform(lambda series: 'DC-OPF' if 'DC-OPF' in series.values else series.iloc[0])
            aggregation_methods_for_columns = {
                # 'Circuit ID': 'first',
                # 'InService': 'max',
                # 'R': 'mean',
                'pXline': lambda x: x.map(lambda a: 1 / a).sum() ** -1,  # Formula: 1/X = sum((i,j), 1/Xij)) (e.g., 1/X = 1/Xij_1 +1/Xij_2 + 1/Xij_3...)
                # 'Bc': 'mean',
                # 'TapAngle': 'mean',
                # 'TapRatio': 'mean',
                'pPmax': lambda x: x.min() * x.count(),  # Number of lines times the minimum Pmax for new Pmax of the merged lines TODO: Calculate this based on more complex method (flow is relative to R, talk to Benjamin)
                # 'FixedCost': 'mean',
                # 'FxChargeRate': 'mean',
                'pTecRepr': 'first',
                'YearCom': 'mean',
                'YearDecom': 'mean'
            }
            # Add aggregation for any missing columns
            for column in cs.dPower_Network.columns:
                if column not in aggregation_methods_for_columns and column not in ['i', 'j', 'c']:
                    aggregation_methods_for_columns[column] = 'first'

            cs.dPower_Network = cs.dPower_Network.groupby(['i', 'j', 'c']).agg(aggregation_methods_for_columns)

            ### Adapt dPower_ThermalGen
            if hasattr(cs, "dPower_ThermalGen"):
                cs.dPower_ThermalGen.loc[cs.dPower_ThermalGen['i'].isin(connected_buses), 'i'] = new_bus_name

            # Adapt dPower_VRES
            if hasattr(cs, "dPower_VRES"):
                cs.dPower_VRES.loc[cs.dPower_VRES['i'].isin(connected_buses), 'i'] = new_bus_name

            # Adapt dPower_Storage
            if hasattr(cs, "dPower_Storage"):
                cs.dPower_Storage.loc[cs.dPower_Storage['i'].isin(connected_buses), 'i'] = new_bus_name

            # Adapt dPower_Demand
            cs.dPower_Demand = cs.dPower_Demand.reset_index()
            mask = cs.dPower_Demand['i'].isin(connected_buses)  # Create mask for rows to be unified
            cs.dPower_Demand.loc[mask, 'i'] = new_bus_name  # Update bus names
            aggregation_methods_power_demand = {
                'value': 'sum',
                'dataPackage': lambda v: f"merged-{'-'.join(v.unique())}",
                'dataSource': lambda v: f"merged-{'-'.join(v.unique())}",
                'scenario': lambda v: '-'.join(v.unique())  # If there are multiple scenarios, this would probably fail later (which is good - then we know, something isn't right!)
            }
            cs.dPower_Demand = cs.dPower_Demand.groupby(['rp', 'k', 'i']).agg(aggregation_methods_power_demand)

        return cs if not inplace else None

    def merge_generators(self, inplace: bool = False) -> Optional['CaseStudy']:
        """
        Merge generators of the same technology at the same bus into one representative generator.
        Affects dPower_ThermalGen, dPower_VRES, dPower_VRESProfiles, and dPower_Inflows.
        The new generator ID is '{i}_{tec}'.

        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        cs = self if inplace else self.copy()

        # Save original VRES mapping before any merges (needed for VRESProfiles and Inflows weighting)
        original_vres_info = None
        if hasattr(cs, 'dPower_VRES') and cs.dPower_VRES is not None and 'MaxProd' in cs.dPower_VRES.columns:
            original_vres_info = cs.dPower_VRES[['tec', 'i', 'MaxProd']].copy()

        ### Merge dPower_ThermalGen
        if hasattr(cs, 'dPower_ThermalGen') and cs.dPower_ThermalGen is not None:
            df = cs.dPower_ThermalGen.reset_index()
            groups = ['tec', 'i']

            thermal_simple_agg = {
                'ExisUnits': 'max',
                'MaxProd': 'sum',
                'MinProd': 'min',
                'RampUp': 'sum',
                'RampDw': 'sum',
                'MinUpTime': 'min',
                'MinDownTime': 'min',
                'Qmax': 'sum',
                'Qmin': 'sum',
                'EnableInvest': 'max',
                'YearCom': 'min',
                'YearDecom': 'max',
                'lat': 'mean',
                'lon': 'mean',
            }
            thermal_weighted_cols = ['InertiaConst', 'FuelCost', 'Efficiency', 'CommitConsumption',
                                     'OMVarCost', 'StartupConsumption', 'EFOR', 'InvestCost',
                                     'FirmCapCoef', 'CO2Emis']

            agg_dict = {}
            skip_cols = set(groups + ['g'] + thermal_weighted_cols)
            for col in df.columns:
                if col in skip_cols:
                    continue
                agg_dict[col] = thermal_simple_agg.get(col, 'first')

            merged = df.groupby(groups).agg(agg_dict).reset_index()

            for col in thermal_weighted_cols:
                if col not in df.columns:
                    continue
                numer = (df[col] * df['MaxProd']).groupby([df['tec'], df['i']]).sum()
                denom = df['MaxProd'].groupby([df['tec'], df['i']]).sum()
                wavg = (numer / denom.replace(0, np.nan)).fillna(df.groupby(groups)[col].mean())
                wavg.name = col
                merged = merged.merge(wavg.reset_index(), on=groups, how='left')

            merged['g'] = merged['i'].astype(str) + '_' + merged['tec']
            cs.dPower_ThermalGen = merged.set_index('g')

        ### Merge dPower_VRESProfiles (before dPower_VRES so original MaxProd weights are available)
        if (hasattr(cs, 'dPower_VRESProfiles') and cs.dPower_VRESProfiles is not None
                and original_vres_info is not None):
            df = cs.dPower_VRESProfiles.reset_index()
            vres_cols = original_vres_info.reset_index()[['g', 'tec', 'i', 'MaxProd']]
            df = df.merge(vres_cols, on='g', how='left')

            groups = ['rp', 'k', 'scenario', 'tec', 'i']
            key = [df['rp'], df['k'], df['scenario'], df['tec'], df['i']]

            numer = (df['value'] * df['MaxProd']).groupby(key).sum()
            denom = df['MaxProd'].groupby(key).sum()
            merged_value = (numer / denom.replace(0, np.nan)).fillna(df.groupby(groups)['value'].mean())
            merged_value.name = 'value'

            meta_cols = [c for c in ['dataPackage', 'dataSource', 'id'] if c in df.columns]
            meta = df.groupby(groups)[meta_cols].first().reset_index()
            merged = meta.merge(merged_value.reset_index(), on=groups, how='left')
            merged['g'] = merged['i'].astype(str) + '_' + merged['tec']
            merged = merged.drop(columns=['tec', 'i'])
            cs.dPower_VRESProfiles = merged.set_index(['rp', 'k', 'g'])

        ### Merge dPower_Inflows
        if (hasattr(cs, 'dPower_Inflows') and cs.dPower_Inflows is not None
                and original_vres_info is not None):
            df = cs.dPower_Inflows.reset_index()
            vres_cols = original_vres_info.reset_index()[['g', 'tec', 'i']]
            df = df.merge(vres_cols, on='g', how='left')

            groups = ['rp', 'k', 'scenario', 'tec', 'i']
            key = [df['rp'], df['k'], df['scenario'], df['tec'], df['i']]

            merged_value = df['value'].groupby(key).sum()
            merged_value.name = 'value'

            meta_cols = [c for c in ['dataPackage', 'dataSource', 'id'] if c in df.columns]
            meta = df.groupby(groups)[meta_cols].first().reset_index()
            merged = meta.merge(merged_value.reset_index(), on=groups, how='left')
            merged['g'] = merged['i'].astype(str) + '_' + merged['tec']
            merged = merged.drop(columns=['tec', 'i'])
            cs.dPower_Inflows = merged.set_index(['rp', 'k', 'g'])

        ### Merge dPower_VRES (last, after VRESProfiles and Inflows)
        if hasattr(cs, 'dPower_VRES') and cs.dPower_VRES is not None:
            df = cs.dPower_VRES.reset_index()
            groups = ['tec', 'i']

            vres_simple_agg = {
                'ExisUnits': 'sum',
                'EnableInvest': 'max',
                'Qmax': 'sum',
                'Qmin': 'sum',
                'YearCom': 'min',
                'YearDecom': 'max',
                'lat': 'mean',
                'lon': 'mean',
            }
            vres_weighted_cols = ['InvestCost', 'OMVarCost', 'FirmCapCoef', 'InertiaConst']
            special_cols = {'MaxProd', 'MaxInvest'}

            agg_dict = {}
            skip_cols = set(groups + ['g'] + vres_weighted_cols + list(special_cols))
            for col in df.columns:
                if col in skip_cols:
                    continue
                agg_dict[col] = vres_simple_agg.get(col, 'first')

            merged = df.groupby(groups).agg(agg_dict).reset_index()

            # Special: newMaxProd = sum(ExisUnits * MaxProd) / sum(ExisUnits); fallback to sum when all units are greenfield
            if 'MaxProd' in df.columns:
                total_mw = (df['ExisUnits'] * df['MaxProd']).groupby([df['tec'], df['i']]).sum()
                total_units = df['ExisUnits'].groupby([df['tec'], df['i']]).sum()
                new_maxprod = (total_mw / total_units.replace(0, np.nan)).fillna(
                    df['MaxProd'].groupby([df['tec'], df['i']]).sum()
                )
                new_maxprod.name = 'MaxProd'
                merged = merged.merge(new_maxprod.reset_index(), on=groups, how='left')

            # Special: newMaxInvest = sum(MaxInvest * MaxProd) / newMaxProd
            if 'MaxInvest' in df.columns and 'MaxProd' in df.columns:
                invest_mw = (df['MaxInvest'] * df['MaxProd']).groupby([df['tec'], df['i']]).sum()
                new_maxprod_s = merged.set_index(groups)['MaxProd']
                new_maxinvest = (invest_mw / new_maxprod_s.replace(0, np.nan)).fillna(0)
                new_maxinvest.name = 'MaxInvest'
                merged = merged.merge(new_maxinvest.reset_index(), on=groups, how='left')

            for col in vres_weighted_cols:
                if col not in df.columns:
                    continue
                numer = (df[col] * df['MaxProd']).groupby([df['tec'], df['i']]).sum()
                denom = df['MaxProd'].groupby([df['tec'], df['i']]).sum()
                wavg = (numer / denom.replace(0, np.nan)).fillna(df.groupby(groups)[col].mean())
                wavg.name = col
                merged = merged.merge(wavg.reset_index(), on=groups, how='left')

            merged['g'] = merged['i'].astype(str) + '_' + merged['tec']
            cs.dPower_VRES = merged.set_index('g')

        return None if inplace else cs

    # Create transition matrix from Hindex
    def get_rpTransitionMatrices(self, clip_method: str = "none", clip_value: float = 0) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        rps = sorted(self.dPower_Hindex.index.get_level_values('rp').unique().tolist())
        ks = sorted(self.dPower_Hindex.index.get_level_values('k').unique().tolist())
        rpTransitionMatrixAbsolute = pd.DataFrame(0, index=rps, columns=rps)  # Initialize with zeros

        # Reduce rps in hindex to only include one rp per row (e.g., if it's 24 hours per rp, only take hours 0, 24, 48, ...)
        hindex_rps = self.dPower_Hindex.index.get_level_values('rp').tolist()[::len(ks)]

        # Iterate through rps in hindex
        previous_rp = hindex_rps[-1]  # Initialize with last rp to make it circular
        for rp in hindex_rps:
            rpTransitionMatrixAbsolute.at[previous_rp, rp] += 1
            previous_rp = rp

        # Clip according to selected method
        match clip_method:
            case "none":
                pass
            case "absolute_count":  # Get 'clip_value' highest values of each row of the transition matrix, set all others to 0
                if int(clip_value) != clip_value or clip_value < 0:
                    raise ValueError(f"For 'absolute_count', clip_value must be a non-negative integer, not {clip_value}.")
                for rp in rps:
                    threshold = rpTransitionMatrixAbsolute.loc[rp].nlargest(int(clip_value)).min()
                    if (rpTransitionMatrixAbsolute.loc[rp] == threshold).sum() > 1:
                        printer.warning(f"For rp {rp}, there are multiple values with the same value as the threshold ({threshold}). This means that more than {clip_value} values are kept.")
                    rpTransitionMatrixAbsolute.loc[rp, rpTransitionMatrixAbsolute.loc[rp] < threshold] = 0
            case "relative_to_highest":  # Get all values that are at least 'clip_value' * 100 % of the highest value of each row of the transition matrix, set all others to 0
                if clip_value < 0 or clip_value > 1:
                    raise ValueError(f"For 'relative_to_highest', clip_value must be between 0 and 1, not {clip_value}.")
                for rp in rps:
                    threshold = rpTransitionMatrixAbsolute.loc[rp].max() * clip_value
                    rpTransitionMatrixAbsolute.loc[rp, rpTransitionMatrixAbsolute.loc[rp] < threshold] = 0
            case _:
                raise ValueError(f"clip_method must be either 'none', 'absolute_count' or 'relative_to_highest', not {clip_method}.")

        # Calculate relative transition matrix (nerd info: for the sum, the axis is irrelevant, as there are the same number of transitions to an rp as there are transitions from an rp away. For the division however, the axis matters)
        rpTransitionMatrixRelativeTo = rpTransitionMatrixAbsolute.div(rpTransitionMatrixAbsolute.sum(axis=1), axis=0)  # Sum of probabilities is 1 for r -> all others
        rpTransitionMatrixRelativeFrom = rpTransitionMatrixAbsolute.div(rpTransitionMatrixAbsolute.sum(axis=0), axis=1)  # Sum of probabilities is 1 for all others -> r
        return rpTransitionMatrixAbsolute, rpTransitionMatrixRelativeTo, rpTransitionMatrixRelativeFrom

    def to_full_hourly_model(self, inplace: bool) -> Optional['CaseStudy']:
        """
        Transforms the given `CaseStudy` with representative periods into a full hourly model by adjusting demand,
        VRES profiles, Hindex, and weights data. Can update in place if `inplace` is set to `True`,
        or return a new `CaseStudy` instance if `inplace` is `False`. The adjustments align the data
        to represent hourly indices and corresponding weights.

        Each enabled scenario in `dGlobal_Scenarios` is processed independently.

        If the case study is already a full hourly model (no two p-values share the same 'k' within
        any scenario), no adjustment is made and the original instance is returned unchanged.

        :param inplace: If `True`, modifies the given instance. If `False`, returns a new `CaseStudy` instance.
        :return: Adjusted `CaseStudy` instance if `inplace` is `False`, otherwise `None`.
        """
        caseStudy = self.copy() if not inplace else self

        # Check if already hourly: within each scenario no 'k' appears more than once
        hindex_flat = caseStudy.dPower_Hindex.reset_index()
        already_hourly = all(
            not (grp.groupby(['k']).size() > 1).any()
            for _, grp in hindex_flat.groupby('scenario')
        )
        if already_hourly:
            return None if inplace else caseStudy

        scenario_names = caseStudy.dGlobal_Scenarios.index.tolist()

        all_demand = []
        all_vresprofiles = []
        all_inflows = []
        all_hindex = []
        all_weightsk = []
        all_weightsrp = []

        demand_flat = caseStudy.dPower_Demand.reset_index()
        vres_flat = caseStudy.dPower_VRESProfiles.reset_index() if hasattr(caseStudy, 'dPower_VRESProfiles') and caseStudy.dPower_VRESProfiles is not None else None
        inflows_flat = caseStudy.dPower_Inflows.reset_index() if hasattr(caseStudy, 'dPower_Inflows') and caseStudy.dPower_Inflows is not None else None

        for scenario in scenario_names:
            scen_hindex = hindex_flat[hindex_flat['scenario'] == scenario].copy().reset_index(drop=True)
            num_hours = len(scen_hindex)
            scen_hindex['new_k'] = [f"k{i + 1:0>4}" for i in range(num_hours)]
            scen_hindex['new_p'] = [f"h{i + 1:0>4}" for i in range(num_hours)]

            # Demand: merge each original (rp, k) with the buses that have that (rp, k)
            demand_scen = demand_flat[demand_flat['scenario'] == scenario][['rp', 'k', 'i', 'value']]
            m = scen_hindex[['rp', 'k', 'new_k']].merge(demand_scen, on=['rp', 'k'])
            all_demand.append(pd.DataFrame({'rp': 'rp01', 'k': m['new_k'], 'i': m['i'], 'value': m['value'],
                                            'scenario': scenario, 'id': None, 'dataPackage': None, 'dataSource': None}))

            # VRESProfiles
            if vres_flat is not None:
                vres_scen = vres_flat[vres_flat['scenario'] == scenario][['rp', 'k', 'g', 'value']]
                m = scen_hindex[['rp', 'k', 'new_k']].merge(vres_scen, on=['rp', 'k'])
                all_vresprofiles.append(pd.DataFrame({'rp': 'rp01', 'k': m['new_k'], 'g': m['g'], 'value': m['value'],
                                                      'scenario': scenario, 'id': None, 'dataPackage': None, 'dataSource': None}))

            # Inflows
            if inflows_flat is not None:
                inflows_scen = inflows_flat[inflows_flat['scenario'] == scenario][['rp', 'k', 'g', 'value']]
                m = scen_hindex[['rp', 'k', 'new_k']].merge(inflows_scen, on=['rp', 'k'])
                all_inflows.append(pd.DataFrame({'rp': 'rp01', 'k': m['new_k'], 'g': m['g'], 'value': m['value'],
                                                 'scenario': scenario, 'id': None, 'dataPackage': None, 'dataSource': None}))

            # Hindex
            hindex_scen = pd.DataFrame({
                'p': scen_hindex['new_p'],
                'rp': 'rp01',
                'k': scen_hindex['new_k'],
                'id': None,
                'dataPackage': None,
                'dataSource': None,
                'scenario': scenario,
            })
            all_hindex.append(hindex_scen)

            # WeightsK
            weightsk_scen = pd.DataFrame({
                'k': scen_hindex['new_k'],
                'id': None,
                'pWeight_k': 1,
                'dataPackage': None,
                'dataSource': None,
                'scenario': scenario,
            })
            all_weightsk.append(weightsk_scen)

            # WeightsRP
            all_weightsrp.append({'rp': 'rp01', 'id': None, 'pWeight_rp': 1, 'dataPackage': None, 'dataSource': None, 'scenario': scenario})

        caseStudy.dPower_Demand = pd.concat(all_demand).set_index(['rp', 'k', 'i'])

        if all_vresprofiles:
            caseStudy.dPower_VRESProfiles = pd.concat(all_vresprofiles).set_index(['rp', 'k', 'g'])

        if all_inflows:
            caseStudy.dPower_Inflows = pd.concat(all_inflows).set_index(['rp', 'k', 'g'])

        caseStudy.dPower_Hindex = pd.concat(all_hindex).set_index(['p', 'rp', 'k'])
        caseStudy.dPower_WeightsK = pd.concat(all_weightsk).set_index('k')
        caseStudy.dPower_WeightsRP = pd.DataFrame(all_weightsrp).set_index('rp')

        return None if inplace else caseStudy

    def filter_scenario(self, scenario_name, inplace: bool = False) -> Optional[Self]:
        """
        Filters each (relevant) dataframe in the case study to only include the scenario with the given name.
        :param scenario_name: The name of the scenario to filter for.
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        caseStudy = self if inplace else self.copy()

        for df_name in CaseStudy.scenario_dependent_dataframes:
            if hasattr(caseStudy, df_name):
                df = getattr(caseStudy, df_name)
                if df is None:
                    continue

                filtered_df = df.loc[df['scenario'] == scenario_name]

                if len(df) > 0 and len(filtered_df) == 0:
                    raise ValueError(f"Scenario '{scenario_name}' not found in '{df_name}'. Please check the input data.")

                setattr(caseStudy, df_name, filtered_df)

        return None if inplace else caseStudy

    def filter_zone(self, zone: str | list[str], inplace: bool = False) -> Optional[Self]:
        """
        Filters the case study to only include buses in the given zone(s). All generators,
        network lines, demand entries, and time series profiles connected to buses outside the
        zone are removed.
        :param zone: Zone name (value of the 'z' column in Power_BusInfo) or list of zone names to keep.
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        case_study = self if inplace else self.copy()

        zones = [zone] if isinstance(zone, str) else list(zone)

        # Filter BusInfo and derive remaining bus set
        case_study.dPower_BusInfo = case_study.dPower_BusInfo[case_study.dPower_BusInfo['z'].astype(str).isin(zones)]
        remaining_buses = set(case_study.dPower_BusInfo.index)

        # Filter Network: drop lines where either endpoint is outside the zone
        network_reset = case_study.dPower_Network.reset_index()
        case_study.dPower_Network = network_reset[
            network_reset['i'].astype(str).isin(remaining_buses) & network_reset['j'].astype(str).isin(remaining_buses)
            ]
        case_study.dPower_Network['i'] = case_study.dPower_Network['i'].astype(str)
        case_study.dPower_Network['j'] = case_study.dPower_Network['j'].astype(str)
        case_study.dPower_Network['c'] = case_study.dPower_Network['c'].astype(str)
        case_study.dPower_Network.set_index(['i', 'j', 'c'], inplace=True)

        # Filter ThermalGen
        if hasattr(case_study, 'dPower_ThermalGen') and case_study.dPower_ThermalGen is not None:
            case_study.dPower_ThermalGen = case_study.dPower_ThermalGen[
                case_study.dPower_ThermalGen['i'].astype(str).isin(remaining_buses)
            ]

        # Filter VRES; collect remaining VRES generator IDs for VRESProfiles / Inflows
        remaining_vres_gens: set = set()
        if hasattr(case_study, 'dPower_VRES') and case_study.dPower_VRES is not None:
            case_study.dPower_VRES = case_study.dPower_VRES[
                case_study.dPower_VRES['i'].astype(str).isin(remaining_buses)
            ]
            remaining_vres_gens = set(case_study.dPower_VRES.index)

        # Filter Storage; collect remaining storage generator IDs for Inflows
        remaining_storage_gens: set = set()
        if hasattr(case_study, 'dPower_Storage') and case_study.dPower_Storage is not None:
            case_study.dPower_Storage = case_study.dPower_Storage[
                case_study.dPower_Storage['i'].astype(str).isin(remaining_buses)
            ]
            remaining_storage_gens = set(case_study.dPower_Storage.index)

        # Filter Demand
        demand_reset = case_study.dPower_Demand.reset_index()
        case_study.dPower_Demand = demand_reset[
            demand_reset['i'].astype(str).isin(remaining_buses)
        ]
        case_study.dPower_Demand['i'] = case_study.dPower_Demand['i'].astype(str)
        case_study.dPower_Demand.set_index(['rp', 'k', 'i'], inplace=True)

        # Filter VRESProfiles by remaining VRES generator IDs
        if hasattr(case_study, 'dPower_VRESProfiles') and case_study.dPower_VRESProfiles is not None:
            profiles_reset = case_study.dPower_VRESProfiles.reset_index()
            case_study.dPower_VRESProfiles = profiles_reset[
                profiles_reset['g'].isin(remaining_vres_gens)
            ].set_index(['rp', 'k', 'g'])

        # Filter Inflows by remaining VRES + Storage generator IDs
        if hasattr(case_study, 'dPower_Inflows') and case_study.dPower_Inflows is not None:
            remaining_gens = remaining_vres_gens | remaining_storage_gens
            inflows_reset = case_study.dPower_Inflows.reset_index()
            case_study.dPower_Inflows = inflows_reset[
                inflows_reset['g'].isin(remaining_gens)
            ].set_index(['rp', 'k', 'g'])

        # Filter ImportExport by remaining buses
        if hasattr(case_study, 'dPower_ImportExport') and case_study.dPower_ImportExport is not None:
            ie_reset = case_study.dPower_ImportExport.reset_index()
            case_study.dPower_ImportExport = ie_reset[
                ie_reset['i'].astype(str).isin(remaining_buses)
            ]
            case_study.dPower_ImportExport['i'] = case_study.dPower_ImportExport['i'].astype(str)
            case_study.dPower_ImportExport.set_index(['hub', 'i', 'rp', 'k'], inplace=True)

        return None if inplace else case_study

    def filter_timesteps(self, start: str, end: str, inplace: bool = False, no_weight_k_adjustment: bool = False) -> Optional[Self]:
        """
        Filters each (relevant) dataframe in the case study to only include the timesteps between start and end (both inclusive).
        :param start: Start timestep (inclusive).
        :param end: End timestep (inclusive).
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :param no_weight_k_adjustment: If True, does not adjust the weights in dPower_WeightsK after filtering. Adjustment is done by default.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        case_study = self if inplace else self.copy()

        # Calculate total weight before filtering
        if not no_weight_k_adjustment and hasattr(case_study, "dPower_WeightsK") and case_study.dPower_WeightsK is not None:
            total_weight = case_study.dPower_WeightsK['pWeight_k'].sum()

        for df_name in CaseStudy.k_dependent_dataframes:
            if hasattr(case_study, df_name) and getattr(case_study, df_name) is not None:
                df = getattr(case_study, df_name)
                if df is None:
                    continue

                index = df.index.names
                df_reset = df.reset_index()

                filtered_df_reset = df_reset.loc[(df_reset['k'] >= start) & (df_reset['k'] <= end)]

                filtered_df = filtered_df_reset.set_index(index)

                setattr(case_study, df_name, filtered_df)

        if no_weight_k_adjustment:
            printer.information("Skipped adjustment of weights in 'dPower_WeightsK' after filtering timesteps as per user request.")
        elif (not hasattr(case_study, "dPower_WeightsK")) or (case_study.dPower_WeightsK is None):
            printer.information("Skipped adjustment of weights in 'dPower_WeightsK' after filtering timesteps because 'dPower_WeightsK' does not exist.")
        else:
            # Adjust weights after filtering
            filtered_total_weight = case_study.dPower_WeightsK['pWeight_k'].sum()
            if filtered_total_weight == 0:
                raise ValueError("After filtering timesteps, the total weight in 'dPower_WeightsK' is zero. Cannot adjust weights.")
            adjustment_factor = total_weight / filtered_total_weight
            case_study.dPower_WeightsK['pWeight_k'] *= adjustment_factor
            printer.information(f"Adjusted weights in 'dPower_WeightsK' by a factor of {adjustment_factor} after filtering timesteps.")

        return None if inplace else case_study

    def filter_representative_periods(self, rp: str, inplace: bool = False) -> Optional[Self]:
        """
        Filters each (relevant) dataframe in the case study to only include the representative period with the given name.
        :param rp: Name of the representative period to filter for.
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        case_study = self if inplace else self.copy()

        for df_name in CaseStudy.rp_dependent_dataframes:
            if hasattr(case_study, df_name) and getattr(case_study, df_name) is not None:
                df = getattr(case_study, df_name)

                index = df.index.names
                df_reset = df.reset_index()

                filtered_df_reset = df_reset.loc[(df_reset['rp'] == rp)]

                filtered_df = filtered_df_reset.set_index(index)

                setattr(case_study, df_name, filtered_df)

        return None if inplace else case_study

    def shift_ks(self, shift: int, inplace: bool = False) -> Optional[Self]:
        """
        Shifts all k indices by the given amount, i.e., if shift is 4, then the first 4
        timesteps are moved to the back of the time series.

        :param shift: The amount to shift the k indices by.
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        case_study = self if inplace else self.copy()

        for df_name in CaseStudy.k_dependent_dataframes:
            if df_name in ["dPower_WeightsK", "dPower_Hindex"]:
                continue  # These dataframes are not shifted, as they are not time series

            if hasattr(case_study, df_name):
                df = getattr(case_study, df_name)
                if df is None or df.empty:
                    continue

                index = df.index.names
                df = df.reset_index()

                df["k_int"] = df["k"].str.replace("k", "").astype(int)
                k_int_max = df["k_int"].max()
                k_int_min = df["k_int"].min()

                df["k_int_new"] = ((df["k_int"] - k_int_min + shift) % (k_int_max - k_int_min + 1)) + k_int_min

                df["k"] = "k" + df["k_int_new"].astype(str).str.zfill(4)
                df = df.drop(columns=["k_int", "k_int_new"])
                df = df.set_index(index)

                # Sort by index to ensure that the order of the indices is correct after shifting
                df = df.sort_index()

                setattr(case_study, df_name, df)

        return None if inplace else case_study

    def apply_kmedoids_aggregation(self, number_rps: int, rp_length: int = 24,
                                   cluster_strategy: Literal["aggregated", "disaggregated"] = "aggregated",
                                   capacity_normalization: Literal["installed", "maxInvestment"] = "maxInvestment",
                                   sum_production: bool = False, inplace: bool = True, verbose: bool = False) -> Optional[Self]:
        """
        Apply k-medoids temporal aggregation to a CaseStudy object.
        Each scenario from dGlobal_Scenarios is processed independently.

        :param self: The CaseStudy object to aggregate
        :param number_rps: Number of representative periods to create
        :param rp_length: Hours per representative period (e.g., 24, 48)
        :param cluster_strategy: "aggregated" (sum across buses) or "disaggregated" (keep buses separate)
        :param capacity_normalization: "installed" or "maxInvestment" for VRES capacity factor weighting
        :param sum_production: If True, sum all technologies into single production column
        :param inplace: If True, modify the original CaseStudy; otherwise, return a new one
        :param verbose: If True, print detailed processing information

        :return:
            CaseStudy: New clustered CaseStudy object if inplace is False; otherwise, None
        """

        cs = self if inplace else self.copy()
        Utilities.apply_kmedoids_aggregation(cs, number_rps, rp_length, cluster_strategy, capacity_normalization, sum_production, inplace=True, verbose=verbose)
        if inplace:
            return None
        else:
            return cs

    def get_kmedoids_representative_periods(self, number_rps: int, rp_length: int = 24,
                                            cluster_strategy: Literal["aggregated", "disaggregated"] = "aggregated",
                                            capacity_normalization: Literal["installed", "maxInvestment"] = "maxInvestment",
                                            sum_production: bool = False, verbose: bool = False) -> dict[str, tsam.TimeSeriesAggregation]:
        """
        Get the representative periods using k-medoids temporal aggregation. Does not modify the original CaseStudy.
        Each scenario from dGlobal_Scenarios is processed independently.

        :param self: The CaseStudy object to aggregate
        :param number_rps: Number of representative periods to create
        :param rp_length: Hours per representative period (e.g., 24, 48)
        :param cluster_strategy: "aggregated" (sum across buses) or "disaggregated" (keep buses separate)
        :param capacity_normalization: "installed" or "maxInvestment" for VRES capacity factor weighting
        :param sum_production: If True, sum all technologies into single production column
        :param verbose: If True, print detailed processing information

        :return: TSAM TimeSeriesAggregation object with representative periods for each scenario
        """

        return Utilities.get_kmedoids_representative_periods(self, number_rps, rp_length, cluster_strategy, capacity_normalization, sum_production=sum_production, verbose=verbose)

    def apply_representative_periods(self, representative_periods: dict[str, tsam.TimeSeriesAggregation], rp_length: int = 24,
                                     inplace: bool = True, verbose: bool = False) -> Optional[Self]:
        """
        Apply precomputed representative periods to a CaseStudy object.
        Each scenario from dGlobal_Scenarios is processed independently.

        :param self: The CaseStudy object to aggregate
        :param representative_periods: Precomputed TimeSeriesAggregation object
        :param rp_length: Hours per representative period (e.g., 24, 48)
        :param inplace: If True, modify the original CaseStudy; otherwise, return a new one
        :param verbose: If True, print detailed processing information
        :returns: New clustered CaseStudy object if inplace is False; otherwise, None
        """

        cs = self if inplace else self.copy()
        Utilities.apply_representative_periods(cs, representative_periods, rp_length, inplace=True, verbose=verbose)
        if inplace:
            return None
        else:
            return cs
