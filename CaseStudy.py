import copy
import os
import warnings
from pathlib import Path
from typing import Optional, Self

import numpy as np
import pandas as pd

import ExcelReader
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
                 global_parameters_file: str = "Global_Parameters.xlsx", dGlobal_Parameters: pd.DataFrame = None,
                 global_scenarios_file: str = "Global_Scenarios.xlsx", dGlobal_Scenarios: pd.DataFrame = None,
                 power_parameters_file: str = "Power_Parameters.xlsx", dPower_Parameters: pd.DataFrame = None,
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
                 power_importexport_file: str = "Power_ImportExport.xlsx", dPower_ImportExport: pd.DataFrame = None):
        self.data_folder = str(data_folder) if str(data_folder).endswith("/") else str(data_folder) + "/"
        self.do_not_scale_units = do_not_scale_units
        self.do_not_merge_single_node_buses = do_not_merge_single_node_buses

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

        if dPower_BusInfo is not None:
            self.dPower_BusInfo = dPower_BusInfo
        else:
            self.power_businfo_file = power_businfo_file
            self.dPower_BusInfo = ExcelReader.get_Power_BusInfo(self.data_folder + self.power_businfo_file)

        if dPower_Network is not None:
            self.dPower_Network = dPower_Network
        else:
            self.power_network_file = power_network_file
            self.dPower_Network = ExcelReader.get_Power_Network(self.data_folder + self.power_network_file)

        if dPower_Demand is not None:
            self.dPower_Demand = dPower_Demand
        else:
            self.power_demand_file = power_demand_file
            self.dPower_Demand = ExcelReader.get_Power_Demand(self.data_folder + self.power_demand_file)

        if dPower_WeightsRP is not None:
            self.dPower_WeightsRP = dPower_WeightsRP
        else:
            self.power_weightsrp_file = power_weightsrp_file
            self.dPower_WeightsRP = ExcelReader.get_Power_WeightsRP(self.data_folder + self.power_weightsrp_file)

        if dPower_WeightsK is not None:
            self.dPower_WeightsK = dPower_WeightsK
        else:
            self.power_weightsk_file = power_weightsk_file
            self.dPower_WeightsK = ExcelReader.get_Power_WeightsK(self.data_folder + self.power_weightsk_file)

        if dPower_Hindex is not None:
            self.dPower_Hindex = dPower_Hindex
        else:
            self.power_hindex_file = power_hindex_file
            self.dPower_Hindex = ExcelReader.get_Power_Hindex(self.data_folder + self.power_hindex_file)

        self.rpTransitionMatrixAbsolute, self.rpTransitionMatrixRelativeTo, self.rpTransitionMatrixRelativeFrom = self.get_rpTransitionMatrices()

        if self.dPower_Parameters["pEnableThermalGen"]:
            if dPower_ThermalGen is not None:
                self.dPower_ThermalGen = dPower_ThermalGen
            else:
                self.power_thermalgen_file = power_thermalgen_file
                self.dPower_ThermalGen = ExcelReader.get_Power_ThermalGen(self.data_folder + self.power_thermalgen_file)

        if self.dPower_Parameters["pEnableVRES"]:
            if dPower_VRES is not None:
                self.dPower_VRES = dPower_VRES
            else:
                self.power_vres_file = power_vres_file
                self.dPower_VRES = ExcelReader.get_Power_VRES(self.data_folder + self.power_vres_file)

            if dPower_VRESProfiles is not None:
                self.dPower_VRESProfiles = dPower_VRESProfiles
            elif os.path.isfile(self.data_folder + power_vresprofiles_file):
                self.power_vresprofiles_file = power_vresprofiles_file
                self.dPower_VRESProfiles = ExcelReader.get_Power_VRESProfiles(self.data_folder + self.power_vresprofiles_file)

        if self.dPower_Parameters["pEnableStorage"]:
            if dPower_Storage is not None:
                self.dPower_Storage = dPower_Storage
            else:
                self.power_storage_file = power_storage_file
                self.dPower_Storage = ExcelReader.get_Power_Storage(self.data_folder + self.power_storage_file)

        if self.dPower_Parameters["pEnableVRES"] or self.dPower_Parameters["pEnableStorage"]:
            if dPower_Inflows is not None:
                self.dPower_Inflows = dPower_Inflows
            elif os.path.isfile(self.data_folder + power_inflows_file):
                self.power_inflows_file = power_inflows_file
                self.dPower_Inflows = ExcelReader.get_Power_Inflows(self.data_folder + self.power_inflows_file)

        if self.dPower_Parameters["pEnablePowerImportExport"]:
            if dPower_ImportExport is not None:
                self.dPower_ImportExport = dPower_ImportExport
            else:
                self.power_importexport_file = power_importexport_file
                self.dPower_ImportExport = self.get_dPower_ImportExport()
        else:
            self.dPower_ImportExport = None

        if not do_not_merge_single_node_buses:
            self.merge_single_node_buses()

        self.power_scaling_factor = self.dGlobal_Parameters["pPowerScalingFactor"]
        self.cost_scaling_factor = self.dGlobal_Parameters["pCostScalingFactor"]
        self.reactive_power_scaling_factor = 1e-3  # MVar to kVar conversion factor
        self.angle_to_rad_scaling_factor = np.pi / 180

        if not do_not_scale_units:
            self.scale_CaseStudy()

    def copy(self):
        return copy.deepcopy(self)

    def scale_CaseStudy(self):
        self.scale_dPower_Parameters()
        self.scale_dPower_Network()
        self.scale_dPower_Demand()

        if self.dPower_Parameters["pEnableThermalGen"]:
            self.scale_dPower_ThermalGen()

        if hasattr(self, "dPower_Inflows") and self.dPower_Inflows is not None:
            self.scale_dPower_Inflows()

        if self.dPower_Parameters["pEnableVRES"]:
            self.scale_dPower_VRES()

        if self.dPower_Parameters["pEnableStorage"]:
            self.scale_dPower_Storage()

        if self.dPower_Parameters["pEnablePowerImportExport"]:
            self.scale_dPower_ImpExpHubs()
            self.scale_dPower_ImpExpProfiles()

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
        self.dPower_Inflows["value"] *= self.power_scaling_factor

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
        self.dPower_ImportExport["ImpExpMin"] *= self.power_scaling_factor
        self.dPower_ImportExport["ImpExpMax"] *= self.power_scaling_factor
        self.dPower_ImportExport["ImpExpPrice"] *= self.cost_scaling_factor / self.power_scaling_factor

    def get_dGlobal_Parameters(self):
        ExcelReader.check_LEGOExcel_version(self.data_folder + self.global_parameters_file, "v0.1.0", False)
        dGlobal_Parameters = pd.read_excel(self.data_folder + self.global_parameters_file, skiprows=[0, 1])
        dGlobal_Parameters = dGlobal_Parameters.drop(dGlobal_Parameters.columns[0], axis=1)
        dGlobal_Parameters = dGlobal_Parameters.set_index('Solver Options')

        self.yesNo_to_bool(dGlobal_Parameters, ['pEnableRMIP'])

        # Transform to make it easier to access values
        dGlobal_Parameters = dGlobal_Parameters.drop(dGlobal_Parameters.columns[1:], axis=1)  # Drop all columns but "Value" (rest is just for information in the Excel)
        dGlobal_Parameters = dict({(parameter_name, parameter_value["Value"]) for parameter_name, parameter_value in dGlobal_Parameters.iterrows()})  # Transform into dictionary

        return dGlobal_Parameters

    def get_dPower_Parameters(self):
        ExcelReader.check_LEGOExcel_version(self.data_folder + self.power_parameters_file, "v0.1.0", False)
        dPower_Parameters = pd.read_excel(self.data_folder + self.power_parameters_file, skiprows=[0, 1])
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

    def merge_single_node_buses(self):
        # Create a connection matrix
        # TODO check
        connectionMatrix = pd.DataFrame(index=self.dPower_BusInfo.index, columns=[self.dPower_BusInfo.index], data=False)

        for index, entry in self.dPower_Network.iterrows():
            if entry["pTecRepr"] == "SN":
                connectionMatrix.loc[index] = True
                connectionMatrix.loc[index[1], index[0]] = True

        merged_buses = set()  # Set of buses that have been merged already

        for index, entry in connectionMatrix.iterrows():
            if index in merged_buses or entry[entry == True].empty:  # Skip if bus has already been merged or has no connections
                continue

            connected_buses = self.get_connected_buses(connectionMatrix, str(index))

            for bus in connected_buses:
                merged_buses.add(bus)

            new_bus_name = "merged-" + "-".join(connected_buses)

            ### Adapt dPower_BusInfo
            dPower_BusInfo_entry = self.dPower_BusInfo.loc[connected_buses]  # Entry for the new bus
            zoneOfInterest = 1 if any(dPower_BusInfo_entry['zoi'] == 1) else 0
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
                'long': 'mean'
            }
            dPower_BusInfo_entry = dPower_BusInfo_entry.agg(aggregation_methods_for_columns)
            dPower_BusInfo_entry['zoi'] = zoneOfInterest
            dPower_BusInfo_entry = dPower_BusInfo_entry.to_frame().T
            dPower_BusInfo_entry.index = [new_bus_name]

            self.dPower_BusInfo = self.dPower_BusInfo.drop(connected_buses)
            with warnings.catch_warnings():  # Suppressing FutureWarning because some entries might include NaN values
                warnings.simplefilter(action='ignore', category=FutureWarning)
                self.dPower_BusInfo = pd.concat([self.dPower_BusInfo, dPower_BusInfo_entry])

            ### Adapt dPower_Network
            self.dPower_Network = self.dPower_Network.reset_index()
            rows_to_drop = []
            for i, row in self.dPower_Network.iterrows():
                if row['i'] in connected_buses and row['j'] in connected_buses:
                    rows_to_drop.append(i)
                elif row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_Network.iloc[i] = row
                elif row['j'] in connected_buses:
                    row['j'] = new_bus_name
                    self.dPower_Network.iloc[i] = row
            self.dPower_Network = self.dPower_Network.drop(rows_to_drop)

            # Always put new_bus_name to 'j' (handles case where e.g. 2->3 and 4->2 would lead to 2->34 and 34->2 (because 3 and 4 are merged))
            for i, row in self.dPower_Network.iterrows():
                if row['i'] == new_bus_name:
                    row['i'] = row['j']
                    row['j'] = new_bus_name
                    self.dPower_Network.loc[i] = row

            # Handle case where e.g. 2->3 and 2->4 would lead to 2->34 and 2->34 (because 3 and 4 are merged); also incl. handling 2->3 and 4->2
            self.dPower_Network['Technical Representation'] = self.dPower_Network.groupby(['i', 'j'])['Technical Representation'].transform(lambda series: 'DC-OPF' if 'DC-OPF' in series.values else series.iloc[0])
            aggregation_methods_for_columns = {
                # 'Circuit ID': 'first',
                # 'InService': 'max',
                # 'R': 'mean',
                'X': lambda x: x.map(lambda a: 1 / a).sum() ** -1,  # Formula: 1/X = sum((i,j), 1/Xij)) (e.g., 1/X = 1/Xij_1 +1/Xij_2 + 1/Xij_3...)
                # 'Bc': 'mean',
                # 'TapAngle': 'mean',
                # 'TapRatio': 'mean',
                'Pmax': lambda x: x.min() * x.count(),  # Number of lines times the minimum Pmax for new Pmax of the merged lines TODO: Calculate this based on more complex method (flow is relative to R, talk to Benjamin)
                # 'FixedCost': 'mean',
                # 'FxChargeRate': 'mean',
                'Technical Representation': 'first',
                'LineID': 'first',
                'YearCom': 'mean',
                'YearDecom': 'mean'
            }
            self.dPower_Network = self.dPower_Network.groupby(['i', 'j']).agg(aggregation_methods_for_columns)

            ### Adapt dPower_ThermalGen
            for i, row in self.dPower_ThermalGen.iterrows():
                if row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_ThermalGen.loc[i] = row

            # Adapt dPower_VRES
            for i, row in self.dPower_VRES.iterrows():
                if row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_VRES.loc[i] = row

            # Adapt dPower_Storage
            for i, row in self.dPower_Storage.iterrows():
                if row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_Storage.loc[i] = row

            # Adapt dPower_Demand
            self.dPower_Demand = self.dPower_Demand.reset_index()
            for i, row in self.dPower_Demand.iterrows():
                if row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_Demand.loc[i] = row
            self.dPower_Demand = self.dPower_Demand.groupby(['rp', 'i', 'k']).sum()

            # Adapt dPower_VRESProfiles
            self.dPower_VRESProfiles = self.dPower_VRESProfiles.reset_index()
            for i, row in self.dPower_VRESProfiles.iterrows():
                if row['i'] in connected_buses:
                    row['i'] = new_bus_name
                    self.dPower_VRESProfiles.loc[i] = row

            self.dPower_VRESProfiles = self.dPower_VRESProfiles.groupby(['rp', 'i', 'k', 'tec']).mean()  # TODO: Aggregate using more complex method (capacity * productionCapacity * ... * / Total Production Capacity)
            self.dPower_VRESProfiles.sort_index(inplace=True)

    # Create transition matrix from Hindex
    def get_rpTransitionMatrices(self):
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

        :param inplace: If `True`, modifies the given instance. If `False`, returns a new `CaseStudy` instance.
        :return: Adjusted `CaseStudy` instance if `inplace` is `False`, otherwise `None`.
        """
        caseStudy = self.copy() if not inplace else self

        # Adjust Demand
        adjusted_demand = []
        for i, _ in caseStudy.dPower_BusInfo.iterrows():
            for h, row in caseStudy.dPower_Hindex.iterrows():
                adjusted_demand.append(["rp01", h[0].replace("h", "k"), i, caseStudy.dPower_Demand.loc[(h[1], h[2], i), "Demand"]])

        caseStudy.dPower_Demand = pd.DataFrame(adjusted_demand, columns=["rp", "k", "i", "Demand"])
        caseStudy.dPower_Demand = caseStudy.dPower_Demand.set_index(["rp", "k", "i"])

        # Adjust VRESProfiles
        if hasattr(caseStudy, "dPower_VRESProfiles"):
            adjusted_vresprofiles = []
            caseStudy.dPower_VRESProfiles.sort_index(inplace=True)
            for g in caseStudy.dPower_VRESProfiles.index.get_level_values('g').unique().tolist():
                if len(caseStudy.dPower_VRESProfiles.loc[:, :, g]) > 0:  # Check if VRESProfiles has entries for g
                    for h, row in caseStudy.dPower_Hindex.iterrows():
                        adjusted_vresprofiles.append(["rp01", h[0].replace("h", "k"), g, caseStudy.dPower_VRESProfiles.loc[(h[1], h[2], g), "Capacity"]])

            caseStudy.dPower_VRESProfiles = pd.DataFrame(adjusted_vresprofiles, columns=["rp", "k", "g", "Capacity"])
            caseStudy.dPower_VRESProfiles = caseStudy.dPower_VRESProfiles.set_index(["rp", "k", "g"])

        # Adjust Hindex
        caseStudy.dPower_Hindex = caseStudy.dPower_Hindex.reset_index()
        for i, row in caseStudy.dPower_Hindex.iterrows():
            caseStudy.dPower_Hindex.loc[i] = f"h{i + 1:0>4}", f"rp01", f"k{i + 1:0>4}", None, None, None
        caseStudy.dPower_Hindex = caseStudy.dPower_Hindex.set_index(["p", "rp", "k"])

        # Adjust WeightsK
        caseStudy.dPower_WeightsK = caseStudy.dPower_WeightsK.reset_index()
        caseStudy.dPower_WeightsK = caseStudy.dPower_WeightsK.drop(caseStudy.dPower_WeightsK.index)
        for i in range(len(caseStudy.dPower_Hindex)):
            caseStudy.dPower_WeightsK.loc[i] = f"k{i + 1:0>4}", None, 1, None, None
        caseStudy.dPower_WeightsK = caseStudy.dPower_WeightsK.set_index("k")

        # Adjust WeightsRP
        caseStudy.dPower_WeightsRP = caseStudy.dPower_WeightsRP.drop(caseStudy.dPower_WeightsRP.index)
        caseStudy.dPower_WeightsRP.loc["rp01"] = 1

        if not inplace:
            return caseStudy
        else:
            return None

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

    def filter_timesteps(self, start: str, end: str, inplace: bool = False) -> Optional[Self]:
        """
        Filters each (relevant) dataframe in the case study to only include the timesteps between start and end (both inclusive).
        :param start: Start timestep (inclusive).
        :param end: End timestep (inclusive).
        :param inplace: If True, modifies the current instance. If False, returns a new instance.
        :return: None if inplace is True, otherwise a new CaseStudy instance.
        """
        case_study = self if inplace else self.copy()

        for df_name in CaseStudy.k_dependent_dataframes:
            if hasattr(case_study, df_name):
                df = getattr(case_study, df_name)
                if df is None:
                    continue

                index = df.index.names
                df_reset = df.reset_index()

                filtered_df_reset = df_reset.loc[(df_reset['k'] >= start) & (df_reset['k'] <= end)]

                filtered_df = filtered_df_reset.set_index(index)

                setattr(case_study, df_name, filtered_df)

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
            if hasattr(case_study, df_name):
                df = getattr(case_study, df_name)

                index = df.index.names
                df_reset = df.reset_index()

                filtered_df_reset = df_reset.loc[(df_reset['rp'] == rp)]

                filtered_df = filtered_df_reset.set_index(index)

                setattr(case_study, df_name, filtered_df)

        return None if inplace else case_study
