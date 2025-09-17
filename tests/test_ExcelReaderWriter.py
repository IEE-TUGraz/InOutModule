import pytest

import ExcelReader as ExcelReader
from ExcelWriter import ExcelWriter
from printer import Printer

printer = Printer.getInstance()

case_study_folder = "data/example/"
ew = ExcelWriter()
combinations = [
    ("Data_Packages", f"{case_study_folder}Data_Packages.xlsx", ExcelReader.get_Data_Packages, ew.write_Data_Packages),
    ("Data_Sources", f"{case_study_folder}Data_Sources.xlsx", ExcelReader.get_Data_Sources, ew.write_Data_Sources),
    ("Global_Scenarios", f"{case_study_folder}Global_Scenarios.xlsx", ExcelReader.get_Global_Scenarios, ew.write_Global_Scenarios),
    ("Power_BusInfo", f"{case_study_folder}Power_BusInfo.xlsx", ExcelReader.get_Power_BusInfo, ew.write_Power_BusInfo),
    ("Power_Demand", f"{case_study_folder}Power_Demand.xlsx", ExcelReader.get_Power_Demand, ew.write_Power_Demand),
    ("Power_Demand_KInRows", f"{case_study_folder}Power_Demand_KInRows.xlsx", ExcelReader.get_Power_Demand_KInRows, ew.write_Power_Demand_KInRows),
    ("Power_Hindex", f"{case_study_folder}Power_Hindex.xlsx", ExcelReader.get_Power_Hindex, ew.write_Power_Hindex),
    ("Power_Inflows", f"{case_study_folder}Power_Inflows.xlsx", ExcelReader.get_Power_Inflows, ew.write_Power_Inflows),
    ("Power_Inflows_KInRows", f"{case_study_folder}Power_Inflows_KInRows.xlsx", ExcelReader.get_Power_Inflows_KInRows, ew.write_Power_Inflows_KInRows),
    ("Power_Network", f"{case_study_folder}Power_Network.xlsx", ExcelReader.get_Power_Network, ew.write_Power_Network),
    ("Power_Storage", f"{case_study_folder}Power_Storage.xlsx", ExcelReader.get_Power_Storage, ew.write_Power_Storage),
    ("Power_ThermalGen", f"{case_study_folder}Power_ThermalGen.xlsx", ExcelReader.get_Power_ThermalGen, ew.write_Power_ThermalGen),
    ("Power_VRES", f"{case_study_folder}Power_VRES.xlsx", ExcelReader.get_Power_VRES, ew.write_VRES),
    ("Power_VRESProfiles", f"{case_study_folder}Power_VRESProfiles.xlsx", ExcelReader.get_Power_VRESProfiles, ew.write_VRESProfiles),
    ("Power_VRESProfiles_KInRows", f"{case_study_folder}Power_VRESProfiles_KInRows.xlsx", ExcelReader.get_Power_VRESProfiles_KInRows, ew.write_VRESProfiles_KInRows),
    ("Power_WeightsK", f"{case_study_folder}Power_WeightsK.xlsx", ExcelReader.get_Power_WeightsK, ew.write_Power_WeightsK),
    ("Power_WeightsRP", f"{case_study_folder}Power_WeightsRP.xlsx", ExcelReader.get_Power_WeightsRP, ew.write_Power_WeightsRP),
    ("Power_Wind_TechnicalDetails", f"{case_study_folder}Power_Wind_TechnicalDetails.xlsx", ExcelReader.get_Power_Wind_TechnicalDetails, ew.write_Power_Wind_TechnicalDetails),
]


@pytest.mark.parametrize("excel_definition_id, file_path, read, write", combinations)
def test_read_and_write(excel_definition_id, file_path, read, write, tmp_path):
    printer.information(f"Writing '{excel_definition_id}', read from '{file_path}'")

    data = read(file_path, True, True)
    write(data, str(tmp_path))

    printer.information(f"Comparing '{tmp_path}/{excel_definition_id}.xlsx' against source file '{file_path}'")
    filesEqual = ExcelReader.compare_Excels(file_path, f"{tmp_path}/{excel_definition_id}.xlsx", dont_check_formatting=False)

    assert filesEqual, f"Read and/or Write for {excel_definition_id} are faulty - please check!"
