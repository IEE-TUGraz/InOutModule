import pytest

import ExcelReader as ExcelReader
from ExcelWriter import ExcelWriter
from printer import Printer

printer = Printer.getInstance()

case_study_folder = "data/example/"
ew = ExcelWriter()
combinations = [
    ("Data_Packages", f"{case_study_folder}Data_Packages.xlsx", ExcelReader.get_dData_Packages, ew.write_dData_Packages),
    ("Data_Sources", f"{case_study_folder}Data_Sources.xlsx", ExcelReader.get_dData_Sources, ew.write_dData_Sources),
    ("Global_Scenarios", f"{case_study_folder}Global_Scenarios.xlsx", ExcelReader.get_dGlobal_Scenarios, ew.write_dGlobal_Scenarios),
    ("Power_BusInfo", f"{case_study_folder}Power_BusInfo.xlsx", ExcelReader.get_dPower_BusInfo, ew.write_dPower_BusInfo),
    ("Power_Demand", f"{case_study_folder}Power_Demand.xlsx", ExcelReader.get_dPower_Demand, ew.write_dPower_Demand),
    ("Power_Hindex", f"{case_study_folder}Power_Hindex.xlsx", ExcelReader.get_dPower_Hindex, ew.write_dPower_Hindex),
    ("Power_Network", f"{case_study_folder}Power_Network.xlsx", ExcelReader.get_dPower_Network, ew.write_dPower_Network),
    ("Power_Storage", f"{case_study_folder}Power_Storage.xlsx", ExcelReader.get_dPower_Storage, ew.write_dPower_Storage),
    ("Power_ThermalGen", f"{case_study_folder}Power_ThermalGen.xlsx", ExcelReader.get_dPower_ThermalGen, ew.write_dPower_ThermalGen),
    ("Power_VRES", f"{case_study_folder}Power_VRES.xlsx", ExcelReader.get_dPower_VRES, ew.write_VRES),
    ("Power_VRESProfiles", f"{case_study_folder}Power_VRESProfiles.xlsx", ExcelReader.get_dPower_VRESProfiles, ew.write_VRESProfiles),
    ("Power_WeightsK", f"{case_study_folder}Power_WeightsK.xlsx", ExcelReader.get_dPower_WeightsK, ew.write_dPower_WeightsK),
    ("Power_WeightsRP", f"{case_study_folder}Power_WeightsRP.xlsx", ExcelReader.get_dPower_WeightsRP, ew.write_dPower_WeightsRP),
]


@pytest.mark.parametrize("excel_definition_id, file_path, read, write", combinations)
def test_read_and_write(excel_definition_id, file_path, read, write, tmp_path):
    printer.information(f"Writing '{excel_definition_id}', read from '{file_path}'")

    data = read(file_path, True, True)
    write(data, str(tmp_path))

    printer.information(f"Comparing '{tmp_path}/{excel_definition_id}.xlsx' against source file '{file_path}'")
    filesEqual = ExcelReader.compare_Excels(file_path, f"{tmp_path}/{excel_definition_id}.xlsx", dont_check_formatting=False)

    assert filesEqual, f"Read and/or Write for {excel_definition_id} are faulty - please check!"
