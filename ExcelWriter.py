import os
import time
import xml.etree.ElementTree as ET
from copy import copy, deepcopy

import numpy as np
import openpyxl
import pandas as pd
from openpyxl.utils.dataframe import dataframe_to_rows

import InOutModule.ExcelDefinition
from InOutModule import ExcelReader, ExcelDefinition
from InOutModule.ExcelDefinition import CellStyle, Alignment, Font, Color, Text, Column, NumberFormat, ExcelDefinition
from InOutModule.printer import Printer

package_directory_ExcelWriter = os.path.dirname(os.path.abspath(__file__))

printer = Printer.getInstance()


class ExcelWriter:
    def __init__(self, excel_definitions_path: str):
        """
        Initialize the ExcelWriter with the XML root element.

        :param excel_definitions_path: Path to the ExcelDefinitions.xml file.
        """

        self.excel_definitions_path = excel_definitions_path
        self.xml_tree = ET.parse(excel_definitions_path)
        self.xml_root = self.xml_tree.getroot()
        self.alignments = Alignment.dict_from_xml(self.xml_root.find("Alignments"))
        self.number_formats = NumberFormat.dict_from_xml(self.xml_root.find("NumberFormats"))
        self.colors = Color.dict_from_xml(self.xml_root.find("Colors"))
        self.fonts = Font.dict_from_xml(self.xml_root.find("Fonts"), self.colors)
        self.texts = Text.dict_from_xml(self.xml_root.find("Texts"))
        self.cell_styles = CellStyle.dict_from_xml(self.xml_root.find("CellStyles"), self.fonts, self.colors, self.number_formats, self.alignments)
        self.columns = Column.dict_from_xml(self.xml_root.find("Columns"), self.cell_styles) | Column.dict_from_xml(self.xml_root.find("PivotColumns"), self.cell_styles)
        self.excel_definitions = ExcelDefinition.dict_from_xml(self.xml_root.find("ExcelDefinitions"), self.columns, self.colors, self.cell_styles)
        pass

    @staticmethod
    def __setCellStyle(cell_style: InOutModule.ExcelDefinition.CellStyle, target_cell: openpyxl.cell.cell):
        """
        Set the cell style of a target cell based on the given cell style.

        :param cell_style: CellStyle object containing the style properties.
        :param target_cell: The target cell to which the style will be applied.
        :return: None
        """

        if cell_style.font is not None:
            target_cell.font = openpyxl.styles.fonts.Font(**cell_style.font.__dict__)
        if cell_style.fill is not None:
            target_cell.fill = cell_style.fill
        if cell_style.number_format is not None:
            target_cell.number_format = copy(cell_style.number_format)
        if cell_style.alignment is not None:
            target_cell.alignment = openpyxl.styles.Alignment(**cell_style.alignment.__dict__)

    def _write_Excel_from_definition(self, data: pd.DataFrame, folder_path: str, excel_definition_id: str) -> None:
        """
        Write the given data to an Excel file based on the provided Excel definition.

        :param data: DataFrame containing the data to be written to Excel.
        :param folder_path: Folder path where the Excel file will be saved.
        :param excel_definition_id: ID of the Excel definition to be used.
        :return: None
        """
        start_time = time.time()
        if excel_definition_id not in self.excel_definitions:
            raise ValueError(f"Excel definition '{excel_definition_id}' not found in the definitions. Please define it in the XML file.")
        excel_definition = self.excel_definitions[excel_definition_id]
        wb = openpyxl.Workbook()
        scenarios = data["scenario"].unique()

        data = data.copy()  # Create a copy of the DataFrame to avoid modifying the original data

        # Prepare columns if data should be pivoted
        pivot_columns = []
        target_column = None
        target_column_index = None
        for i, column in enumerate(excel_definition.columns):
            if column.pivoted:
                if target_column is not None:
                    raise ValueError(f"Excel definition '{excel_definition_id}' has (at least) two pivot columns defined: '{target_column.db_name}' and '{column.db_name}'. Only one pivot column is allowed.")
                target_column = column
                target_column_index = i
            else:
                if column.db_name != "NOEXCL":  # Skip first column if it is the (empty and thus unused) placeholder for the excl column
                    pivot_columns.append(column.db_name)

        if target_column is not None:
            data.reset_index(inplace=True)
            data = data.pivot(index=pivot_columns + ["scenario"], columns=target_column.db_name, values="value")
            excel_definition.columns.remove(target_column)  # Remove the pivot column from the list of columns
            for i, column in enumerate(data.columns):
                col_definition = copy(target_column)
                col_definition.db_name = column
                col_definition.readable_name = column
                if i != 0:  # Remove description for all but the first pivoted column
                    col_definition.description = None
                excel_definition.columns.append(col_definition)  # Add the new column definition to the list of columns

            data.reset_index(inplace=True)

        for scenario_index, scenario in enumerate(scenarios):
            scenario_data = data[data["scenario"] == scenario]

            if scenario_index == 0:
                ws = wb.active
                ws.title = scenario
            else:
                ws = wb.create_sheet(title=scenario)

            # Set sheet properties
            ws.sheet_properties.tabColor = '008080'  # Set tab color
            ws.sheet_view.showGridLines = False  # Hide grid lines
            ws.freeze_panes = "C8"  # Freeze panes at row 8 (below the header)

            # Prepare row heights
            ws.row_dimensions[5].height = excel_definition.description_row_height
            ws.row_dimensions[6].height = 30  # Standard for database behavior row

            # Prepare header columns
            for i, column in enumerate(excel_definition.columns):
                if i == 1:  # Column with title text & 'Format' text
                    ws.cell(row=1, column=i + 1, value=excel_definition.sheet_header)
                    ExcelWriter.__setCellStyle(self.cell_styles["title"], ws.cell(row=1, column=i + 1))
                    ws.cell(row=2, column=i + 1, value="Format:")
                    ExcelWriter.__setCellStyle(self.cell_styles["formatDescription"], ws.cell(row=2, column=i + 1))
                elif i == 2:  # Column with format value
                    ExcelWriter.__setCellStyle(self.cell_styles["header"], ws.cell(row=1, column=i + 1))
                    ws.cell(row=2, column=i + 1, value=excel_definition.version)
                    ExcelWriter.__setCellStyle(self.cell_styles["formatValue"], ws.cell(row=2, column=i + 1))
                else:  # Standard header column (no text, just setting the color)
                    ExcelWriter.__setCellStyle(self.cell_styles["header"], ws.cell(row=1, column=i + 1))
                # Set column width
                if column.column_width is not None:
                    ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = column.column_width
                # Set notes
                if i == 0 and column.db_name == "excl":
                    ws.cell(row=3, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderExclDescription"], "")
                if i == 1:
                    ws.cell(row=3, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderReadableName"], "")
                    ws.cell(row=4, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderValueSpecifierDB"], "")
                    ws.cell(row=5, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderDescription"], "")
                    ws.cell(row=6, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderDBBehavior"], "")
                    ws.cell(row=7, column=i + 1).comment = openpyxl.comments.Comment(self.texts["colHeaderUnit"], "")

                if column.db_name != "NOEXCL":  # Skip first column if it is the (empty and thus unused) placeholder for the excl column
                    # Readable name
                    ws.cell(row=3, column=i + 1, value=column.readable_name)
                    ExcelWriter.__setCellStyle(self.cell_styles["readableName"], ws.cell(row=3, column=i + 1))

                    # Database name
                    ws.cell(row=4, column=i + 1, value=column.db_name)
                    ExcelWriter.__setCellStyle(self.cell_styles["dbName"], ws.cell(row=4, column=i + 1))

                    # Description
                    ws.cell(row=5, column=i + 1, value=column.description)
                    if i != target_column_index:
                        ExcelWriter.__setCellStyle(self.cell_styles["description"], ws.cell(row=5, column=i + 1))
                    else:  # If the column is a pivoted column, set the style without wrapping text
                        cell_style_withou_wrap_text = deepcopy(self.cell_styles["description"])
                        cell_style_withou_wrap_text.alignment.wrap_text = False
                        ExcelWriter.__setCellStyle(cell_style_withou_wrap_text, ws.cell(row=5, column=i + 1))

                    # Database behavior
                    if i != 0:  # Skip db-behavior for the first column (excl)
                        ws.cell(row=6, column=i + 1, value=column.get_db_behavior(self.texts))
                    ExcelWriter.__setCellStyle(self.cell_styles["dbBehavior"], ws.cell(row=6, column=i + 1))

                    # Unit
                    ws.cell(row=7, column=i + 1, value=column.unit)
                    ExcelWriter.__setCellStyle(self.cell_styles["unit"], ws.cell(row=7, column=i + 1))

            # Write data to Excel
            scenario_data = scenario_data.reset_index()
            for i, values in scenario_data.iterrows():
                for j, col in enumerate(excel_definition.columns):
                    if col.readable_name is None and j == 0: continue  # Skip first column if it is empty, since it is the (unused) placeholder for the excl column
                    if col.db_name == "excl":  # Excl. column is written by placing 'X' in lines which should be excluded
                        ws.cell(row=i + 8, column=j + 1, value='X' if isinstance(values[col.db_name], str) or not np.isnan(values[col.db_name]) else None)
                    else:
                        ws.cell(row=i + 8, column=j + 1, value=values[col.db_name])
                    ExcelWriter.__setCellStyle(col.cell_style, ws.cell(row=i + 8, column=j + 1))

        path = folder_path + "/" + excel_definition.file_name + ".xlsx"
        if not os.path.exists(os.path.dirname(path)) and os.path.dirname(path) != "":
            printer.information(f"Creating folder '{os.path.dirname(path)}'")
            os.makedirs(os.path.dirname(path))  # Create folder if it does not exist
        wb.save(path)
        printer.information(f"Saved Excel file to '{path}' after {time.time() - start_time:.2f} seconds")

    def write_dPower_Hindex(self, dPower_Hindex: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_Hindex DataFrame to an Excel file in LEGO format.
        :param dPower_Hindex: DataFrame containing the dPower_Hindex data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_Hindex, folder_path, "Power_Hindex")

    def write_dPower_WeightsRP(self, dPower_WeightsRP: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_WeightsRP DataFrame to an Excel file in LEGO format.
        :param dPower_WeightsRP: DataFrame containing the dPower_WeightsRP data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_WeightsRP, folder_path, "Power_WeightsRP")

    def write_dPower_WeightsK(self, dPower_WeightsK: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_WeightsK DataFrame to an Excel file in LEGO format.
        :param dPower_WeightsK: DataFrame containing the dPower_WeightsK data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_WeightsK, folder_path, "Power_WeightsK")

    def write_dPower_BusInfo(self, dPower_BusInfo: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_BusInfo DataFrame to an Excel file in LEGO format.
        :param dPower_BusInfo: DataFrame containing the dPower_BusInfo data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_BusInfo, folder_path, "Power_BusInfo")

    def write_dPower_Network(self, dPower_Network: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_Network DataFrame to an Excel file in LEGO format.
        :param dPower_Network: DataFrame containing the dPower_Network data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_Network, folder_path, "Power_Network")

    def write_dPower_Demand(self, dPower_Demand: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_Demand DataFrame to an Excel file in LEGO format.
        :param dPower_Demand: DataFrame containing the dPower_Demand data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """

        self._write_Excel_from_definition(dPower_Demand, folder_path, "Power_Demand")

    def write_dPower_ThermalGen(self, dPower_ThermalGen: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_ThermalGen DataFrame to an Excel file in LEGO format.
        :param dPower_ThermalGen: DataFrame containing the dPower_ThermalGen data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_ThermalGen, folder_path, "Power_ThermalGen")

    def write_VRES(self, dPower_VRES: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_VRES DataFrame to an Excel file in LEGO format.
        :param dPower_VRES: DataFrame containing the dPower_VRES data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_VRES, folder_path, "Power_VRES")

    def write_VRESProfiles(self, dPower_VRESProfiles: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dPower_VRESProfiles DataFrame to an Excel file in LEGO format.
        :param dPower_VRESProfiles: DataFrame containing the dPower_VRESProfiles data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dPower_VRESProfiles, folder_path, "Power_VRESProfiles")

    def write_dData_Sources(self, dData_Sources: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dData_Sources DataFrame to an Excel file in LEGO format.
        :param dData_Sources: DataFrame containing the dData_Sources data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dData_Sources, folder_path, "Data_Sources")

    def write_dData_Packages(self, dData_Packages: pd.DataFrame, folder_path: str) -> None:
        """
        Write the dData_Packages DataFrame to an Excel file in LEGO format.
        :param dData_Packages: DataFrame containing the dData_Packages data.
        :param folder_path: Path to the folder where the Excel file will be saved.
        :return: None
        """
        self._write_Excel_from_definition(dData_Packages, folder_path, "Data_Packages")


if __name__ == "__main__":
    printer.set_width(300)

    ew = ExcelWriter("ExcelDefinitions.xml")

    combinations = [
        ("Power_Hindex", "examples/Power_Hindex.xlsx", ExcelReader.get_dPower_Hindex, ew.write_dPower_Hindex),
        ("Power_WeightsRP", "examples/Power_WeightsRP.xlsx", ExcelReader.get_dPower_WeightsRP, ew.write_dPower_WeightsRP),
        ("Power_WeightsK", "examples/Power_WeightsK.xlsx", ExcelReader.get_dPower_WeightsK, ew.write_dPower_WeightsK),
        ("Power_BusInfo", "examples/Power_BusInfo.xlsx", ExcelReader.get_dPower_BusInfo, ew.write_dPower_BusInfo),
        ("Power_Network", "examples/Power_Network.xlsx", ExcelReader.get_dPower_Network, ew.write_dPower_Network),
        ("Power_Demand", "examples/Power_Demand.xlsx", ExcelReader.get_dPower_Demand, ew.write_dPower_Demand),
        ("Power_ThermalGen", "examples/Power_ThermalGen.xlsx", ExcelReader.get_dPower_ThermalGen, ew.write_dPower_ThermalGen),
        ("Power_VRES", "examples/Power_VRES.xlsx", ExcelReader.get_dPower_VRES, ew.write_VRES),
        ("Power_VRESProfiles", "examples/Power_VRESProfiles.xlsx", ExcelReader.get_dPower_VRESProfiles, ew.write_VRESProfiles),
        ("Data_Sources", "examples/Data_Sources.xlsx", ExcelReader.get_dData_Sources, ew.write_dData_Sources),
        ("Data_Packages", "examples/Data_Packages.xlsx", ExcelReader.get_dData_Packages, ew.write_dData_Packages),
    ]

    for excel_definition_id, file_path, read, write in combinations:
        printer.information(f"Writing '{excel_definition_id}', read from '{file_path}'")
        data = read(file_path, True, True)
        write(data, "examples/output")

        printer.information(f"Comparing '{excel_definition_id}' against source file '{file_path}'")
        filesEqual = ExcelReader.compare_Excels(file_path, f"examples/output/{excel_definition_id}.xlsx")
        if filesEqual:
            printer.success(f"Excel files are equal")
        else:
            printer.error(f"Excel files are NOT equal - see above for details")

        printer.separator()
