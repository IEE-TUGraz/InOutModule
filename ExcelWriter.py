import os
import time
from copy import copy

import numpy as np
import openpyxl
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

import InOutModule.ExcelDefinition
from InOutModule import ExcelReader
from InOutModule.ExcelDefinition import ExcelDefinition, CellStyle, ColumnNote
from InOutModule.printer import Printer

package_directory_ExcelWriter = os.path.dirname(os.path.abspath(__file__))

printer = Printer.getInstance()


def __checkFilepath(given_file_path):
    if not given_file_path.endswith(".xlsx"):
        print("File extension is not .xlsx, appending .xlsx")
        given_file_path += ".xlsx"
        print("File will be saved as " + given_file_path)
    return given_file_path


def __copyCellStyle(origin, target):
    target.font = copy(origin.font)
    target.fill = copy(origin.fill)
    target.number_format = copy(origin.number_format)
    target.alignment = copy(origin.alignment)


def __setCellStyle(cell_style: InOutModule.ExcelDefinition.CellStyle, target_cell: openpyxl.cell.cell):
    """
    Set the cell style of a target cell based on the given cell style.

    :param cell_style: CellStyle object containing the style properties.
    :param target_cell: The target cell to which the style will be applied.
    :return: None
    """

    if cell_style.font is not None:
        target_cell.font = cell_style.font
    if cell_style.fill is not None:
        target_cell.fill = cell_style.fill
    if cell_style.number_format is not None:
        target_cell.number_format = cell_style.number_format
    if cell_style.alignment is not None:
        target_cell.alignment = cell_style.alignment


def _write_Excel_from_definition(data: pd.DataFrame, folder_path: str, excel_definition: ExcelDefinition):
    """
    Write the given data to an Excel file based on the provided Excel definition.
    :param data: DataFrame containing the data to be written to Excel.
    :param folder_path: Folder path where the Excel file will be saved.
    :param excel_definition: ExcelDefinition object containing the configuration for the Excel file.
    :return: None
    """
    start_time = time.time()
    wb = openpyxl.Workbook()

    scenarios = data["scenario"].unique()

    for scenario_index, scenario in enumerate(scenarios):
        scenario_data = data[data["scenario"] == scenario]

        if scenario_index == 0:  # Use existing sheet for first scenario
            ws = wb.active
            ws.title = "ScenarioA"
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
                __setCellStyle(CellStyle.TITLE_CELL, ws.cell(row=1, column=i + 1))

                ws.cell(row=2, column=i + 1, value="Format:")
                __setCellStyle(CellStyle.FORMAT_DESCRIPTION, ws.cell(row=2, column=i + 1))
            elif i == 2:  # Column with format value
                __setCellStyle(CellStyle.HEADER_ROW, ws.cell(row=1, column=i + 1))

                ws.cell(row=2, column=i + 1, value=excel_definition.version)
                __setCellStyle(CellStyle.FORMAT_VALUE, ws.cell(row=2, column=i + 1))
            else:  # Standard header column (no text, just setting the color)
                __setCellStyle(CellStyle.HEADER_ROW, ws.cell(row=1, column=i + 1))

            # Set column width
            if column.column_width is not None:
                ws.column_dimensions[openpyxl.utils.get_column_letter(i + 1)].width = column.column_width

            # Set notes
            if i == 0 and excel_definition.has_excl_column:
                ws.cell(row=3, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.EXCL_DESCRIPTION, "")
            elif i == 1:
                ws.cell(row=3, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.READABLE_NAME, "")
                ws.cell(row=4, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.VALUE_SPECIFIER_DB, "")
                ws.cell(row=5, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.DESCRIPTION, "")
                ws.cell(row=6, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.DB_BEHAVIOR, "")
                ws.cell(row=7, column=i + 1).comment = openpyxl.comments.Comment(ColumnNote.UNIT, "")

            # Set header information for each column
            if i == 0 and not excel_definition.has_excl_column:
                continue
            else:
                # Readable name
                ws.cell(row=3, column=i + 1, value=column.readable_name)
                __setCellStyle(CellStyle.READABLE_NAME, ws.cell(row=3, column=i + 1))

                # Database name
                ws.cell(row=4, column=i + 1, value=column.db_name)
                __setCellStyle(CellStyle.DB_NAME, ws.cell(row=4, column=i + 1))

                # Description
                ws.cell(row=5, column=i + 1, value=column.description)
                __setCellStyle(CellStyle.DESCRIPTION, ws.cell(row=5, column=i + 1))

                # Database behavior
                ws.cell(row=6, column=i + 1, value=column.database_behavior)
                __setCellStyle(CellStyle.DB_BEHAVIOR, ws.cell(row=6, column=i + 1))

                # Unit
                ws.cell(row=7, column=i + 1, value=column.unit)
                __setCellStyle(CellStyle.UNIT, ws.cell(row=7, column=i + 1))

        # Write data to Excel
        scenario_data = scenario_data.reset_index()
        for i, values in scenario_data.iterrows():
            for j, col in enumerate(excel_definition.columns):
                if col.db_name == "" and j == 0: continue  # Skip first column if it is empty, since it is the (unused) placeholder for the excl column
                ws.cell(row=i + 8, column=j + 1, value=values[col.db_name])
                __setCellStyle(col.column_style, ws.cell(row=i + 8, column=j + 1))

    path = folder_path + "/" + excel_definition.file_name + ".xlsx"
    if not os.path.exists(os.path.dirname(path)) and os.path.dirname(path) != "":
        printer.information(f"Creating folder '{os.path.dirname(path)}'")
        os.makedirs(os.path.dirname(path))  # Create folder if it does not exist
    wb.save(path)
    printer.information(f"Saved Excel file to '{path}' after {time.time() - start_time:.2f} seconds")


def write_dPower_Hindex(dPower_Hindex: pd.DataFrame, folder_path: str):
    """
    Write the dPower_Hindex DataFrame to an Excel file in LEGO format.
    :param dPower_Hindex: DataFrame containing the dPower_Hindex data.
    :param folder_path: Path to the folder where the Excel file will be saved.
    :return: None
    """
    _write_Excel_from_definition(dPower_Hindex, folder_path, ExcelDefinition.POWER_HINDEX)


def write_VRESProfiles(data: pd.DataFrame, file_path: str):
    templateName = "Power_VRESProfiles"

    # Get file paths
    file_path = __checkFilepath(file_path)
    templatePath = f"{package_directory_ExcelWriter}/templates/{templateName}-template.xlsx"
    ExcelReader.__check_LEGOExcel_version(templatePath, "v0.0.3")

    # Load template workbook
    templateWorkbook = load_workbook(templatePath)
    targetSheet = templateWorkbook.active

    # Get cells from template sheet for style templates
    rowTemplate_id = targetSheet.cell(row=8, column=2)  # Cell B8
    rowTemplate_rp = targetSheet.cell(row=8, column=3)  # Cell C8
    rowTemplate_g = targetSheet.cell(row=8, column=4)  # Cell D8
    rowTemplate_dataPackage = targetSheet.cell(row=8, column=5)  # Cell E8
    rowTemplate_dataSource = targetSheet.cell(row=8, column=6)  # Cell F8
    rowTemplate_values = targetSheet.cell(row=8, column=7)  # Cell G8

    colTemplate_readableName = targetSheet.cell(row=3, column=7)  # Cell G3
    colTemplate_dbName = targetSheet.cell(row=4, column=7)  # Cell G4
    colTemplate_description = targetSheet.cell(row=5, column=7)  # Cell G5
    colTemplate_dbBehavior = targetSheet.cell(row=6, column=7)  # Cell G6
    colTemplate_unit = targetSheet.cell(row=7, column=7)  # Cell G7

    # Data preparation
    pivot_df = data.reset_index()
    pivot_df.insert(0, "empty", np.nan)  # Insert empty column to match the template
    pivot_df = pivot_df.pivot(index=["empty", "id", "rp", "g", "dataPackage", "dataSource"], columns="k", values="Capacity")  # Pivot table

    # Print column names to cells E3, F3, G3, ...
    for i, column in enumerate(pivot_df.columns):
        targetSheet.cell(row=3, column=i + 7, value=column)
        __copyCellStyle(colTemplate_readableName, targetSheet.cell(row=3, column=i + 7))
        targetSheet.cell(row=4, column=i + 7, value=column)
        __copyCellStyle(colTemplate_dbName, targetSheet.cell(row=4, column=i + 7))
        __copyCellStyle(colTemplate_description, targetSheet.cell(row=5, column=i + 7))
        __copyCellStyle(colTemplate_dbBehavior, targetSheet.cell(row=6, column=i + 7))

        targetSheet.cell(row=7, column=i + 7, value=colTemplate_unit.value)
        __copyCellStyle(colTemplate_unit, targetSheet.cell(row=7, column=i + 7))

    # Write data to Excel file
    for i, values in enumerate(dataframe_to_rows(pivot_df, index=True, header=False)):
        if i == 0:  # Skip first row (which is only column names)
            continue
        row = targetSheet[i + 7]  # Target row in Excel sheet
        for j, cell in enumerate(row):
            cell.value = values[j]
            match cell.column:
                case 1:
                    continue
                case 2:
                    __copyCellStyle(rowTemplate_id, cell)
                case 3:
                    __copyCellStyle(rowTemplate_rp, cell)
                case 4:
                    __copyCellStyle(rowTemplate_g, cell)
                case 5:
                    __copyCellStyle(rowTemplate_dataPackage, cell)
                case 6:
                    __copyCellStyle(rowTemplate_dataSource, cell)
                case _:
                    __copyCellStyle(rowTemplate_values, cell)

    if not os.path.exists(os.path.dirname(file_path)) and os.path.dirname(file_path) != "":
        os.makedirs(os.path.dirname(file_path))  # Create folder if it does not exist
    templateWorkbook.save(file_path)


if __name__ == "__main__":
    data = ExcelReader.get_dPower_VRESProfiles("examples/Power_VRESProfiles.xlsx")  # Read in example data
    write_VRESProfiles(data, "examples/output/Power_VRESProfiles_output.xlsx")  # Write data to Excel file
    pass
