import os
from copy import copy

import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

from InOutModule import ExcelReader

package_directory_ExcelWriter = os.path.dirname(os.path.abspath(__file__))


def __checkFilepath(given_file_path):
    if not given_file_path.endswith(".xlsx"):
        print("File extension is not .xlsx, appending .xlsx")
        given_file_path += ".xlsx"
        print("File will be saved as " + given_file_path)
    return given_file_path


def __copyCellStyle(origin, target):
    target.font = copy(origin.font)
    target.border = copy(origin.border)
    target.fill = copy(origin.fill)
    target.number_format = copy(origin.number_format)
    target.protection = copy(origin.protection)
    target.alignment = copy(origin.alignment)


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

    if not os.path.exists(os.path.dirname(file_path)):
        os.makedirs(os.path.dirname(file_path))  # Create folder if it does not exist
    templateWorkbook.save(file_path)


if __name__ == "__main__":
    data = ExcelReader.get_dPower_VRESProfiles("examples/Power_VRESProfiles.xlsx")  # Read in example data
    write_VRESProfiles(data, "examples/output/Power_VRESProfiles_output.xlsx")  # Write data to Excel file
    pass
