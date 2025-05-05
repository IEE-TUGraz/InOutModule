import typing
from copy import copy

import openpyxl


class DatabaseBehavior:
    FILLED_BY_DATABASE = "Filled automatically by database"
    SCENARIO_DEPENDENT = "Scenario-dependent"
    NO_BEHAVIOR = "-"


class ColumnNote:
    EXCL_DESCRIPTION = "If a line has a value in this column, it is not read in (i.e., does not exist)."
    READABLE_NAME = "Readable name"
    VALUE_SPECIFIER_DB = "Value specifier in database"
    DESCRIPTION = "Description"
    DB_BEHAVIOR = "Details on database behavior"
    UNIT = "Unit or valid values"


class CellStyle:
    def __init__(self, font: openpyxl.styles.fonts.Font,
                 fill: openpyxl.styles.fills.PatternFill,
                 number_format: typing.Optional[str],
                 alignment: openpyxl.styles.alignment.Alignment):
        self.font = font  # openpyxl.styles.fonts.Font
        self.fill = fill  # openpyxl.styles.fills.PatternFill
        self.number_format = number_format  # string?
        self.alignment = alignment  # openpyxl.styles.alignment.Alignment


CellStyle.NONE = CellStyle(None, None, None, None)

# Header & Title
CellStyle.TITLE_CELL = CellStyle(openpyxl.styles.Font(name="Aptos", bold=True, size=18, color="FFFFFF"),
                                 openpyxl.styles.PatternFill(start_color="008080", end_color="008080", fill_type="solid"),
                                 None,
                                 None
                                 )
CellStyle.HEADER_ROW = CellStyle(None,
                                 openpyxl.styles.PatternFill(start_color="008080", end_color="008080", fill_type="solid"),
                                 None,
                                 None
                                 )
CellStyle.FORMAT_DESCRIPTION = CellStyle(openpyxl.styles.Font(name="Aptos", italic=True, size=11),
                                         None,
                                         None,
                                         openpyxl.styles.Alignment(horizontal="right", vertical="center")
                                         )
CellStyle.FORMAT_VALUE = CellStyle(openpyxl.styles.Font(name="Aptos", italic=True, size=11),
                                   None,
                                   None,
                                   None
                                   )

# Column headers
CellStyle.READABLE_NAME = CellStyle(openpyxl.styles.Font(name="Aptos", bold=True, size=11),
                                    openpyxl.styles.PatternFill(start_color="DAEEF3", end_color="DAEEF3", fill_type="solid"),
                                    None,
                                    None
                                    )
CellStyle.DB_NAME = CellStyle(openpyxl.styles.Font(name="Aptos", bold=True, size=11),
                              openpyxl.styles.PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),
                              None,
                              None
                              )
CellStyle.DESCRIPTION = CellStyle(openpyxl.styles.Font(name="Aptos", italic=True, size=11),
                                  openpyxl.styles.PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),
                                  None,
                                  openpyxl.styles.Alignment(wrap_text=True, vertical="top")
                                  )
CellStyle.DB_BEHAVIOR = CellStyle(openpyxl.styles.Font(name="Aptos", italic=True, size=11),
                                  openpyxl.styles.PatternFill(start_color="D9D9D9", end_color="D9D9D9", fill_type="solid"),
                                  None,
                                  openpyxl.styles.Alignment(wrap_text=True, vertical="top")
                                  )
CellStyle.UNIT = CellStyle(openpyxl.styles.Font(name="Aptos", size=11, color="0000FF"),
                           openpyxl.styles.PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),
                           None,
                           openpyxl.styles.Alignment(horizontal="center", vertical="center")
                           )

# Values
CellStyle.VALUE_DB_KEY = CellStyle(openpyxl.styles.Font(name="Aptos", size=11),
                                   openpyxl.styles.PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid"),
                                   None,
                                   openpyxl.styles.Alignment(indent=1)
                                   )

CellStyle.VALUE_GENERAL_NOSCENARIO = CellStyle(openpyxl.styles.Font(name="Aptos", size=11),
                                               openpyxl.styles.PatternFill(start_color="CCFFCC", end_color="CCFFCC", fill_type="solid"),
                                               None,
                                               openpyxl.styles.Alignment(indent=1)
                                               )

CellStyle.VALUE_GENERAL_SCENARIO = copy(CellStyle.VALUE_GENERAL_NOSCENARIO)
CellStyle.VALUE_GENERAL_SCENARIO.fill = openpyxl.styles.PatternFill(start_color="B8CCE4", end_color="B8CCE4", fill_type="solid")

CellStyle.VALUE_GENERAL_NOSCENARIO_BOLD = copy(CellStyle.VALUE_GENERAL_NOSCENARIO)
CellStyle.VALUE_GENERAL_NOSCENARIO_BOLD.font = openpyxl.styles.Font(name="Aptos", size=11, bold=True)

CellStyle.VALUE_GENERAL_SCENARIO_BOLD = copy(CellStyle.VALUE_GENERAL_NOSCENARIO_BOLD)
CellStyle.VALUE_GENERAL_SCENARIO_BOLD.fill = openpyxl.styles.PatternFill(start_color="B8CCE4", end_color="B8CCE4", fill_type="solid")


class ColumnDefinition:
    def __init__(self, readable_name: str, db_name: str, description: str, database_behavior: str, unit: str, column_width: float, column_style: CellStyle):
        """
        Represents a column definition in a spreadsheet.

        :param readable_name: The name of the column as it appears in the spreadsheet.
        :param db_name: The name of the column as it appears in the database.
        :param description: A description of the column's purpose or content.
        :param database_behavior: The behavior of the column in relation to the database.
        :param unit: The unit of measurement for the column's values.
        :param column_width: The width of the columns in the spreadsheet. Can be None (which will keep Excel's default).
        :param column_style: The style of the column in the spreadsheet.
        """
        self.readable_name = readable_name
        self.db_name = db_name
        self.description = description
        self.database_behavior = database_behavior
        self.unit = unit
        self.column_width = column_width + 0.7109375  # Difference between Excel's default font and the shown column width (see https://foss.heptapod.net/openpyxl/openpyxl/-/issues/293)
        self.column_style = column_style


ColumnDefinition.EXCL = ColumnDefinition("Excl.", "excl", "", "", "", 4.86, CellStyle.VALUE_GENERAL_SCENARIO_BOLD)
ColumnDefinition.NO_EXCL = ColumnDefinition("", "", "", "", "", 4.86, CellStyle.NONE)
ColumnDefinition.ID = ColumnDefinition("Database ID", "id", "ID within database", DatabaseBehavior.FILLED_BY_DATABASE, "[db-key]", 19.57, CellStyle.VALUE_DB_KEY)
ColumnDefinition.P = ColumnDefinition("p", "p", "Hour of the year", DatabaseBehavior.NO_BEHAVIOR, "[p]", 19.0, CellStyle.VALUE_GENERAL_NOSCENARIO)
ColumnDefinition.RP = ColumnDefinition("rp", "rp", "Representative period for this hour", DatabaseBehavior.SCENARIO_DEPENDENT, "[rp]", 19.0, CellStyle.VALUE_GENERAL_SCENARIO)
ColumnDefinition.K = ColumnDefinition("k", "k", "Representative hour in rp for this hour", DatabaseBehavior.SCENARIO_DEPENDENT, "[k]", 19.0, CellStyle.VALUE_GENERAL_SCENARIO)
ColumnDefinition.DATAPACKAGE = ColumnDefinition("Data Package", "dataPackage", "Which package this belongs to", DatabaseBehavior.SCENARIO_DEPENDENT, "[DataPackage]", 23.86, CellStyle.VALUE_GENERAL_SCENARIO)
ColumnDefinition.DATASOURCE = ColumnDefinition("Data Source", "dataSource", "Where the data for the entry comes from", DatabaseBehavior.SCENARIO_DEPENDENT, "[DataSource]", 23.86, CellStyle.VALUE_GENERAL_SCENARIO)


class ExcelDefinition:
    def __init__(self, file_name: str, sheet_header: str, version: str, has_excl_column: bool, columns: list[ColumnDefinition], description_row_height: float):
        """
        Represents a configuration for a spreadsheet.

        :param file_name: The name of the file.
        :param sheet_header: The header of the spreadsheet.
        :param version: The version identifier of the spreadsheet configuration.
        :param has_excl_column: Flag indicating whether the spreadsheet includes an
            exclusion column.
        :param columns: List of columns of the spreadsheet.
        :param description_row_height: Height of the description row in the spreadsheet.
        """
        self.file_name = file_name
        self.sheet_header = sheet_header
        self.version = version
        self.has_excl_column = has_excl_column
        self.columns = columns
        self.description_row_height = description_row_height

        if has_excl_column:
            self.columns.insert(0, ColumnDefinition.EXCL)
        else:
            self.columns.insert(0, ColumnDefinition.NO_EXCL)


ExcelDefinition.POWER_HINDEX = ExcelDefinition(
    "Power_Hindex",
    "Power - Relation among periods and representative periods",
    "v0.1.0",
    False,
    [
        ColumnDefinition.ID,
        ColumnDefinition.P,
        ColumnDefinition.RP,
        ColumnDefinition.K,
        ColumnDefinition.DATAPACKAGE,
        ColumnDefinition.DATASOURCE,
    ],
    30.0
)
