import logging
import os
from pathlib import Path
from shutil import rmtree
from zipfile import ZipFile

import pandas as pd
from tqdm import tqdm

html_string = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отчет по платформам</title>
<style>
body {{font-family: Arial;}}

/* Style the tab */
.tab {{
  overflow: hidden;
  border: 1px solid #ccc;
  background-color: #f1f1f1;
}}

/* Style the buttons inside the tab */
.tab button {{
  background-color: inherit;
  float: left;
  border: none;
  outline: none;
  cursor: pointer;
  padding: 14px 16px;
  transition: 0.3s;
  font-size: 17px;
}}

/* Change background color of buttons on hover */
.tab button:hover {{
  background-color: #ddd;
}}

/* Create an active/current tablink class */
.tab button.active {{
  background-color: #ccc;
}}

/* Style the tab content */
.tabcontent {{
  display: none;
  padding: 6px 12px;
  border: 1px solid #ccc;
  border-top: none;
}}

/* includes alternating gray and white with on-hover color */

.striped {{
    font-size: 11pt; 
    font-family: Arial;
    border-collapse: collapse; 
    border: 1px solid silver;

}}

.striped td, th {{
    padding: 5px;
}}

.striped tr:nth-child(even) {{
    background: #E0E0E0;
}}

.striped tr:hover {{
    background: silver;
    cursor: pointer;
}}
</style>
</head>
<body>

<p>
    <a href="{xlsx_location}">Скачать XLSX</a>
</p>

{tabdefinitions}

{tabcontent}

<script>
function openCity(evt, cityName) {{
  var i, tabcontent, tablinks;
  tabcontent = document.getElementsByClassName("tabcontent");
  for (i = 0; i < tabcontent.length; i++) {{
    tabcontent[i].style.display = "none";
  }}
  tablinks = document.getElementsByClassName("tablinks");
  for (i = 0; i < tablinks.length; i++) {{
    tablinks[i].className = tablinks[i].className.replace(" active", "");
  }}
  document.getElementById(cityName).style.display = "block";
  evt.currentTarget.className += " active";
}}

// Get the element with id="defaultOpen" and click on it
document.getElementById("defaultOpen").click();
</script>

</body>
</html> 
"""


class ReportWriter:
    def __init__(self, last_export, html_path, queries_path: Path):
        self.last_export = last_export
        self.html_path = Path(html_path)
        # self.special_files = special_files
        self.queries_path = queries_path

        self.sheet_names = []
        self.sheet_data = []
        self.sheet_options = []

        self.special_files = {"student_16.11.bz2", "educational_course_statistic_16.11.bz2",
                              "educational_course_type_16.11.bz2", "educational_courses_16.11.bz2",
                              "educational_courses_only_courses_16.11.bz2", "external_system_16.11.bz2", "last_export",
                              "profile_educational_institution_16.11.bz2", "profile_role_16.11.bz2", "role_16.11.bz2",
                              "school_students.bz2", "statistic_type_16.11.bz2", "educational_institution_16.11.bz2"}

    def create_definitions(self, tab_names):
        definitions = ""
        for ind, name in enumerate(tab_names):
            if ind == 0:
                default_tab = ' id="defaultOpen"'
            else:
                default_tab = ""
            definitions += f"""<button class="tablinks" onclick="openCity(event, '{name}')"{default_tab}>{name}</button>\n"""

        definiitons_div = f"""
<div class="tab">
  {definitions}
</div>
        """
        return definiitons_div

    def create_content(self, tab_names, tab_content):
        contents = ""
        for name, content in zip(tab_names, tab_content):
            contents += f"""
<div id="{name}" class="tabcontent">
  {content.to_html(classes="striped", index=False)}
</div>
"""
        return contents

    def write_html(self, sheet_names, sheet_data, sheet_options, name):
        self.html_string = html_string
        html = self.html_string.format(
            xlsx_location=f"{name}.xlsx",
            tabdefinitions=self.create_definitions(sheet_names),
            tabcontent=self.create_content(sheet_names, sheet_data)
        )
        with open(self.html_path.joinpath(f"{name}.html"), "w") as report_html:
            report_html.write(html)

    def format_worksheet(self, workbook, worksheet, data, max_column_len=30, long_column=None):
        format = workbook.add_format({'text_wrap': True, 'num_format': '#,##0.######'})
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'vcenter',
            'align': 'center',
            'fg_color': '#F2F2F2',
            'border': 1
        })
        for ind, col in enumerate(data.columns):
            def get_max_len(data):
                if len(data) == 0:
                    return 15
                return max(data.astype(str).map(len))
            col_width = min(
                max(get_max_len(data[col]), len(col)),
                max_column_len if ind != long_column else max_column_len * 2
            ) + 4
            worksheet.set_column(ind, ind, col_width, format)
            worksheet.write(0, ind, col, header_format)

    def normalize_worksheet_name(self, name):
        prohibited = "[]:*?/\\"
        for c in prohibited:
            name = name.replace(c, " ")
        if len(name) > 31:
            parts = name.split("_")
            name = "_".join([part[:4] + "." for part in parts])
        return name

    def cell_formatter(self, value):
        if pd.isna(value) or isinstance(value, str):
            return value
        else:
            val = float(value)
            if val.is_integer():
                return f"{int(val):,}"
            else:
                return f"{val:,.3f}"

    def write_xlsx(self, sheet_names, sheet_data, sheet_options, name):
        with pd.ExcelWriter(self.html_path.joinpath(f'{name}.xlsx'), engine='xlsxwriter') as writer:
            for sheet_name, data, options in zip(sheet_names, sheet_data, sheet_options):
                # data = data.applymap(self.cell_formatter)
                data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)
                self.format_worksheet(
                    writer.book, writer.sheets[sheet_name], data, long_column=options.get("long_column", None)
                )

    def write_index_html(self, name):
        # <head>
        # <meta http-equiv="refresh" content="0; url={name}.html" />
        html = f"""
<html>
<head>
<meta charset="UTF-8">
<title>Отчет по платформам</title>
</head>
<body>
<p>
Последнее обновление: {self.last_export}
</p>
<p>
<a id="report" href="report_{self.last_export}.xlsx">Отчёт</a>
</p>
<p>
<a id="billing" href="billing_report_{self.last_export}.xlsx">Биллинг</a>
</p>
<p>
<a id="region_arc" href="region_report_{self.last_export}.zip">Отчет по регионам zip</a>
</p>
</body>
</html>
"""
        with open(self.html_path.joinpath("index.html"), "w") as index_html:
            index_html.write(html)

    def add_sheet(self, name, data, options=None):
        if options is None:
            options = {"long_column": None}
        self.sheet_names.append(name)
        self.sheet_data.append(data)
        self.sheet_options.append(options)

    def add_extra_sheets(self):

        max_len, max_width = 1048576, 16384
        for file_path in self.queries_path.iterdir():
            filename = file_path.name
            if not filename.endswith(".bz2") or filename.startswith("___") or filename in self.special_files:
                continue

            data = pd.csv(file_path, compression=None)
            if data.shape[0] >= max_len or data.shape[1] > max_width:
                logging.info(f"Skipping {filename}")
                continue
            data = data.dropna(how="all")

            self.add_sheet(self.normalize_worksheet_name(filename.strip(".bz2")), data)

    def get_report_name(self):
        return f"report_{self.last_export}"

    def save_report(self):
        export_file_name = self.get_report_name()

        self.write_xlsx(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        # self.write_html(self.sheet_names, self.sheet_data, self.sheet_options, export_file_name)
        self.write_index_html(export_file_name)


class CommonBillingReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_report_name(self):
        return f"billing_report_{self.last_export}"


class RegionReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def add_region_info_as_sheets(self, region_report):
        region_index = []

        for ind, (region, data) in enumerate(tqdm(
                region_report.groupby([
                    "Регион"
                ]),
                desc="Preparing billing sheets", leave=False
        )):
            sheet_name = f"{ind + 1}"
            region_index.append({
                "Регион": region,
                "Страница": sheet_name,
            })
            self.add_sheet(sheet_name,
                           data.sort_values(["Всего учеников"], ascending=False),
                           {"long_column": 1})

        course_index_df = pd.DataFrame.from_records(region_index)

        self.sheet_names.insert(0, "Индекс регионов")
        self.sheet_data.insert(0, course_index_df)
        self.sheet_options.insert(0, {"long_column": 1})

    def get_report_name(self):
        return f"region_report_{self.last_export}"

    def save_report(self):
        # export_file_name = self.get_report_name()

        region_report_folder = f"region_report_{self.last_export}"

        save_location = self.html_path.joinpath(region_report_folder)

        if not save_location.is_dir():
            save_location.mkdir()

        for ind, (name, data, options) in enumerate(zip(self.sheet_names, self.sheet_data, self.sheet_options)):
            if ind == 0:
                continue
            region_name = data.iloc[0, 0].replace("/", "")
            export_file_name = os.path.join(region_report_folder, region_name)
            self.write_xlsx(["Данные по региону"], [data], [options], export_file_name)

        save_location = save_location.absolute()

        curr_dir = os.getcwd()
        os.chdir(self.html_path)

        with ZipFile(
            region_report_folder + ".zip",
            mode='w'
        ) as zipper:
            for file in save_location.iterdir():
                if file.name.endswith(".xlsx"):
                    zipper.write(Path(region_report_folder).joinpath(file.name))

        rmtree(save_location)
        os.chdir(curr_dir)


class BillingReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.name = "billing_report_uchi"

    def add_billing_info_as_sheets(self, billing_report):
        course_index = []

        for ind, ((platform, course), data) in enumerate(tqdm(
                billing_report.groupby([
                    "Наименование образовательной цифровой площадки", "Наименование ЦОК"
                ]),
                desc="Preparing billing sheets", leave=False
        )):
            sheet_name = f"{ind + 1}"
            course_index.append({
                "Наименование образовательной цифровой площадки": platform,
                "Наименование ЦОК": course,
                "Страница": sheet_name,
                # "Всего лицензий": len(data)
            })
            self.add_sheet(sheet_name, data, {"long_column": 1})

        course_index_df = pd.DataFrame.from_records(course_index)

        self.sheet_names.insert(0, "Индекс курсов")
        self.sheet_data.insert(0, course_index_df)
        self.sheet_options.insert(0, {"long_column": 1})

    def set_name(self, name):
        self.name = name

    def get_report_name(self):
        return f"{self.name}_{self.last_export}"

    def write_index_html(self, name):
        pass


class SchoolActivityReportWriter(ReportWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_report_name(self):
        return f"active_and_approved_by_schools_{self.last_export}"

    def write_index_html(self, name):
        html = f"""
<html>
<head>
<meta charset="UTF-8">
<title>Отчет по платформам</title>
</head>
<body>
<p>
Последнее обновление: {self.last_export}
</p>
<p>
<a id="region_arc" href="active_and_approved_by_schools_{self.last_export}.xlsx">Отчет по регионам</a>
</p>
</body>
</html>
"""
        with open(self.html_path.joinpath("index_regions.html"), "w") as index_html:
            index_html.write(html)