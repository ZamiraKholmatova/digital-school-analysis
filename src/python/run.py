import argparse
import logging
from pathlib import Path

from dsa import SharedModel
from dsa.writers import ReportWriter, BillingReportWriter, RegionReportWriter, SchoolActivityReportWriter
from dsa import Reporter


def get_last_export(path):
    with open(path, "r") as last_export:
        last_export = last_export.read().strip()
    return last_export


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--billing", default=None)
    parser.add_argument("--student_grades", default=None)
    parser.add_argument("--statistics_type", default=None)
    parser.add_argument("--external_system", default=None)
    parser.add_argument("--profile_educational_institution", default=None)
    parser.add_argument("--course_structure", default=None)
    parser.add_argument("--course_structure_foxford", default=None)
    parser.add_argument("--course_structure_meo", default=None)
    parser.add_argument("--course_structure_uchi", default=None)
    parser.add_argument("--course_types", default=None)
    parser.add_argument("--course_statistics", default=None)
    parser.add_argument("--course_statistics_foxford", default=None)
    parser.add_argument("--course_statistics_uchi", default=None)
    parser.add_argument("--course_statistics_meo", default=None)
    parser.add_argument("--region_info", default=None)
    parser.add_argument("--educational_institution", default=None)
    parser.add_argument("--last_export", default=None)
    parser.add_argument("--html_path", default=None)
    parser.add_argument("--resources_path", default=None)
    parser.add_argument("--freeze_date", default=None, type=str)
    parser.add_argument("--minute_activity", action="store_true")
    args = parser.parse_args()
    return args


def main():
    args = parse_arguments()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(module)s:%(lineno)d:%(message)s")

    shared_model = SharedModel(args)
    reporter = Reporter(args, shared_model, freeze_date=args.freeze_date)
    reports = reporter.get_reports()

    if reports is not None:
        logging.info("Preparing new report")
        last_export = get_last_export(args.last_export)
        report_writer = ReportWriter(last_export, args.html_path, queries_path=Path(args.course_types).parent)

        report_writer.add_sheet("Сводная", reports.user_report)

        for provider, table in reports.courses_report.groupby("Платформа"):
            report_writer.add_sheet(
                report_writer.normalize_worksheet_name(provider),
                table, {"long_column": 1}
            )

        report_writer.add_sheet("Статистика активности", reports.convergence_report)
        report_writer.add_sheet(
            "Активно и подтвержд. по школам", reports.school_active_students_report, {"long_column": 1}
        )

        report_writer.add_sheet(
            "Активно и подтвержд. по рег.", reports.region_active_students_report, {"long_column": 1}
        )

        # report_writer.add_extra_sheets()

        report_writer.save_report()

        # common_billing_report_writer = CommonBillingReportWriter(last_export, html_path, queries_path=Path(course_types).parent)
        # common_billing_report_writer.add_sheet(
        #     "ЦОК",
        #     reports.billing_report.query("`Наименование образовательной цифровой площадки` != 'Учи.Ру'")
        # )
        # common_billing_report_writer.save_report()

        for platform in reports.billing_report["Наименование образовательной цифровой площадки"].unique():
            billing_report_writer = BillingReportWriter(last_export, args.html_path, queries_path=Path(args.course_types).parent)
            billing_report_writer.add_billing_info_as_sheets(
                reports.billing_report.query(f"`Наименование образовательной цифровой площадки` == '{platform}'")
            )
            billing_report_writer.set_name("billing_report_uchi")
            billing_report_writer.save_report()

        region_report_writer = RegionReportWriter(last_export, args.html_path, queries_path=Path(args.course_types).parent)
        region_report_writer.add_region_info_as_sheets(reports.school_active_students_report)
        region_report_writer.save_report()

        school_report_writer = SchoolActivityReportWriter(last_export, args.html_path, queries_path=Path(args.course_types).parent)
        school_report_writer.add_sheet(
            "Активно и подтвержд. по школам", reports.school_active_students_report, {"long_column": 1}
        )
        school_report_writer.save_report()
    else:
        logging.info("No new data")
    logging.info("Finished")

if __name__ == "__main__":
    main()
