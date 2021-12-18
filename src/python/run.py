import argparse

from dsa import SharedModel


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

    SharedModel(args)

if __name__ == "__main__":
    main()