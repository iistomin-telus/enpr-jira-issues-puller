import os.path
from datetime import datetime, timezone

from dateutil import parser
import json
from typing import Any, Dict


class JsonPathMatcher:

    def __init__(self, data: Dict):
        self.data = data

    def match(self, expr: str, fallback_value: Any = None) -> Any:
        from jsonpath_ng.ext import parse
        jsonpath = parse(expr)
        match = jsonpath.find(self.data)
        if len(match) != 0:
            return match[0].value
        else:
            return fallback_value


class IssueData(Dict):

    def __init__(self, data: Dict):
        super().__init__(data)

    def get_field(self, field: str, fallback_value: Any = None) -> Any:
        return if_not_none(if_not_none(self.get("fields", None), {}).get(field, fallback_value), fallback_value)


def if_not_none(value: Any, fallback_value: Any) -> Any:
    if value:
        return value
    else:
        return fallback_value


def transform_user(data: Dict) -> Dict:
    if data is None:
        return None
    return {
        "key": data.get("key", None),
        "name": data.get("name", None),
        "accountId": data.get("accountId", None),
        "emailAddress": data.get("emailAddress", None),
        "displayName": data.get("displayName", None),
    }


def transform_project(data: Dict) -> Dict:
    return {
        "id": data.get("id"),
        "key": data.get("key"),
        "name": data.get("name", None),
        "email": data.get("email", None),
        "description": data.get("description", None),
    }


def convert_timestamp(value: Any) -> str:
    if value is None:
        return None
    elif type(value) == int:
        ts = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        ts = parser.parse(value).astimezone(tz=timezone.utc)
    return ts.isoformat()


def transform_issue(json_data: Dict) -> Dict:
    data = IssueData(json_data)

    issue = {
        "id": data["id"],
        "key": data["key"],
        "summary": data.get_field("summary", None),
        "description": data.get_field("description", None),
    }

    issue_type = data.get_field("issuetype", {})
    issue["issuetype"] = {
        "name": issue_type.get("name", None),
        "subtask": issue_type.get("subtask", False)
    }

    issue["priority"] = data.get_field("priority", {}).get("name", None)
    issue["status"] = data.get_field("status", {}).get("name", None)
    issue["statusCategory"] = data.get_field("status", {}).get("statusCategory", {}).get("name", None)

    issue["created"] = convert_timestamp(data.get_field("created", None))
    issue["updated"] = convert_timestamp(data.get_field("updated", None))

    issue["resolution"] = data.get_field("resolution", {}).get("name", None)
    issue["resolutiondate"] = convert_timestamp(data.get_field("resolutiondate", None))

    issue["creator"] = transform_user(data.get_field("creator", None))
    issue["reporter"] = transform_user(data.get_field("reporter", None))
    issue["assignee"] = transform_user(data.get_field("assignee", None))

    issue["project"] = transform_project(data.get_field("project", None))

    if "changelog" in data:
        changelog_entries = list()
        histories = data["changelog"]["histories"]
        for history_item in histories:
            changelog_items = list()
            for item in history_item["items"]:
                changelog_item = {
                    "field": item["field"],
                    "from": item.get("from", None),
                    "fromString": item.get("fromString", None),
                    "to": item.get("to", None),
                    "toString": item.get("toString", None),
                }
                changelog_items.append(changelog_item)
            changelog_entry = {
                "created": convert_timestamp(history_item["created"]),
                "author": transform_user(history_item.get("author", None)),
                "items": changelog_items
            }
            changelog_entries.append(changelog_entry)
        issue["changelog"] = changelog_entries

    # print(json.dumps(issue, indent=4))
    #
    # if "changelog" in issue:
    #     changelog = issue["changelog"]
    #     for changelog_entry in changelog:
    #         for item in changelog_entry["items"]:
    #             if item["field"] == "status":
    #                 print(changelog_entry["created"] + " " + item["toString"])

    return issue


def transform_issues(input_file_path: str, output_file_path: str):
    with open(input_file_path, 'r', encoding='UTF-8') as input_file, open(output_file_path, 'w') as output_file:
        line_num = 0
        for line in input_file:
            json_data = json.loads(line)
            result = transform_issue(json_data)
            if line_num != 0:
                output_file.write('\n')
            json.dump(result, output_file)
            line_num = line_num + 1


if __name__ == "__main__":
    projects = [
        "APIEXPPL",
        "BSCP",
        "DSPL",
        "ECP",
        "EOFR",
        "EWSRPR",
        "IFDR",
        "IM",
        "NGCA",
        "NGCP",
        "NGCQE",
        "NGCS",
        "OCB2B",
        "OHUB",
        "POSTSAPCC",
        "S20TC",
        "SEMS2",
        "SQA",
        "SWT",
        "TEC",
        "TIQAPI",
        "TTA",
        "WLSC",
    ]

    for project in projects:
        try:
            print("---------------------------------------")
            print(f"Transforming data for project: {project}")
            input_file_path = f"export-{project}-20210101000000.jsonl"
            name, ext = os.path.splitext(input_file_path)
            output_file_path = name + "-transformed" + ext
            transform_issues(input_file_path, output_file_path)
        except Exception as e:
            print(e)
            pass
