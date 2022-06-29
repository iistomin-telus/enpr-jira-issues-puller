import os
from datetime import datetime, timedelta, timezone
from typing import List

import requests
from dateutil import parser
from requests.auth import HTTPBasicAuth
import json

base_url = os.environ.get("JIRA_BASE_URL", "https://jira.tsl.telus.com/rest/api/2")
auth = HTTPBasicAuth(os.environ.get("JIRA_API_USER"), os.environ.get("JIRA_API_TOKEN"))
headers = {
    "Accept": "application/json"
}


def remove_urls(data):
    if type(data) == dict:
        if "avatarUrls" in data:
            del data["avatarUrls"]
        if "iconUrl" in data:
            del data["iconUrl"]
        if "self" in data:
            del data["self"]
        for key, value in data.items():
            remove_urls(value)
    elif type(data) == list:
        for value in data:
            remove_urls(value)


def preprocess_issue(issue):
    if "expand" in issue:
        del issue["expand"]
    remove_urls(issue)


def get_issue():
    url = f"{base_url}/issue/NGCS-6176?fields=*all,-comment&expand=changelog"
    response = requests.request("GET", url, headers=headers, auth=auth)
    issue = json.loads(response.text)
    preprocess_issue(issue)
    print(json.dumps(issue, sort_keys=True, indent=4, separators=(",", ": ")))


def search_issues(projects: List[str]):
    tracking_file_path = "tracking.json"

    for project_key in projects:
        tracking = None
        if os.path.isfile(tracking_file_path):
            with open(tracking_file_path, 'r') as config_file:
                tracking = json.load(config_file)
        if tracking is None:
            tracking = dict()
            tracking[project_key] = {
                "lastUpdated": "2020-01-01 00:00"
            }

        url = f"{base_url}/search"
        last_updated = tracking.get(project_key, {}).get("lastUpdated")
        base_params = {
            "jql": f"project = '{project_key}' AND updated >= '{last_updated}' ORDER BY updated ASC",
            "maxResults": 100,
            "fields": "*all,-comment",
            "expand": "changelog"
        }

        total = 1
        max_results = 0
        start_at = 0

        while (start_at + max_results) < total:
            print(f"Page: start at {start_at}, max results {max_results}")
            params = {
                "startAt": start_at + max_results
            }
            params.update(base_params)

            issue_list = []

            response = requests.request("GET", url, headers=headers, auth=auth, params=params)
            if response.status_code != 200:
                response.raise_for_status()
            response_json = json.loads(response.text)

            if "issues" in response_json:
                issues = response_json["issues"]
                for issue in issues:
                    preprocess_issue(issue)
                    issue_list.append(issue)

            total = response_json['total']
            start_at = response_json['startAt']
            max_results = response_json['maxResults']

            if len(issue_list) > 0:
                print(f"Publishing issues: {len(issue_list)}")
                for issue in issue_list:
                    print(json.dumps(issue, sort_keys=True, indent=4, separators=(",", ": ")))

                updated = issue_list[len(issue_list) - 1]["fields"]["updated"]
                updated_dt = parser.parse(updated)
                new_last_updated_dt = updated_dt  # + timedelta(minutes=0o1)
                new_last_updated = new_last_updated_dt.strftime('%Y-%m-%d %H:%M')
                print(f"Last updated: {new_last_updated}")
                tracking[project_key]["lastUpdated"] = new_last_updated

                print(f"Updating tracking data")
                with open(tracking_file_path, 'w') as outfile:
                    json.dump(tracking, outfile, indent=4, sort_keys=True)

        new_last_updated = tracking["lastUpdated"]
        print(f"Last updated: {new_last_updated}")


def export_issues(projects):
    tracking_file_path = "tracking.json"

    export_datetime = datetime.now(tz=timezone.utc)
    export_datetime_filename_formatted = export_datetime.strftime('%Y%m%d%H%M%S')
    export_file_path = f"export-{export_datetime_filename_formatted}.jsonl"

    with open(export_file_path, 'w') as export_file:
        for project_key in projects:
            print(f"Extracting data for project: {project_key}")

            tracking = None
            if os.path.isfile(tracking_file_path):
                with open(tracking_file_path, 'r') as tracking_file:
                    tracking = json.load(tracking_file)
            if tracking is None:
                tracking = dict()
            if project_key not in tracking:
                tracking[project_key] = {
                    "lastUpdated": "2021-01-01 00:00"
                }

            url = f"{base_url}/search"
            last_updated = tracking[project_key]["lastUpdated"]
            base_params = {
                "jql": f"project = '{project_key}' AND updated >= '{last_updated}' ORDER BY updated ASC",
                "maxResults": 1000,
                "fields": "*all,-comment",
                "expand": "changelog"
            }

            line_num = 0
            total = 1
            max_results = 0
            start_at = 0

            while (start_at + max_results) < total:
                print(f"Page: start at {start_at}, max results {max_results}")
                params = {
                    "startAt": start_at + max_results
                }
                params.update(base_params)

                issue_list = []

                response = requests.request("GET", url, headers=headers, auth=auth, params=params)
                if response.status_code != 200:
                    response.raise_for_status()
                response_json = json.loads(response.text)

                if "issues" in response_json:
                    issues = response_json["issues"]
                    for issue in issues:
                        preprocess_issue(issue)
                        issue_list.append(issue)

                total = response_json['total']
                start_at = response_json['startAt']
                max_results = response_json['maxResults']

                if len(issue_list) > 0:
                    print(f"Publishing issues: {len(issue_list)}")
                    for issue in issue_list:
                        if line_num != 0:
                            export_file.write('\n')
                        json.dump(issue, export_file)
                        line_num = line_num + 1

                    updated = issue_list[len(issue_list) - 1]["fields"]["updated"]
                    updated_dt = parser.parse(updated)
                    new_last_updated_dt = updated_dt + timedelta(minutes=0o1)
                    new_last_updated = new_last_updated_dt.strftime('%Y-%m-%d %H:%M')
                    print(f"Last updated: {new_last_updated}")
                    tracking[project_key]["lastUpdated"] = new_last_updated

                    print(f"Updating tracking data")
                    with open(tracking_file_path, 'w') as outfile:
                        json.dump(tracking, outfile, indent=4, sort_keys=True)

                    export_file.flush()

            new_last_updated = tracking[project_key]["lastUpdated"]
            print(f"Last updated: {new_last_updated}")
# export_issues


if __name__ == "__main__":
    with open("config.json", 'r') as config_file:
        config = json.load(config_file)
    projects = config["projects"]
    export_issues(projects)
