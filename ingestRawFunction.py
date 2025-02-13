import os
from datetime import datetime

import boto3
import pandas as pd
import requests

headers = {"Authorization": "Bearer "}
api_url = "https://api.github.com"
raw_url = "https://raw.githubusercontent.com"

repo = "statsbomb/open-data"
branch = "master"

github = requests.Session()
github.headers = headers

s3_client = boto3.client("s3")

response = s3_client.get_object(Bucket="statsbomb-raw-data", Key="process_date.txt")
last_update = response["Body"].read().decode("utf-8")
last_update = datetime.strptime(last_update, "%Y-%m-%dT%H:%M:%S")


def lambda_handler(event, context):
    """Lambda function"""

    def get_commits(api_url, repo, branch, last_update):
        """Get a list containg the SHA and date of all commits to the GitHub repo branch"""

        page_num = 1
        all_commits = []
        next_page = True

        while next_page:
            try:
                # Returns commits page
                url = f"{api_url}/repos/{repo}/commits?sha={branch}&per_page=100&page={page_num}"
                response = github.get(url)
                response.raise_for_status()
                commits = response.json()

                # Stores commit details from page to dictionary in list
                for commit in commits:
                    date = datetime.strptime(commit["commit"]["author"]["date"], "%Y-%m-%dT%H:%M:%SZ")
                    commit_info = {"sha": commit["sha"], "date": date}
                    all_commits.append(commit_info)

                # If there are no more pages, stop the while loop
                if "next" not in response.links:
                    next_page = False

                page_num += 1

            except requests.exceptions.RequestException as e:
                print(f"API request failed with status code {response.status_code}: {response.text}")

        # Sort list of dicts by dict value
        if all_commits:
            all_commits.sort(key=lambda x: x["date"])
            all_commits = [commit for commit in all_commits if (commit["date"] > last_update)]

        return all_commits

    def get_files(api_url, repo, sha):
        """Returns a list of all the data/events files contained in a commit"""

        files = []

        try:
            # Returns info associated with commit/branch
            url = f"{api_url}/repos/{repo}/commits/{sha}"
            response = github.get(url)
            response.raise_for_status()
            content = response.json()

            # Iterates through files found in commit, appends to list if file has been added or modified
            for file in content["files"]:
                if "data/events" in file["filename"]:
                    if file["status"] in ["added", "modified"]:
                        files.append(
                            file["filename"])

            print(f"{len(files)} file(s) found")
            return files

        except requests.exceptions.RequestException:
            print(f"get_files: API request failed with status code {response.status_code}: {response.text}")
            return files

    def get_competitions(raw_url, repo, sha):
        """Downloads competitions.json from GitHub"""

        # If so, accesses from GitHub
        try:
            url = f"{raw_url}/{repo}/{sha}/data/competitions.json"
            response = github.get(url)
            response.raise_for_status()
            data = response.json()
            data = pd.json_normalize(data)
            return data[["competition_id", "season_id"]]

        except requests.exceptions.RequestException:
            print(f"get_competitions: API request failed with status code {response.status_code}: {response.text}")
            return pd.DataFrame()

    def get_matches(competitions, raw_url, repo, sha):
        """Downloads relevant match files from GitHub if added/modified, or from S3 otherwise"""

        # Generates match file paths from competition_id and season_id
        competitions = competitions.astype(str)
        competitions["path"] = ("data/matches/" + competitions["competition_id"] +
                                "/" + competitions["season_id"] + ".json")
        match_paths = competitions["path"].to_list()

        all_matches = []

        # Iterates through match file paths and gets match_id
        for path in match_paths:
            try:
                url = f"{raw_url}/{repo}/{sha}/{path}"
                response = github.get(url)
                response.raise_for_status()
                data = response.json()

                # Checks if the path contains any data
                if data:
                    matches = pd.json_normalize(data)
                    matches = matches[["competition.competition_id", "season.season_id", "match_id"]]
                    matches.columns = ["competition_id", "season_id", "match_id"]
                    all_matches.append(matches)
                else:
                    print(f"No match file found at {path}")

            except requests.exceptions.RequestException:
                print(f"get_matches: API request failed with status code {response.status_code}: {response.text}")

        # Concatenates to single DataFrame
        if all_matches:
            return pd.concat(all_matches, ignore_index=True)

        return pd.DataFrame()

    def get_paths(events):
        """Convert competition_id, season_id and match_id to GitHub and S3 file names, and store as list of tuples"""

        events = events.astype(str)
        events["github_path"] = "data/events/" + events["match_id"] + ".json"
        events["s3_path"] = (events["competition_id"] + "/" + events["season_id"] + "/" + events["match_id"] + ".json")
        return list(events[["github_path", "s3_path"]].itertuples(index=False, name=None))

    def upload_to_s3(raw_url, repo, sha, paths, s3_client):
        """Extract file from GitHub and store to S3"""

        github_path = paths[0]
        s3_path = paths[1]

        # Get relevant file from GitHub repo
        try:
            url = f"{raw_url}/{repo}/{sha}/{github_path}"
            response = github.get(url)
            response.raise_for_status()
            data = response.content

            # If file contains data, put to S3
            if data:
                try:
                    s3_client.put_object(Bucket="statsbomb-raw-data", Key=s3_path, Body=data)
                    print(f"Successfully uploaded to S3: {github_path} -> {s3_path}")
                except Exception as e:
                    print(f"upload_to_s3: Failed to upload to S3: {str(e)}")

        except requests.exceptions.RequestException:
            print(f"upload_to_s3: API request failed with status code {response.status_code}: {response.text}")

    # Get a list of commits
    all_commits = get_commits(api_url, repo, branch, last_update)

    for commit in all_commits:
        sha = commit["sha"]
        date = commit["date"].strftime("%Y-%m-%dT%H:%M:%S")
        print(f"Processing {sha}, {date}")

        # Search for any new events files
        files_found = False
        files = get_files(api_url, repo, sha)

        # If found, get competitions.json and compare against matches
        if files:
            files_found = True
            competitions = get_competitions(raw_url, repo, sha)
            matches = get_matches(competitions, raw_url, repo, sha)

            # Return competition, season, match IDs for new event files found. Get relevant GitHub and S3 paths.
            event_ids = [int(os.path.splitext(os.path.basename(file))[0]) for file in files]
            events = matches[matches["match_id"].isin(event_ids)].astype(str)
            paths = get_paths(events)

            # Push from GitHub to S3
            files_processed = []

            for item in paths:
                upload_to_s3(raw_url, repo, sha, item, s3_client)
                files_processed.append(item[1])

            s3_client.put_object(Bucket="statsbomb-raw-data", Key="update_log.txt", Body=", ".join(files_processed))

        # Write date of last SHA and files processed to S3 files
        s3_client.put_object(Bucket="statsbomb-raw-data", Key="process_date.txt", Body=date)

        if files_found:
            break
