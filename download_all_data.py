import argparse
import os
import subprocess
import sys

import requests

parser = argparse.ArgumentParser()
parser.add_argument(
    "--days-old",
    "-d",
    type=int,
    default=14,
    help="How many days old should the PRs be to be included in the downloaded set?",
)
parser.add_argument(
    "--org",
    type=str,
    default="mpb-com",
    help="Which org / user to download PR data from.",
)
parser.add_argument(
    "--user",
    type=str,
    default=os.getenv("USER"),
    help="Your github username that matches the $GH_API_TOKEN environment variable.",
)
args = parser.parse_args()

user = args.user
token = os.getenv("GH_API_TOKEN")

# TODO: Handle pagination but most people / orgs don't have more than 200 repos.
res = requests.get(
    f"https://api.github.com/orgs/{args.org}/repos?per_page=200",
    auth=(user, token),
)
res.raise_for_status()
data = res.json()

DATA_DIR = os.path.join("data", "raw")
REPOSITORIES = [repo["name"] for repo in data]
PRIMARY_REPOS = [
    'Flamingo',
    'Toucan',
    'TransactionService',
    'SearchService',
    'MPBX',
    'MediaService',
    'pdf-rendering-service',
    'TranslationService',
]
REPOSITORIES += PRIMARY_REPOS
print(f"Repositories: {', '.join(REPOSITORIES)}")

if not os.path.isdir(DATA_DIR):
    os.makedirs(DATA_DIR)

for repository in REPOSITORIES:
    output_file = os.path.join(DATA_DIR, f"{repository}.json")
    print("Loading PR data for", repository, "to", output_file)
    subprocess.run(
        [
            sys.executable,
            "./download_data.py",
            "-o",
            output_file,
            "--days-old",
            str(args.days_old),
            "mpb-com",
            repository,
        ]
    )
