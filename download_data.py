import argparse
import json
import os
import sys
from datetime import timedelta

import arrow
import requests

parser = argparse.ArgumentParser(
    description="Downloads PR review data from GitHub for a given repo"
)
parser.add_argument(
    "repo_owner",
    help="the owner of a GitHub repo. For 'https://github.com/Microsoft/TypeScript' would be 'Microsoft'",
)
parser.add_argument(
    "repo_name",
    help="the name of a GitHub repo of the above owner. For 'https://github.com/Microsoft/TypeScript' would be 'TypeScript'",
)
parser.add_argument(
    "--prs-per-batch",
    type=int,
    help="the number of PRs to download per request",
    default=100,
)
parser.add_argument(
    "-o", "--output-file", help="file to output; if omitted uses stdout"
)
parser.add_argument(
    "-tz",
    default="Europe/London",
    help="timezone to use for calculating business hours for review status",
)
parser.add_argument(
    "--days-old",
    "-d",
    type=int,
    default=14,
    help="How many days old should the PRs be to be included in the downloaded set?",
)
args = parser.parse_args()

API_TOKEN_KEY = "GH_API_TOKEN"
if not API_TOKEN_KEY in os.environ:
    print(
        f"There must be a '{API_TOKEN_KEY}' environment variable defined",
        file=sys.stderr,
    )
    exit(1)

token = os.environ[API_TOKEN_KEY]
ENDPOINT = "https://api.github.com/graphql"
HEADERS = {
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github.starfire-preview+json",
}

query = """
query($repoOwner: String!, $repoName: String!, $prBefore: String, $prCount: Int = 100){
  repository(owner: $repoOwner, name: $repoName) {
    pullRequests(last: $prCount, before: $prBefore, orderBy: {field:CREATED_AT, direction:ASC}) {
      pageInfo {
        startCursor
        hasPreviousPage
      }
      nodes {
        title
        createdAt
        baseRepository { name }
        author { login }
        timelineItems(first: 200, itemTypes:[REVIEW_REQUESTED_EVENT, REVIEW_REQUEST_REMOVED_EVENT, PULL_REQUEST_REVIEW, CLOSED_EVENT, MERGED_EVENT]) {
          nodes {
            ... on ReviewRequestedEvent {
              __typename
              createdAt
              requestedReviewer {
                ...ReviewerInfo
              }
            }
            ... on ReviewRequestRemovedEvent {
              __typename
              createdAt
              requestedReviewer {
                ...ReviewerInfo
              }
            }
            ... on PullRequestReview {
              __typename
              state
              submittedAt
              author {
                login
              }
            }
            ... on ClosedEvent {
              __typename
              createdAt
            }
            ... on MergedEvent {
              __typename
              createdAt
            }
          }
        }
      }
    }
  }
}

fragment ReviewerInfo on RequestedReviewer {
  ... on User {
    login
  }
  ... on Team {
    name
  }
}
"""

start_cursor = None
has_previous_page = True
all_nodes = []
too_old = arrow.utcnow().to(args.tz).datetime - timedelta(days=args.days_old)

while has_previous_page:
    variables = dict(
        repoOwner=args.repo_owner,
        repoName=args.repo_name,
        prBefore=start_cursor,
        prCount=args.prs_per_batch,
    )
    data = json.dumps({"query": query, "variables": variables})

    response = requests.post(ENDPOINT, headers=HEADERS, data=data)
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        print(result["errors"], file=sys.stderr)
        exit(1)

    pull_requests = result["data"]["repository"]["pullRequests"]
    start_cursor = pull_requests["pageInfo"]["startCursor"]
    has_previous_page = pull_requests["pageInfo"]["hasPreviousPage"]
    nodes = pull_requests["nodes"]
    if not nodes:
        continue

    all_nodes.extend(nodes)
    pr = nodes[-1]
    if arrow.get(pr["createdAt"]).to(args.tz).datetime < too_old:
        has_previous_page = False

    print(f"Loaded {len(all_nodes)} pull requests", file=sys.stderr)
else:
    print("Loaded all pull requests successfully", file=sys.stderr)

output_file = open(args.output_file, "w") if args.output_file else sys.stdout
output_file.write(json.dumps(all_nodes, indent=2) + "\n")
