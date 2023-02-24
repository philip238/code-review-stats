import json
import os
from collections import defaultdict
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from typing import cast

from businesstimedelta import LunchTimeRule, Rules, WorkDayRule
from dateutil import parser
from matplotlib import pylab


def get_raw_data(primary_repos):
    data_dir = os.path.join("data", "raw")
    raw_data_files = [f for f in os.listdir(data_dir)]
    raw_data_files = [f for f in raw_data_files if "transformed" not in f]
    raw_data_files = [
        f
        for f in raw_data_files
        if not primary_repos or f.replace(".json", "") in primary_repos
    ]
    raw_data = {
        f.replace(".json", ""): json.load(open(os.path.join(data_dir, f)))
        for f in raw_data_files
    }
    return raw_data


@dataclass
class Review:
    index: int
    reviewer: str
    pull_request: str
    repository: str
    author: str
    request: datetime
    response: datetime
    resolved: datetime
    target_review_time: timedelta
    working_hours: Rules

    @property
    def duration(self):
        return self._calculate_duration_business_hours()

    @property
    def duration_string(self):
        duration = self.duration
        if self.duration:
            duration = duration - timedelta(microseconds=duration.microseconds)
        return str(duration)

    def _calculate_duration_hours(self):
        return self.response - self.request if self.response else None

    def _calculate_duration_business_hours(self):
        return (
            self.working_hours.difference(self.request, self.response).timedelta
            if self.response
            else None
        )

    @property
    def request_string(self):
        return self.request.isoformat() if self.request else "N/A"

    @property
    def response_string(self):
        return self.response.isoformat() if self.response else "N/A"

    @property
    def resolved_string(self):
        return self.resolved.isoformat() if self.resolved else "N/A"

    @property
    def is_actioned(self):
        return bool(self.duration is not None)

    @property
    def is_actioned_within_target(self):
        return bool(
            self.duration < self.target_review_time if self.is_actioned else None
        )

    @property
    def expects_review(self):
        # If a PR is merged or closed within the target review time, it counts as not
        #  expecting review
        time_for_review = self.resolved - self.request
        return time_for_review > self.target_review_time or self.response

    @property
    def expects_review_string(self):
        return "   " if self.expects_review else "not"

    def __str__(self):
        index = format(self.index, f"<{STATS_CONFIG.max_index_len}")
        reviewer = format(self.reviewer, f"<{STATS_CONFIG.max_reviewer_len}")
        pull_request = format(
            self.pull_request, f"<{STATS_CONFIG.max_pull_request_len}"
        )
        repository = format(self.repository, f"<{STATS_CONFIG.max_repository_len}")
        author = format(self.author, f"<{STATS_CONFIG.max_author_len}")
        request = format(self.request_string, f"<{STATS_CONFIG.max_date_len}")
        response = format(self.response_string, f"<{STATS_CONFIG.max_date_len}")
        resolved = format(self.resolved_string, f"<{STATS_CONFIG.max_date_len}")
        duration = format(
            self.duration_string, f"<{STATS_CONFIG.max_review_duration_len}"
        )
        expectation = format(
            str(self.expects_review_string), f"<{STATS_CONFIG.max_expectation_len}"
        )
        return (
            f"Review {index} by {reviewer} on '{pull_request}' in {repository} by"
            f" {author} requested on {request} actioned on {response} took {duration}"
            f" and the pull request was resolved at {resolved} "
            f" and was {expectation} expected"
        )


@dataclass
class Reviewer:
    name: str
    full_name: str = field(init=False)
    reviews: list[Review]

    def __post_init__(self):
        self.full_name = GITHUB_NAMES.get(self.name, self.name)

    @property
    def actioned_reviews(self):
        return [review for review in self.reviews if review.is_actioned]

    @property
    def actioned_within_target_reviews(self):
        return [review for review in self.reviews if review.is_actioned_within_target]

    @property
    def reviews_expect_review(self):
        return [review for review in self.reviews if review.expects_review]

    @property
    def rate(self):
        return (
            Decimal(self.actioned_count) / self.total_count
            if self.total_count
            else Decimal(0)
        )

    @property
    def rate_string(self):
        rate_percentage = self.rate * 100
        rate_percentage_rounded = rate_percentage.quantize(Decimal("0"))
        return str(rate_percentage_rounded)

    @property
    def rate_with_target(self):
        return (
            Decimal(self.actioned_within_target_count) / self.target_to_action_count
            if self.target_to_action_count
            else Decimal(0)
        )

    @property
    def rate_with_target_string(self):
        rate_percentage = self.rate_with_target * 100
        rate_percentage_rounded = rate_percentage.quantize(Decimal("0"))
        return str(rate_percentage_rounded)

    @property
    def actioned_count(self):
        return len(self.actioned_reviews)

    @property
    def actioned_within_target_count(self):
        return len(self.actioned_within_target_reviews)

    @property
    def target_to_action_count(self):
        return len([review for review in self.reviews_expect_review])

    @property
    def total_count(self):
        return len(self.reviews)

    @property
    def duration(self):
        durations = [review.duration for review in self.actioned_reviews]
        total = sum(durations, timedelta())
        return total / self.actioned_count if total else timedelta()

    @property
    def duration_string(self):
        duration = self.duration
        if self.duration:
            duration = duration - timedelta(microseconds=duration.microseconds)
        return str(duration)

    def __str__(self):
        reviewer = format(self.full_name, f"<{STATS_CONFIG.max_reviewer_len}")
        actioned_count = format(self.actioned_count, f">{STATS_CONFIG.max_count_len}")
        total_count = format(self.total_count, f"<{STATS_CONFIG.max_count_len}")
        actioned_within_target_count = format(
            self.actioned_within_target_count,
            f">{STATS_CONFIG.max_count_len}",
        )
        target_to_action_count = format(
            self.target_to_action_count,
            f"<{STATS_CONFIG.max_count_len}",
        )
        rate_with_target = format(
            self.rate_with_target_string, f">{STATS_CONFIG.max_rate_len}"
        )
        rate = format(self.rate_string, f">{STATS_CONFIG.max_rate_len}")
        duration = format(
            self.duration_string, f">{STATS_CONFIG.max_reviewer_duration_len}"
        )
        return (
            f"{reviewer} reviewed {actioned_count}/{total_count} ({rate}%),"
            f" with {actioned_within_target_count}/{target_to_action_count}"
            f" ({rate_with_target}%) hitting target,"
            f" in on average {duration}"
        )


@dataclass
class Reviews:
    reviewers: list[Reviewer]

    def print_stats(self):
        self.finalise_formatting()
        # Stats per-reviewer
        for reviewer in self.reviewers:
            print(reviewer)
        # Stats per-pull-request
        # for reviewer in self.reviewers:
        #     for review in reviewer.reviews:
        #         print(review)

    def finalise_formatting(self):
        reviews = [review for reviewer in self.reviewers for review in reviewer.reviews]
        max_index_len = max([len(str(review.index)) for review in reviews])
        max_pull_request_len = max([len(review.pull_request) for review in reviews])
        max_repository_len = max([len(review.repository) for review in reviews])
        max_author_len = max([len(review.author) for review in reviews])
        max_date_len = max(
            [len(review.request_string) for review in reviews]
            + [len(review.response_string) for review in reviews]
            + [len(review.resolved_string) for review in reviews]
        )
        max_reviewer_duration_len = max(
            [len(reviewer.duration_string) for reviewer in self.reviewers]
        )
        max_review_duration_len = max(
            [len(review.duration_string) for review in reviews]
        )
        max_expectation_len = max(
            [len(str(review.expects_review_string)) for review in reviews]
        )

        max_reviewer_len = max([len(reviewer.full_name) for reviewer in self.reviewers])
        max_count_len = max(
            [len(str(reviewer.total_count)) for reviewer in self.reviewers]
        )
        max_rate_len = max([len(reviewer.rate_string) for reviewer in self.reviewers])

        global STATS_CONFIG
        STATS_CONFIG = StatsConfig(
            max_index_len=max_index_len,
            max_reviewer_len=max_reviewer_len,
            max_pull_request_len=max_pull_request_len,
            max_repository_len=max_repository_len,
            max_author_len=max_author_len,
            max_date_len=max_date_len,
            max_reviewer_duration_len=max_reviewer_duration_len,
            max_review_duration_len=max_review_duration_len,
            max_count_len=max_count_len,
            max_rate_len=max_rate_len,
            max_expectation_len=max_expectation_len,
        )


@dataclass(frozen=True)
class ReviewConfig:
    duration: timedelta
    end: datetime
    target_review_time: timedelta

    @property
    def start(self):
        return self.end - self.duration


@dataclass(frozen=True)
class StatsConfig:
    max_index_len: int
    max_reviewer_len: int
    max_pull_request_len: int
    max_repository_len: int
    max_author_len: int
    max_date_len: int
    max_review_duration_len: int
    max_reviewer_duration_len: int
    max_count_len: int
    max_rate_len: int
    max_expectation_len: int


STATS_CONFIG: StatsConfig = cast(StatsConfig, None)


# noinspection PyMethodMayBeStatic
class ReviewFactory:
    def __init__(self, review_config):
        self.review_config = review_config

    def create(self, raw_data):
        return self._get_reviewers(raw_data)

    def _get_reviewers(self, repositories):
        reviews = self._get_reviews(repositories)
        reviews = [
            review for review in reviews if review.request > self.review_config.start
        ]
        reviewer_names = {review.reviewer for review in reviews}
        reviewers = [
            Reviewer(
                name=reviewer,
                reviews=[review for review in reviews if review.reviewer == reviewer],
            )
            for reviewer in reviewer_names
        ]
        # Sort in decending order by reviews meeting the target
        reviewers = sorted(
            reviewers, key=lambda reviewer: reviewer.rate_with_target * -1
        )
        # Exclude unexpected users
        reviewers = [
            reviewer
            for reviewer in reviewers
            if not GITHUB_NAMES or reviewer.full_name in GITHUB_NAMES.values()
        ]
        return Reviews(reviewers)

    def _get_reviews(self, repositories):
        reviews = []
        for repository_name, repository in repositories.items():
            reviews.extend(
                self._get_reviews_for_repository(repository, repository_name)
            )
        return reviews

    def _get_reviews_for_repository(self, repository, repository_name):
        reviews = []
        for pr in repository:
            reviews.extend(self._get_reviews_for_pr(pr, repository_name))
        return reviews

    def _get_reviews_for_pr(self, pr, repository_name):
        reviews = []
        title = pr["title"]
        author = pr["author"]["login"]
        (
            pr_review_requests,
            pr_reviews,
            pr_resolutions,
        ) = self._get_pr_review_requests_and_reviews(
            pr,
        )

        reviewers = set(pr_review_requests).union(pr_reviews)
        for reviewer in reviewers:
            reviews.extend(
                self._get_reviews_for_reviewer_for_pr(
                    reviewer=reviewer,
                    pr_review_requests=pr_review_requests,
                    pr_resolutions=pr_resolutions,
                    pr_reviews=pr_reviews,
                    title=title,
                    repository_name=repository_name,
                    author=author,
                ),
            )
        return reviews

    def _get_reviews_for_reviewer_for_pr(
        self,
        reviewer,
        pr_review_requests,
        pr_reviews,
        pr_resolutions,
        title,
        repository_name,
        author,
    ):
        reviews = []
        reviewer_requests = sorted(pr_review_requests.get(reviewer, set()))
        reviewer_reviews = sorted(pr_reviews.get(reviewer, set()))
        for i, request in enumerate(reviewer_requests):
            response = next(
                iter([response for response in reviewer_reviews if response > request]),
                None,
            )
            resolution = next(
                iter([resolved for resolved in pr_resolutions if resolved > request]),
                self.review_config.end,
            )
            reviews.append(
                Review(
                    index=i + 1,
                    reviewer=reviewer,
                    pull_request=title,
                    repository=repository_name,
                    author=author,
                    request=request,
                    response=response,
                    resolved=resolution,
                    target_review_time=self.review_config.target_review_time,
                    working_hours=WORKING_HOURS[reviewer],
                ),
            )
        return reviews

    def _get_pr_review_requests_and_reviews(self, pr):
        pr_review_requests = self._get_pr_review_requests(pr)
        pr_reviews = self._get_pr_reviews(pr)
        pr_resolutions = self._get_pr_resolutions(pr)
        return pr_review_requests, pr_reviews, pr_resolutions

    def _get_pr_review_requests(self, pr):
        events = pr["timelineItems"]["nodes"]
        pr_review_requests = {}
        for event in events:
            if event["__typename"] == "ReviewRequestedEvent":
                if (
                    not event["requestedReviewer"]
                    or "login" not in event["requestedReviewer"]
                ):
                    continue
                created = parser.parse(event["createdAt"])
                times = pr_review_requests.setdefault(
                    event["requestedReviewer"]["login"], set()
                )
                times.add(created)
        return pr_review_requests

    def _get_pr_reviews(self, pr):
        events = pr["timelineItems"]["nodes"]
        pr_reviews = {}
        for event in events:
            if event["__typename"] == "PullRequestReview":
                if "login" not in event["author"]:
                    continue
                submitted = parser.parse(event["submittedAt"])
                times = pr_reviews.setdefault(event["author"]["login"], set())
                times.add(submitted)
        return pr_reviews

    def _get_pr_resolutions(self, pr):
        events = pr["timelineItems"]["nodes"]
        pr_resolutions = set()
        for event in events:
            if event["__typename"] in {"MergedEvent", "ClosedEvent"}:
                submitted = parser.parse(event["createdAt"])
                pr_resolutions.add(submitted)
        return sorted(pr_resolutions)


# noinspection PyMethodMayBeStatic
class ReviewGrapher:
    def graph(self, reviews):
        self._graph_reviews_by_reviewer(reviews)
        self._graph_rate_by_reviewer(reviews)
        self._graph_time_by_reviewer(reviews)

    def _graph_reviews_by_reviewer(self, reviews):
        """
        Review count by reviewer.
        """
        reviewers = copy(reviews.reviewers)
        reviewers = list(
            reversed(
                sorted(
                    reviewers,
                    key=lambda reviewer: reviewer.actioned_within_target_count,
                )
            )
        )
        labels = [reviewer.full_name for reviewer in reviewers]
        success = [reviewer.actioned_within_target_count for reviewer in reviewers]
        slow = [reviewer.actioned_count for reviewer in reviewers]
        fail = [reviewer.target_to_action_count for reviewer in reviewers]
        slow_difference = [item[1] - item[0] for item in zip(success, slow)]
        fail_difference = [item[2] - item[1] for item in zip(success, slow, fail)]
        pylab.clf()
        pylab.figure(figsize=(10, 15))
        pylab.bar(
            labels,
            success,
            label="Reviewed within target",
            color="green",
        )
        pylab.bar(
            labels,
            slow_difference,
            bottom=success,
            label="Reviewed slower than target",
            color="orange",
        )
        pylab.bar(
            labels,
            fail_difference,
            bottom=slow,
            label="Not reviewed",
            color="red",
        )
        pylab.xticks(rotation=90)
        pylab.yticks(ticks=[0, 20, 40, 60, 80, 100, 120])
        pylab.legend()
        pylab.ylabel("Number of code reviews")
        pylab.title("Code reviews actioned")
        pylab.grid(True)
        pylab.savefig("output/reviews_by_reviewer.png")

    def _graph_rate_by_reviewer(self, reviews):
        """
        Review success rate by reviewer.
        """
        data = [
            (reviewer.full_name, reviewer.rate_with_target.quantize(Decimal("0.00")))
            for reviewer in reviews.reviewers
        ]
        data = list(reversed(sorted(data, key=lambda item: item[1])))
        pylab.clf()
        pylab.figure(figsize=(10, 10))
        pylab.bar(*list(zip(*data)))
        pylab.ylim([0, 1])
        pylab.xticks(rotation=90)
        pylab.yticks(
            ticks=[0, 0.2, 0.4, 0.6, 0.8, 1],
            labels=["0", "20%", "40%", "60%", "80%", "100%"],
        )
        pylab.ylabel("Pull requests reviewed within half a business day / %")
        pylab.title("Reviews responded to by reviewer (target is 100%)")
        pylab.grid(True)
        pylab.savefig("output/rate_by_reviewer.png")

    def _graph_time_by_reviewer(self, reviews):
        """
        Review time by reviewer.
        """
        data = [
            (
                reviewer.full_name,
                (Decimal(reviewer.duration.seconds) / 60 / 60).quantize(
                    Decimal("0.00")
                ),
            )
            for reviewer in reviews.reviewers
        ]
        data = list(sorted(data, key=lambda item: item[1]))
        pylab.clf()
        pylab.figure(figsize=(10, 10))
        pylab.bar(*list(zip(*data)))
        pylab.xticks(rotation=90)
        pylab.ylabel("Average time to review a pull request / working hours")
        pylab.title("Review time by reviewer (target is 3.5 hours)")
        pylab.grid(True)
        pylab.savefig("output/time_by_reviewer.png")


if __name__ == "__main__":
    # The data is averaged over the last four weeks.
    # Time is measured in working hours. Nights and weekends are excluded.
    # Working hours for part-timers only include days they work.
    # Pull requests are only expected to be reviewed if they stayed open for more than
    #  half a working day.
    # Only reviewers in the Backend CTP that have reviewed any pull requests are included.
    # A review is either an approval, comment, or request for changes.
    # Reviews that aren't actioned do not affect the time-to-review metric.
    PRIMARY_REPOS = [
        "MPBX",
        "Python-Core-SDK",
        "TransactionService",
        "SearchService",
        "FixtureService",
        "MediaService",
        "pdf-rendering-service",
        "TranslationService",
        "IdentityProvider",
    ]
    INCLUDE_ALL_REPOS = True
    GITHUB_NAMES = {
        "lucasmoreirampb": "Lucas",
        "sinistamunkey": "Gary",
        "P4rk": "Luke",
        "irena7777": "Irena",
        "philip238": "Phil W",
        "humberto-politi-mpb": "Humberto",
        "Ellimelon": "Elliot",
        "Jaime-Birdbrook": "Jaime",
        "chazmead": "Chaz",
        "harry-adams": "Harry",
    }
    INCLUDE_ALL_USERS = False
    GITHUB_NAMES = {} if INCLUDE_ALL_USERS else GITHUB_NAMES
    DEFAULT_WORKING_HOURS_RULES = Rules(
        [
            WorkDayRule(
                start_time=time(hour=9),
                end_time=time(hour=17, minute=30),
                working_days=[0, 1, 2, 3, 4],
            ),
            LunchTimeRule(
                start_time=time(hour=12, minute=30),
                end_time=time(hour=13, minute=30),
                working_days=[0, 1, 2, 3, 4],
            ),
        ],
    )
    WORKING_HOURS = defaultdict(
        lambda: DEFAULT_WORKING_HOURS_RULES,
        P4rk=Rules(
            [
                WorkDayRule(
                    start_time=time(hour=9),
                    end_time=time(hour=17, minute=30),
                    working_days=[1, 2, 3, 4],
                ),
                LunchTimeRule(
                    start_time=time(hour=12, minute=30),
                    end_time=time(hour=13, minute=30),
                    working_days=[1, 2, 3, 4],
                ),
            ],
        ),
        irena7777=Rules(
            [
                WorkDayRule(
                    start_time=time(hour=9),
                    end_time=time(hour=17, minute=30),
                    working_days=[2, 3, 4],
                ),
                LunchTimeRule(
                    start_time=time(hour=12, minute=30),
                    end_time=time(hour=13, minute=30),
                    working_days=[2, 3, 4],
                ),
            ],
        ),
    )
    RAW_DATA = get_raw_data(None if INCLUDE_ALL_REPOS else PRIMARY_REPOS)
    REVIEW_CONFIG: ReviewConfig = ReviewConfig(
        duration=timedelta(weeks=4),
        end=datetime.now().replace(tzinfo=timezone.utc),
        target_review_time=timedelta(hours=3, minutes=30),
    )
    REVIEW_FACTORY = ReviewFactory(REVIEW_CONFIG)
    REVIEWS = REVIEW_FACTORY.create(RAW_DATA)
    REVIEWS.print_stats()
    ReviewGrapher().graph(REVIEWS)
