import argparse
import asyncio
import collections
import csv
import dataclasses
import itertools
import json
import logging
import os
import warnings
from asyncio.proactor_events import _ProactorBasePipeTransport
from functools import wraps
from typing import TYPE_CHECKING, Any, Awaitable, Iterable

import httpx
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from vspy.core.type_hints import ProactorDelType, WarnCallback


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.FileHandler("logging.txt", "a", encoding="utf-8")
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] <%(levelname)s> %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


_PROJECTS_PER_PAGE = 99999
_QUERY_ARGS = {
    "type": "dateissued",
    "sort_by": "2",
    "order": "DESC",
    "rpp": f"{_PROJECTS_PER_PAGE}",
    "etal": "-1",
    "offset": "0",
}
_QUERY_ARGS_STR = "&".join((f"{k}={v}" for k, v in _QUERY_ARGS.items()))
_SKEMMA_HOST = "https://skemman.is"
_MSC_BASE_PATH = f"{_SKEMMA_HOST}/handle/1946/34329/"
_BSC_BASE_PATH = f"{_SKEMMA_HOST}/handle/1946/34325/"
_QUERY = f"browse?{_QUERY_ARGS_STR}"
_MSC_PATH = f"{_MSC_BASE_PATH}{_QUERY}"
_BSC_Path = f"{_BSC_BASE_PATH}{_QUERY}"


def name_split(name: str) -> tuple[str, int]:
    """Extract birth year from name, 0 if none is found."""
    parts = name.split(" ")
    if (
        parts[-1][-1] == "-" or (len(parts[-1]) == 9 and parts[-1][4] == "-")
    ) and parts[-1][:4].isnumeric():
        return " ".join(parts[:-1]), int(parts[-1][:4])
    return " ".join(parts), 0


def date_split(name: str) -> tuple[int, int, int]:
    "Return date as (D,M,Y)."
    day, month, year = name.split(".")
    return int(day), int(month), int(year)


def skemma_full_path(handle: str) -> str:
    """Relative handle to full path."""
    return f"{_SKEMMA_HOST}{handle}"


def silence_event_loop_closed() -> None:
    """Silence the `Event loop is closed` bug."""

    def _do_nothing(_self: _ProactorBasePipeTransport) -> None:
        pass

    def _silence_event_loop_closed(func: "ProactorDelType") -> "ProactorDelType":
        @wraps(func)
        def wrapper(
            self: _ProactorBasePipeTransport,
            _warn: "WarnCallback" = warnings.warn,
        ) -> None:
            try:
                return func(self, _warn)
            except RuntimeError as exc:
                if str(exc) != "Event loop is closed":
                    raise
                return _do_nothing(self)

        return wrapper

    def _wrapped() -> "ProactorDelType":
        return _silence_event_loop_closed(_ProactorBasePipeTransport.__del__)

    _ProactorBasePipeTransport.__del__ = _wrapped()  # type: ignore


def is_windows() -> bool:
    """Check if the operating system who is running this is Windows."""
    return os.name == "nt"


@dataclasses.dataclass
class Project:
    """Data row for total data."""

    title: str
    education_type: str
    authors: list[tuple[str, int]]
    advisors: list[tuple[str, int]]
    accepted: tuple[int, int, int]


def store_project_raw(projects: list[Project]) -> None:
    """Dump raw data to file."""
    with open("raw.json", "w", encoding="utf-8") as file:
        file.write(json.dumps([project_to_dictionary(project) for project in projects]))


def read_raw_data() -> list[Project]:
    """Read dumped json str"""
    with open("raw.json", "r", encoding="utf-8") as file:
        json_str = file.read()
    try:
        lis = json.loads(json_str)
    except (ValueError, TypeError):
        logger.error("Failed to deserialize json str")
    return [dict_to_project(item) for item in lis]


def project_to_dictionary(project: Project) -> dict:  # type: ignore
    """Convert project to json"""
    return {
        "title": project.title,
        "education_type": project.education_type,
        "authors": project.authors,
        "advisors": project.advisors,
        "accepted": project.accepted,
    }


def dict_to_project(dictionary: dict) -> Project:  # type: ignore
    """Convert dict to project"""
    return Project(
        dictionary["title"],
        dictionary["education_type"],
        [tuple(author) for author in dictionary["authors"]],  # type: ignore
        [tuple(author) for author in dictionary["advisors"]],  # type: ignore
        tuple(dictionary["accepted"]),  # type: ignore
    )


@dataclasses.dataclass
class Advisor:
    """Data row for advisors"""

    title: str
    education_type: str
    number_of_students: int
    number_of_advisors: int
    accepted: tuple[int, int, int]


class AsyncClient:
    """Async client to make multiple requests."""

    def __init__(self) -> None:
        self._client = httpx.AsyncClient()

    async def _get(self, url: str) -> httpx.Response:
        res = await self._client.get(url, timeout=60)
        res.raise_for_status()
        return res

    async def get_json(self, url: str) -> dict:  # type: ignore
        """Get request promise to the url, deserialized as a json."""
        json_data = (await self._get(url)).json()
        assert isinstance(json_data, dict)
        return json_data

    async def get(self, url: str) -> str:
        """Get request promise to the url, raw content as string."""
        return (await self._get(url)).text


class ProjectUrlsGetterClient:
    """Async client for getting urls of each project."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def get_project_urls(self, url: str) -> list[str]:
        """Get he urls of all projects in the given url."""
        soup = BeautifulSoup(await self._client.get(url), "html.parser")
        return [
            span["href"]
            for span in soup.select(
                "table.t-data-grid > tbody > tr > td:nth-child(2) > a"
            )
        ]

    def create_tasks(self) -> Iterable[Awaitable[list[str]]]:
        """A task wrapper for `active_python3_version`."""
        return (
            asyncio.create_task(self.get_project_urls(path))
            for path in (_BSC_Path, _MSC_PATH)
        )


class ProjectClient:
    """Async client to scrape individual projects."""

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    async def get_project_data(self, url: str, edu_type: str) -> Project | None:
        """Get he urls of all projects in the given url."""
        try:
            web_raw = await self._client.get(url)
            soup = BeautifulSoup(web_raw, "html.parser")
            return Project(
                self._get_title(soup),
                edu_type,
                self._get_authors(soup),
                self._get_advisors(soup),
                self._get_date(soup),
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error(
                "Failed to get data from %s, error type %s, error msg `%s`",
                url,
                type(exc),
                exc,
                stack_info=True,
            )
            return None

    def create_tasks(
        self, urls: str, edu_type: str
    ) -> Iterable[Awaitable[Project | None]]:
        """A task wrapper for `active_python3_version`."""
        return (
            asyncio.create_task(self.get_project_data(skemma_full_path(url), edu_type))
            for url in urls
        )

    def _get_title(self, soup: BeautifulSoup) -> str:
        titles = soup.find("div", {"class": "dc_title"}).select("ul > li")
        title: str = next(
            (title for title in titles if title.find("img")), titles[0]
        ).text.strip()
        return title

    def _get_authors(self, soup: BeautifulSoup) -> list[tuple[str, int]]:
        div = soup.find("div", {"class": "dc_contributor_author"})
        if not div:
            return []
        return [
            name_split(name.text.strip())
            for name in (
                soup.find("div", {"class": "dc_contributor_author"}).select("ul > li")
            )
        ]

    def _get_advisors(self, soup: BeautifulSoup) -> list[tuple[str, int]]:
        div = soup.find("div", {"class": "dc_description_advisor"})
        if not div:
            return []
        return [name_split(name.text.strip()) for name in (div.select("ul > li"))]

    def _get_date(self, soup: BeautifulSoup) -> tuple[int, int, int]:
        date_div = soup.find("div", {"class": "dc_date_issued"})
        return date_split(date_div.select_one("ul > li").text.strip())


def _project_to_csv_row(
    project: Project, n_authors: int, n_advisors: int
) -> dict[str, Any]:
    data = {
        "Title": project.title,
        "Education type": project.education_type,
        "Year accepted": project.accepted[2],
        "Month accepted": project.accepted[1],
        "Day accepted": project.accepted[0],
    }
    for idx, (name, yob) in enumerate(project.authors):
        data[f"Author{idx + 1}"] = name
        data[f"Author{idx + 1} YOB"] = yob
    for idx in range(len(project.authors), n_authors):
        data[f"Author{idx + 1}"] = ""
        data[f"Author{idx + 1} YOB"] = ""
    for idx, (name, yob) in enumerate(project.advisors):
        data[f"Advisor{idx + 1}"] = name
        data[f"Advisor{idx + 1} YOB"] = yob
    for idx in range(len(project.advisors), n_advisors):
        data[f"Advisor{idx + 1}"] = ""
        data[f"Advisor{idx + 1} YOB"] = ""
    return data


def _headers(max_authors: int, max_advisors: int) -> list[str]:
    headers = [
        "Title",
        "Education type",
        "Year accepted",
        "Month accepted",
        "Day accepted",
    ]
    for idx in range(max_authors):
        headers.append(f"Author{idx + 1}")
        headers.append(f"Author{idx + 1} YOB")
    for idx in range(max_advisors):
        headers.append(f"Advisor{idx + 1}")
        headers.append(f"Advisor{idx + 1} YOB")
    return headers


def _max_authors_and_advisors(projects: list[Project]) -> tuple[int, int]:
    max_authors, max_advisors = 0, 0
    for project in projects:
        max_advisors = max(max_advisors, len(project.advisors))
        max_authors = max(max_authors, len(project.authors))
    return max_authors, max_advisors


def _to_csv(filename: str, projects: list[Project]) -> None:
    max_authors, max_advisors = _max_authors_and_advisors(projects)
    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=_headers(max_authors, max_advisors))
        writer.writeheader()
        writer.writerows(
            [
                _project_to_csv_row(project, max_authors, max_advisors)
                for project in sorted(
                    projects, key=lambda p: tuple(reversed(p.accepted))
                )
            ]
        )


def _advisor_data_init(projects: list[Project]) -> dict[tuple[str, int], list[Advisor]]:
    teacher_to_advisory: dict[tuple[str, int], list[Advisor]] = collections.defaultdict(
        list
    )
    for project in projects:
        for advisor, advisor_yob in project.advisors:
            teacher_to_advisory[(advisor, advisor_yob)].append(
                Advisor(
                    project.title,
                    project.education_type,
                    len(project.authors),
                    len(project.advisors),
                    project.accepted,
                )
            )
    return teacher_to_advisory


def _advisor_data_rows(
    teacher_to_advisory: dict[tuple[str, int], list[Advisor]]
) -> Iterable[dict[str, Any]]:
    for advisor, advisor_yob in sorted(teacher_to_advisory.keys()):
        for project in sorted(
            teacher_to_advisory[(advisor, advisor_yob)],
            key=lambda p: tuple(reversed(p.accepted)),
        ):
            yield {
                "Advisor": advisor,
                "Advisor YOB": advisor_yob,
                "Year accepted": project.accepted[2],
                "Month accepted": project.accepted[1],
                "Day accepted": project.accepted[0],
                "Education type": project.education_type,
                "Number of students": project.number_of_students,
                "Number of advisors": project.number_of_advisors,
                "Title": project.title,
            }


def _to_per_teacher_csv(filename: str, projects: list[Project]) -> None:
    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        headers = [
            "Advisor",
            "Advisor YOB",
            "Year accepted",
            "Month accepted",
            "Day accepted",
            "Education type",
            "Number of students",
            "Number of advisors",
            "Title",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(_advisor_data_rows(_advisor_data_init(projects)))


async def _main() -> None:
    client = AsyncClient()
    projects_url_client = ProjectUrlsGetterClient(client)
    bsc_urls, msc_urls = await asyncio.gather(*projects_url_client.create_tasks())
    project_client = ProjectClient(client)
    logger.info("Fetching data from %s urls", len(bsc_urls) + len(msc_urls))
    projects = await asyncio.gather(
        *itertools.chain(
            project_client.create_tasks(bsc_urls, "BSc"),
            project_client.create_tasks(msc_urls, "MSc"),
        )
    )
    without_none: list[Project] = [
        project for project in projects if project is not None
    ]
    logger.info("Removed %s project that were None", len(projects) - len(without_none))
    store_project_raw(without_none)
    _to_csv("output.csv", without_none)
    _to_per_teacher_csv("teacher.csv", without_none)


def _run_locally() -> bool:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--local",
        dest="local",
        action="store_true",
        default=False,
        help="Create csv from raw data.",
    )
    local: bool = vars(parser.parse_args()).get("local")  # type: ignore
    return local


def local_run() -> None:
    """Recreate csv from json dump."""
    projects = read_raw_data()
    _to_csv("output.csv", projects)
    _to_per_teacher_csv("teacher.csv", projects)


def main() -> None:
    """Starting point."""
    if _run_locally():
        local_run()
        return
    if is_windows():
        silence_event_loop_closed()
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        print("Cancelled")


if __name__ == "__main__":
    main()
