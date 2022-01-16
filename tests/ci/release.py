#!/usr/bin/env python

# -
# TODO:
# - use particular commit for release tag
# - use context and roll-back changes on failure
# - improve logging

import argparse
import logging
import re
import subprocess
from os.path import dirname, realpath, join
from typing import Dict, Optional, Union

VERSIONS = Dict[str, Union[int, str]]

VERSIONS_TEMPLATE = """# This variables autochanged by release_lib.sh:

# NOTE: has nothing common with DBMS_TCP_PROTOCOL_VERSION,
# only DBMS_TCP_PROTOCOL_VERSION should be incremented on protocol changes.
SET(VERSION_REVISION {VERSION_REVISION})
SET(VERSION_MAJOR {VERSION_MAJOR})
SET(VERSION_MINOR {VERSION_MINOR})
SET(VERSION_PATCH {VERSION_PATCH})
SET(VERSION_GITHASH {VERSION_GITHASH})
SET(VERSION_DESCRIBE {VERSION_DESCRIBE})
SET(VERSION_STRING {VERSION_STRING})
# end of autochange
"""


class Git:
    """A small wrapper around subprocess to invoke git commands"""

    def __init__(self):
        cwd = dirname(realpath(__file__))
        rel_root = self.run("git rev-parse --show-cdup", cwd=cwd)
        self.root = realpath(join(cwd, rel_root))
        self.branch = self.run("git branch --show-current")
        self.sha = self.run("git rev-parse HEAD")
        self.sha_short = self.sha[:10]
        # Format: {latest_tag}-{commits_since_tag}-g{sha_short}
        self.latest_tag = self.run("git describe --tags --abbrev=0")
        pattern = re.compile(r"^v\d+[.]\d+[.]\d+[.]\d+-\w+$")
        if not pattern.match(self.latest_tag):
            raise Exception(f"last tag {self.latest_tag} doesn't match the pattern")
        self.commits_since_tag = int(
            self.run(f"git rev-list {self.latest_tag}..HEAD --count")
        )
        self.create_new_branch = True
        self.new_branch = ""
        self.new_tag = ""
        logging.info(
            "Current repo info: root dir - %s, branch - %s, commit sha - %s",
            self.root,
            self.branch,
            self.sha,
        )

    def run(self, cmd: str, cwd: Optional[str] = None) -> str:
        if cwd is None:
            cwd = self.root
        return subprocess.check_output(
            cmd, shell=True, cwd=cwd, encoding="utf-8"
        ).strip()

    def check_branch(self, release_type: str, versions: VERSIONS):
        if release_type in ("major", "minor"):
            if self.branch != "master":
                raise Exception(f"branch must be 'master' for {release_type} release")
        # TODO: process later
        if release_type == "patch":
            branch = f"{versions['VERSION_MAJOR']}.{versions['VERSION_MINOR']}"
            if self.branch != branch:
                raise Exception(f"branch must be '{branch}' for {release_type} release")

    def update_versions(self, release_type: str, versions: VERSIONS) -> VERSIONS:
        # The only change to an old versions file is updating hash to the
        # current commit
        original_versions = versions
        original_versions["VERSION_GITHASH"] = self.sha
        original_versions["VERSION_STRING"] = (
            f"{original_versions['VERSION_MAJOR']}."
            f"{original_versions['VERSION_MINOR']}."
            f"{original_versions['VERSION_PATCH']}."
            f"{self.commits_since_tag}"
        )
        original_versions["VERSION_DESCRIBE"] = (
            f"v{original_versions['VERSION_MAJOR']}."
            f"{original_versions['VERSION_MINOR']}."
            f"{original_versions['VERSION_PATCH']}."
            f"{self.commits_since_tag}-prestable"
        )

        versions = original_versions.copy()
        self.new_branch = f"{versions['VERSION_MAJOR']}.{versions['VERSION_MINOR']}"

        tag_version, tag_type = self.latest_tag.split("-", maxsplit=1)
        tag_parts = tag_version[1:].split(".")
        if (
            tag_type in ("prestable", "testing")
            and tag_parts[0] == versions["VERSION_MAJOR"]
            and tag_parts[1] == versions["VERSION_MINOR"]
        ):
            # changes are incremental for these releases
            versions["changes"] = (
                int(tag_version.split(".")[-1]) + self.commits_since_tag
            )
        else:
            versions["changes"] = self.commits_since_tag

        self.new_tag = (
            "v{VERSION_MAJOR}.{VERSION_MINOR}.{VERSION_PATCH}.{changes}"
            "-prestable".format_map(versions)
        )

        if release_type == "patch":
            self.create_new_branch = False
            versions["VERSION_PATCH"] = int(versions["VERSION_PATCH"]) + 1
        elif release_type == "minor":
            versions["VERSION_MINOR"] = int(versions["VERSION_MINOR"]) + 1
            versions["VERSION_PATCH"] = 1
        elif release_type == "major":
            versions["VERSION_MAJOR"] = int(versions["VERSION_MAJOR"]) + 1
            versions["VERSION_MINOR"] = 1
            versions["VERSION_PATCH"] = 1
        else:
            raise ValueError(f"release type {release_type} is not known")

        # Should it be updated for any release?..
        versions["VERSION_STRING"] = (
            f"{versions['VERSION_MAJOR']}."
            f"{versions['VERSION_MINOR']}."
            f"{versions['VERSION_PATCH']}.1"
        )
        versions["VERSION_REVISION"] = int(versions["VERSION_REVISION"]) + 1
        versions["VERSION_GITHASH"] = self.sha
        versions["VERSION_DESCRIBE"] = f"v{versions['VERSION_STRING']}-prestable"
        return versions


def read_versions(filename: str) -> VERSIONS:
    versions = {}
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("SET("):
                continue

            value = 0  # type: Union[int, str]
            name, value = line[4:-1].split(maxsplit=1)
            try:
                value = int(value)
            except ValueError:
                pass
            versions[name] = value

    return versions


def write_versions(filename: str, versions: VERSIONS):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(VERSIONS_TEMPLATE.format_map(versions))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to release a new ClickHouse version, requires `git` and "
        "`gh` (github-cli) commands",
    )

    parser.add_argument(
        "--type",
        default="minor",
        # choices=("major", "minor", "patch"), # add support later
        choices=("major", "minor"),
        dest="release_type",
        help="a release type, new branch is created only for 'major' and 'minor'",
    )
    parser.add_argument(
        "--versions-file",
        type=str,
        default="cmake/autogenerated_versions.txt",
        help="a path to versions cmake file, relative to the repository root",
    )
    parser.add_argument(
        "--no-check-dirty",
        action="store_true",
        help="skip check repository for uncommited changes",
    )
    parser.add_argument(
        "--no-check-branch",
        action="store_true",
        help="by default, 'major' and 'minor' types work only for master, and 'patch' "
        "works only for a release branches, that name should be the same as "
        "'$MAJOR.$MINOR' version, e.g. 22.2",
    )
    parser.add_argument(
        "--no-publish-release",
        action="store_true",
        help="by default, 'major' and 'minor' types work only for master, and 'patch' ",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = parse_args()
    git = Git()
    if not args.no_check_dirty:
        logging.info("Checking if repo is clean")
        git.run("git diff HEAD --exit-code")

    versions_file = join(git.root, args.versions_file)
    versions = read_versions(versions_file)
    if not args.no_check_branch:
        git.check_branch(args.release_type, versions)

    new_versions = git.update_versions(args.release_type, versions)

    if not args.no_publish_release:
        # Publish release on github for the current HEAD (master, if checked)
        git.run(f"gh release create --draft {git.new_tag} --target {git.sha}")

    # Commit updated versions to HEAD and push to remote
    write_versions(versions_file, new_versions)
    git.run(f"git checkout -b {git.new_branch}-helper")
    git.run(
        f"git commit -m 'Auto version update to [{new_versions['VERSION_STRING']}] "
        f"[{new_versions['VERSION_REVISION']}]' {versions_file}"
    )
    git.run(f"git push -u origin {git.new_branch}-helper")
    git.run(
        f"gh pr create --title 'Update version after release {git.new_branch}' "
        f"--body-file '{git.root}/.github/PULL_REQUEST_TEMPLATE.md'"
    )

    # Create a new branch from the previous commit and push there with creating
    # a PR
    git.run(f"git checkout -b {git.new_branch} HEAD~")
    write_versions(versions_file, versions)
    git.run(
        f"git commit -m 'Auto version update to [{versions['VERSION_STRING']}] "
        f"[{versions['VERSION_REVISION']}]' {versions_file}"
    )
    git.run(f"git push -u origin {git.new_branch}")
    git.run(
        "gh pr create --title 'Release pull request for branch "
        f"{versions['VERSION_MAJOR']}.{versions['VERSION_MINOR']}' --body "
        "'This PullRequest is part of ClickHouse release cycle. It is used by CI "
        "system only. Do not perform any changes with it.' --label release"
    )


if __name__ == "__main__":
    main()
