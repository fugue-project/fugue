#!/usr/bin/env python3
"""Setup script that validates GitHub release tag matches package version."""

import os
import sys

from fugue import __version__


def validate_release_tag():
    """Validate that GITHUB_REF matches the package version."""
    github_ref = os.environ.get("GITHUB_REF", "")

    # Skip validation if not in a GitHub release context
    if not github_ref.startswith("refs/tags/"):
        raise Exception(
            "⚠️  Not running in a GitHub release context - skipping tag validation"
        )

    tag = github_ref.replace("refs/tags/", "")

    # Remove 'v' prefix if present
    tag_version = tag.lstrip("v")
    package_version = __version__

    print(f"GitHub release tag: {tag}")
    print(f"Package version: {package_version}")

    if tag_version != package_version:
        print(
            f"❌ ERROR: Tag version '{tag_version}' does not match package version '{package_version}'"
        )
        sys.exit(1)

    print(f"✅ Version validation passed: {tag_version}")


if __name__ == "__main__":
    validate_release_tag()
