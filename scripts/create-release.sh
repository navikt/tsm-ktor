#!/bin/bash
set -eo pipefail

# Creates the git tag and GitHub release for the version already written to
# libs/version by next-version.sh. It never changes the version, so the
# artifacts built and tested earlier in the pipeline are exactly what gets
# released.

if [ ! -s libs/version ]; then
  echo "libs/version is missing or empty, run scripts/next-version.sh first" >&2
  exit 1
fi

NEW_VERSION=$(cat libs/version)
NEW_TAG="v${NEW_VERSION}"

if git rev-parse -q --verify "refs/tags/$NEW_TAG" >/dev/null; then
  echo "Tag $NEW_TAG already exists" >&2
  exit 1
fi

LAST_TAG=${LAST_TAG:-$(git tag --sort=-creatordate | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | head -n 1 || true)}

RELEASE_NOTES=$(git log ${LAST_TAG:+$LAST_TAG..HEAD} --pretty=format:"* %s")

echo "Releasing $NEW_TAG"
echo "$RELEASE_NOTES"

git tag "$NEW_TAG"
git push origin "$NEW_TAG"

gh release create "$NEW_TAG" -t "$NEW_TAG" -n "$RELEASE_NOTES"

if [ -n "$GITHUB_STEP_SUMMARY" ]; then
  {
    echo "## Release $NEW_TAG"
    echo ""
    echo "$RELEASE_NOTES"
  } >> "$GITHUB_STEP_SUMMARY"
fi
