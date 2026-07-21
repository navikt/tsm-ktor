#!/bin/bash
set -o pipefail

# Find last version tag
LAST_TAG=$(git tag --sort=-creatordate | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | head -n 1)
if [ -z "$LAST_TAG" ]; then
  echo "No previous tag found"
  BASE_COMMIT=""
else
  BASE_COMMIT="$LAST_TAG"
fi

SECOND_LINE=$(git log -1 --pretty=format:"%b")

case "$SECOND_LINE" in
  MAJOR) BUMP="major" ;;
  MINOR) BUMP="minor" ;;
  *)     BUMP="patch" ;;
esac


# Parse last version
if [ -z "$LAST_TAG" ]; then
  MAJOR=0; MINOR=0; PATCH=0
else
  VERSION=${LAST_TAG#v}
  IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"
fi

case "$BUMP" in
  major) ((MAJOR++)); MINOR=0; PATCH=0 ;;
  minor) ((MINOR++)); PATCH=0 ;;
  patch) ((PATCH++)) ;;
esac
NEW_VERSION="${MAJOR}.${MINOR}.${PATCH}"
NEW_TAG="v${NEW_VERSION}"

# Collect commits since last tag
RAW_COMMITS=$(git log ${BASE_COMMIT:+$BASE_COMMIT..HEAD} --pretty=format:"%s%n%b%n---")

echo "Relevant commits:"
echo "$RAW_COMMITS"

RELEASE_NOTES=$(git log ${BASE_COMMIT:+$BASE_COMMIT..HEAD} --pretty=format:"* %s")


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
