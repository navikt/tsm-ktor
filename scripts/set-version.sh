#!/bin/bash
set -eo pipefail

TAG=$(git tag --sort=-creatordate | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | head -n 1)
if [ -z "$TAG" ]; then
  echo "No version tag found"
  exit 1
fi

VERSION=${TAG#v}
echo -n "$VERSION" > lib/version
echo "Set version $VERSION in lib/version"
