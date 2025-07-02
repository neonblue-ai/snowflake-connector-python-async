#!/bin/bash
set -e

# Get current version from version.py
CURRENT_VERSION=$(python3 -c "
import sys
sys.path.insert(0, 'src')
from snowflake.connector.version import VERSION
print(f'{VERSION[0]}.{VERSION[1]}.{VERSION[2]}')
")
CURRENT_BUILD=$(cat aio-version)

echo "Current version: $CURRENT_VERSION"
echo "Current build: $CURRENT_BUILD"

NEW_BUILD=$((CURRENT_BUILD+1))
NEW_TAG="$CURRENT_VERSION.$NEW_BUILD"

echo "New tag: $NEW_TAG"
echo "$NEW_BUILD" > aio-version

# Commit changes
git add aio-version
git commit -m "Bump aio version to $NEW_TAG"

# Create and push tag
TAG="v$NEW_TAG"
git tag -a "$TAG" -m "Release $TAG"

echo "Version bumped to $NEW_TAG and tagged as $TAG"
echo "To push: git push origin aio && git push origin $TAG"

