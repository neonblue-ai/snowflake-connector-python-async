#!/bin/bash
set -e

# Check for clean repository
if [[ -n $(git status --porcelain) ]]; then
    echo "Error: Repository is not clean. Please commit or stash changes first."
    git status
    exit 1
fi

# Check GitHub authentication
if ! gh auth status >/dev/null 2>&1; then
    echo "Error: Not authenticated with GitHub. Please run 'gh auth login' first."
    exit 1
fi

echo "Repository is clean and GitHub authentication verified."

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
git push origin aio && git push origin $TAG

# Create GitHub release
echo "Creating GitHub release..."
gh release create "$TAG" --title "Release $TAG" --notes "AIO version $NEW_TAG" --target aio

