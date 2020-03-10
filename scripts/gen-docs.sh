#!/bin/sh

USER="adam-fowler"
REPOSITORY="s3-filesystem-kit"

set -eux

jazzy --clean

# stash everything that isn't in docs, store result in STASH_RESULT
STASH_RESULT=$(git stash push -- ":(exclude)docs")
# get branch name
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
REVISION_HASH=$(git rev-parse HEAD)

git checkout gh-pages
# copy contents of docs to current replacing the ones that are already there
rm -rf current
mv docs/ current/
# commit
git add --all current
git commit -m "Documentation for https://github.com/$USER/$REPOSITORY/tree/$REVISION_HASH"
git push
# return to branch
git checkout $CURRENT_BRANCH

if [ "$STASH_RESULT" != "No local changes to save" ]; then
    git stash pop
fi

