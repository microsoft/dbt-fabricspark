#!/bin/bash -xe

GIT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || echo "$(pwd)")

# Install node packages if this particular repo has a package.json
if [ -f "$GIT_ROOT/package.json" ]; then
    npm install
fi

echo "Hatch (Python): $(hatch --version)"
