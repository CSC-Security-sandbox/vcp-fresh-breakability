#!/usr/bin/env bash
set -euo pipefail

cd vsa-cp-cd

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
    echo "Usage: $0 [TEST_FILE] [CONFIG_FILE] [JUNIT_XML_OUTPUT]"
    exit 0
fi

TEST_FILE="${1:-tests/ccfe/billing/test_harvest_leases.py}"
CONFIG_FILE="${2:-/var/test_config_proxy_local.json}"
JUNIT_XML="${3:-out.xml}"

export TEST_FILE CONFIG_FILE JUNIT_XML

python3 -m venv ./.venv
source ./.venv/bin/activate
python3 -m pip install poetry
pip install --upgrade pip

# Handle corrupted lock file: automatically detect and regenerate if needed
# This works without modifying the repository's poetry.lock file
echo "Checking poetry.lock file..."
set +e  # Temporarily disable exit on error
LOCK_OUTPUT=$(poetry lock --no-update 2>&1)
LOCK_EXIT=$?
set -e  # Re-enable exit on error

if [ "$LOCK_EXIT" -ne 0 ]; then
    if echo "$LOCK_OUTPUT" | grep -qE "Cannot declare|Unable to read the lock file"; then
        echo "WARNING: Lock file is corrupted (duplicate entries detected)"
        echo "Automatically regenerating lock file from pyproject.toml..."
        rm -f poetry.lock
        poetry lock
        echo "Lock file regenerated successfully"
    else
        # Other errors - try to update, then regenerate if needed
        echo "Lock file validation failed, attempting update..."
        set +e
        poetry lock
        LOCK_EXIT=$?
        set -e
        if [ "$LOCK_EXIT" -ne 0 ]; then
            echo "Lock file update failed, regenerating from scratch..."
            rm -f poetry.lock
            poetry lock
        fi
    fi
else
    echo "Lock file is valid"
fi

poetry install --no-root

echo poetry run pytest "$TEST_FILE" --test-config "$CONFIG_FILE" --clean-alluredir --alluredir=allure-results --junit-xml "$JUNIT_XML"

poetry run pytest "$TEST_FILE" --test-config "$CONFIG_FILE" --clean-alluredir --alluredir=allure-results --junit-xml "$JUNIT_XML"