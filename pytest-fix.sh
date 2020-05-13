#!/usr/bin/env bash
# NOTE: https://github.com/pytest-dev/pytest/issues/2042
# Run this script if you switch between multiple environments multiple environments (host machine, docker, vm, etc.)
hash -r
find . -name '*.py[odc]' -type f -delete
find . -name '__pycache__' -type d -delete
rm -Rf .pytest_cache
rm -rf *.egg-info .cache .eggs build dist
pip install -e .
