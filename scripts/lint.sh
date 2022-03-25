#!/bin/sh

set -e
set -x

isort --profile black --check-only bin demo examples pgsync plugins scripts tests
black --check bin demo examples pgsync plugins scripts tests
