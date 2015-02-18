#!/bin/sh

mongoimport --db test --type json --collection zips --file test_data/zips.json
