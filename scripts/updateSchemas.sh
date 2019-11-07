#!/bin/bash

MYDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

curl http://mindsnacks.github.io/Zinc/schemas/v1/catalog.json > "${MYDIR}/../src/zinc/resources/schemas/v1/catalog.json"
curl http://mindsnacks.github.io/Zinc/schemas/v1/manifest.json > "${MYDIR}/../src/zinc/resources/schemas/v1/manifest.json"
