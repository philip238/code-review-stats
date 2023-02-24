#!/usr/bin/env bash

export PYTHONPATH="$(pwd):$PYTHONPATH"
mkdir -p data/reports

PYTHON=$(which python3)
if [[ -z "${VIRTUAL_ENV}" ]]; then
    PYTHON="pipenv run"
fi

DATA_FILES=$(find data/raw -type f)
if [[ -z "$DATA_FILES" ]]; then
    echo "You must run the download scripts first to generate reports."
    exit 1
fi

OLD_DATA=$(find "data/raw" -mtime +14 -print)
if [[ -n "$OLD_DATA" ]]; then
    echo "WARNING: Your data is older than 14 days, you should re-download it."
fi

REPORTS=$(find reports -mindepth 1 -maxdepth 1 -type d -exec basename {} \;)
for REPORT in $REPORTS; do
    title="Generating report: $REPORT" 
    echo "================================================================================"
    printf "%*s\n" $(((${#title}+80)/2)) "$title"
    echo "================================================================================"
    $PYTHON ./reports/$REPORT/transform_data.py
    $PYTHON ./reports/$REPORT/visualize_data.py \
        -f data/raw/transformed.json \
        data/reports/$REPORT.html
    echo "================================================================================"
done
