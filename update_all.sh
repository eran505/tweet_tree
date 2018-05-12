#!/usr/bin/env bash

info={$1}

git add -A

data_today="$(date +"%m/%d %H:%M:%S")"

if [ -z "${info}" ]; then
    git commit -m "$(data_today)_"${info}
else
    git commit -m "$(date +"%m/%d %H:%M:%S")"
fi

git push
