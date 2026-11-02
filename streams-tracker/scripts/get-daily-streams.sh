#!/usr/bin/env bash

while [[ $(pgrep -fl stream-gatherer.sh) ]]; do
    sleep 1m
done

now=$(date +"%Y-%m-%d_%H-%M")

mongoexport -d twitch -c streams -o dump-"${now}".json
mongo twitch --eval "db.dropDatabase()"

