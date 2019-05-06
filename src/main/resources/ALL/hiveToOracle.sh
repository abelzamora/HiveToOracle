#!/usr/bin/env bash

yarn jar export-0.0.1-SNAPSHOT-jar-with-dependencies.jar es.hablapps.export.Export config_cgp.yaml 2018-08-01 2018-08-03 \
-Dmapreduce.job.ubertask.enable=false \
-Dmapreduce.task.timeout=3600000 \
-Dmapreduce.jobtracker.expire.trackers.interval=3600000