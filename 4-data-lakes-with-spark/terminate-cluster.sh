#!/bin/sh

aws emr terminate-clusters --cluster-ids $1
