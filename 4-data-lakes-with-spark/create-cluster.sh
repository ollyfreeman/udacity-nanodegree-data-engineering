#!/bin/sh

aws emr create-cluster --name $1 --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark --ec2-attributes KeyName=$2 --instance-type m5.xlarge
