#!/bin/bash

# Specify the main class of your application
MAIN_CLASS=org.example.MainProg

# Specify the URL of the YARN resource manager
YARN_RM=127.0.0.1:8088

# Submit the application to YARN
spark-submit --class $MAIN_CLASS --master yarn --deploy-mode client --num-executors 2 --executor-memory 1G --executor-cores 1 projectSpark/target/projectSpark-1.0-SNAPSHOT.jar