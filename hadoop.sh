#!/bin/bash

#  the main 
MAIN_CLASS=org.example.MainProg

#  the URL of the YARN 
YARN_RM=127.0.0.1:8088

# Submit the application to YARN
spark-submit --class $MAIN_CLASS --master yarn --deploy-mode client --num-executors 2 --executor-memory 1G --executor-cores 1 projectSpark/target/projectSpark-1.0-SNAPSHOT.jar