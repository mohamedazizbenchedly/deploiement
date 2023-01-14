#!/bin/bash
source ~/.bash_profile
# Specify the main class of your application
MAIN_CLASS=org.example.MainProg

# Specify the URL of the Spark master
MASTER_URL=spark://aziz-virtual-machine:7077

start-slave.sh $MASTER_URL

# spark-shell --master spark://aziz-virtual-machine:7077 --deploy-mode client

# Submit the application to Spark
spark-submit --master $MASTER_URL --class $MAIN_CLASS projectSpark/target/projectSpark-1.0-SNAPSHOT.jar 
