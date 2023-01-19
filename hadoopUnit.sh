#!/bin/bash

# fonction qui permet de telecharge et de desarchiver hadoop 

fonction downloadHadoopUnit() { 
 WGET https://archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz -p ./
 	echo « downloading hadoop with done... »
tar -xzf archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
	echo « Unarchive done ... »
}


# active cette fonction si hadoop n'est pas installe sur votre machine 

## downloadHadoopUnit()


dirname = $(cd $(dirname $ 0) ;  pwd )
className = org.example.MainProg
app_jar =  /projectSpark/target/projectSpark-1.0-SNAPSHOT.jar

--files $(dir/application.conf) 


## installer maven pour recuperce le jar de l'application 
## mvn install

# Submit the application
spark-submit --master yarn --class $MAIN_CLASS --deploy-mode client --executor-memory 1G --files $(dir/application.conf) $app_jar

 echo « the deployment on hadoopUnit was done successfully ...»