#!/bin/bash

DIR_NAME=$(dirname ${BASH_SOURCE[0]})

if [ "${M2_HOME}" = "" ] ; then
    M2_HOME="../../tools/maven/apache-maven-3.2.5"; export M2_HOME
fi

if [ -z "$1" ] || ([ $1 != "clean" ] && [ $1 != "compile" ] && [ $1 != "package" ] && [ $1 != "run-translator" ] && [ $1 != "run-feeder" ] && [ $1 != "run-partitioned-translator" ] && [ $1 != "intellij" ]); then
  echo ""
  echo "Error: Invalid input command $1 "
  echo ""
  echo "The available commands are:"
  echo ""
  echo "clean                       --> Cleans all output dirs"
  echo "compile                     --> Builds all (don't create jars)"
  echo "package                     --> Builds the distribution jars"
  echo "run-translator              --> Starts the translator (single data-grid)"
  echo "run-feeder                  --> Starts the phrase feeder"
  echo "run-partitioned-translator  --> Starts a partitioned translator (data-grid of 2 partitions)"
  echo "intellij                    --> Copies run configuration into existing .idea IntelliJ IDE folder"
  echo
  echo "**note:** IntelliJ command assumes that the new IntelliJ project is under the examples root"
  echo "          directory, e.g. the .idea hidden folder is at this location."

elif [ $1 = "clean" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn clean; )

elif [ $1 = "compile" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn compile; )

elif [ $1 = "package" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn --batch-mode package; )

elif [ $1 = "run-translator" ]; then
  ../../bin/pu-instance.sh -path ${DIR_NAME}/translator/target/hola-mundo-translator.jar

elif [ $1 = "run-feeder" ]; then
  ../../bin/pu-instance.sh -path ${DIR_NAME}/feeder/target/hola-mundo-feeder.jar

elif [ $1 = "run-partitioned-translator" ]; then
  ../../bin/pu-instance.sh -path ${DIR_NAME}/translator/target/hola-mundo-translator.jar -cluster schema=partitioned total_members=2,0

elif [ $1 = "intellij" ]; then
  cp -r $DIR_NAME/runConfigurations $DIR_NAME/.idea
  echo "Run configurations for IntelliJ IDE copied into .idea folder"
fi
