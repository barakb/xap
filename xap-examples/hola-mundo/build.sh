#!/bin/bash

DIR_NAME=$(dirname ${BASH_SOURCE[0]})

if [ "${M2_HOME}" = "" ] ; then
    M2_HOME="${XAP_HOME}/tools/maven/apache-maven-3.2.5"; export M2_HOME
fi

if [ -z "$1" ] || ([ $1 != "clean" ] && [ $1 != "compile" ] && [ $1 != "package" ] && [ $1 != "run-single-translator" ] && [ $1 != "run-clustered-translator" ] && [ $1 != "run-feeder" ] && [ $1 != "intellij" ]); then
  echo ""
  echo "Error: Invalid input command $1 "
  echo ""
  echo "The available commands are:"
  echo ""
  echo "clean                    --> Cleans all output dirs"
  echo "compile                  --> Builds all; don't create JARs"
  echo "package                  --> Builds the distribution"
  echo "run-single-translator    --> Starts the translator on a single cluster data-grid"
  echo "run-clustered-translator --> Starts the translator on a two clusters data-grid"
  echo "run-feeder               --> Starts the feeder"
  echo "intellij                 --> Creates run configuration for IntelliJ IDE"
  echo

elif [ $1 = "clean" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn clean; )

elif [ $1 = "compile" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn compile; )

elif [ $1 = "package" ]; then
  (cd $DIR_NAME;
  ${M2_HOME}/bin/mvn --batch-mode package; )

elif [ $1 = "run-single-translator" ]; then
  ../../bin/pu-instance.sh -path ${DIR_NAME}/translator/target/hola-mundo-translator.jar

elif [ $1 = "run-clustered-translator" ]; then
../../bin/pu-instance.sh -path ${DIR_NAME}/translator/target/hola-mundo-translator.jar -cluster schema=partitioned total_members=2,0

elif [ $1 = "run-feeder" ]; then
  ../../bin/pu-instance.sh -path ${DIR_NAME}/phrase-feeder/target/hola-mundo-feeder.jar

elif [ $1 = "intellij" ]; then
  cp -r $DIR_NAME/runConfigurations $DIR_NAME/.idea
  echo "Run configurations for IntelliJ IDE created successfully"
fi
