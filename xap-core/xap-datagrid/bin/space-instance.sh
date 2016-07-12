#!/bin/bash
# This script provides the command and control utility for the 
# GigaSpaces Technologies space-instance.
# The space-instance script starts a space using the IntegratedProcessingUnitContainer utility.

# Usage: If no args are passed, it will use the default space configuration.
# One argument can be passed through a command line:

# arg 1  - the space arguments, will be set into the SPACE_ARGS variable.
# If no arguments passed the system will load single data grid instance.

# E.g. space-instance "-cluster schema=partitioned total_members=2,1 id=2 backup_id=1"
# or space-instance
# In this case it will use the default space arguments

# 		SPACE_INSTANCE_JAVA_OPTIONS 	- Extended java options that are proprietary defined for Space Instance such as heap size, system properties or other JVM arguments.
#										- These settings can be overridden externally to this script.

# XAP_SPACE_INSTANCE_OPTIONS=; export XAP_SPACE_INSTANCE_OPTIONS
XAP_COMPONENT_OPTIONS="${XAP_SPACE_INSTANCE_OPTIONS}"
export XAP_COMPONENT_OPTIONS

# The call to setenv.sh can be commented out if necessary.
. `dirname $0`/setenv.sh

echo Starting a Space Instance
if [ "${XAP_HOME}" = "" ] ; then
  XAP_HOME=`dirname $0`/..
fi
export XAP_HOME

CPS=":"
export CPS

SPACE_ARGS="$*"
echo Running with the following arguments: [${SPACE_ARGS}]

if [ "$1" = "" ] ; then
  SPACE_ARGS="--help"
fi

COMMAND_LINE="${JAVACMD} ${JAVA_OPTIONS} ${XAP_COMPONENT_OPTIONS} ${XAP_OPTIONS} -classpath "${PRE_CLASSPATH}:${XAP_HOME}/deploy/templates/datagrid:${GS_JARS}:${POST_CLASSPATH}" org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer "${SPACE_ARGS}""

echo
echo
echo Starting space-instance with line:
echo ${COMMAND_LINE}

${COMMAND_LINE}
echo
echo