#!/bin/bash
# This script provides the command and control utility for the 
# GigaSpaces Technologies lookup-service.
# The lookup-service script starts a Lookup Service using the LookupServiceFactory utility.
# 		XAP_LUS_OPTIONS 	- Extended java options that are proprietary defined for Lookup Instance such as heap size, system properties or other JVM arguments.
#										- These settings can be overridden externally to this script.

# XAP_LUS_OPTIONS=; export XAP_LUS_OPTIONS

XAP_COMPONENT_OPTIONS="${XAP_LUS_OPTIONS}"
export XAP_COMPONENT_OPTIONS

. `dirname $0`/setenv.sh

echo Starting a Reggie Jini Lookup Service instance
if [ "${XAP_HOME}" = "" ] ; then
  XAP_HOME=`dirname $0`/..
fi
export XAP_HOME

COMMAND_LINE="${JAVACMD} ${JAVA_OPTIONS} ${XAP_COMPONENT_OPTIONS} ${XAP_OPTIONS} -classpath "${GS_JARS}" com.gigaspaces.internal.lookup.LookupServiceFactory"

echo
echo
echo Starting Jini Lookup Service instance with line:
echo ${COMMAND_LINE}

${COMMAND_LINE}
echo
echo