export JSHOMEDIR=`dirname $0`/../..
. `dirname $0`/../../bin/setenv.sh

REPL_CLASSPATH="-cp $GS_JARS:$SPRING_JARS:$SIGAR_JARS:"${XAP_HOME}"/lib/optional/scala/lib/*:"${XAP_HOME}"/lib/optional/scala/*"
REPL_MAIN_CLASS="org.openspaces.scala.repl.GigaSpacesScalaRepl"
REPL_COMPILER_OPTS="-usejavacp -Yrepl-sync"
REPL_COMMAND="$JAVACMD $XAP_OPTIONS $REPL_CLASSPATH $REPL_MAIN_CLASS $REPL_COMPILER_OPTS"

$REPL_COMMAND
