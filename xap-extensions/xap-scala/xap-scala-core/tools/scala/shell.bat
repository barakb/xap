@call "%~dp0\..\..\bin\setenv.bat"

set REPL_CLASSPATH=-cp %GS_JARS%;%SPRING_JARS%;%SIGAR_JARS%;"%XAP_HOME%\lib\platform\scala\lib\*;";"%XAP_HOME%\lib\platform\scala\*;"
set REPL_MAIN_CLASS=org.openspaces.scala.repl.GigaSpacesScalaRepl
set REPL_COMPILER_OPTS=-usejavacp -Yrepl-sync
set REPL_COMMAND=%JAVACMD% %XAP_OPTIONS% %REPL_CLASSPATH% %REPL_MAIN_CLASS% %REPL_COMPILER_OPTS%

%REPL_COMMAND%
