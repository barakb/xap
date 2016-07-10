@echo off
rem This script provides the command and control utility for the
rem GigaSpaces Technologies space-instance.
rem The space-instance script starts a space using the IntegratedProcessingUnitContainer utility.

rem Usage: If no args are passed, it will use the default space configuration.
rem One argument can be passed through a command line:

rem arg 1  - the space arguments, will be set into the SPACE_ARGS variable.
rem If no arguments passed the system will load single data grid instance.

rem E.g. space-instance "-cluster schema=partitioned total_members=2,1 id=2 backup_id=1"
rem or space-instance
rem In this case it will use the default space arguments

rem 		SPACE_INSTANCE_JAVA_OPTIONS 	- Extended java options that are proprietary defined for Space Instance such as heap size, system properties or other JVM arguments.
rem										- These settings can be overridden externally to this script.

@rem set XAP_SPACE_INSTANCE_OPTIONS
set XAP_COMPONENT_OPTIONS=%XAP_SPACE_INSTANCE_OPTIONS%

@rem The call to setenv.bat can be commented out if necessary.
@call "%~dp0\setenv.bat"
rem Use local variables
rem setlocal

@echo Starting a Space Instance
@set XAP_HOME=%~dp0\..

set SPACE_INSTANCE_ARGS=%*
echo Running with the following arguments: %SPACE_INSTANCE_ARGS%

set TITLE="Space Instance ["%SPACE_URL%"] started on [%computername%]"
@title %TITLE%

set COMMAND_LINE=%JAVACMD% %JAVA_OPTIONS% %XAP_COMPONENT_OPTIONS% %XAP_OPTIONS% -classpath %PRE_CLASSPATH%;%XAP_HOME%\deploy\templates\datagrid;%GS_JARS%;%POST_CLASSPATH% org.openspaces.pu.container.integrated.IntegratedProcessingUnitContainer %SPACE_INSTANCE_ARGS%

echo.
echo.
echo Starting space-instance with line:
echo %COMMAND_LINE%
echo.

%COMMAND_LINE%
goto end

:end
endlocal
title Command Prompt
set TITLE=
if "%OS%"=="Windows_NT" @endlocal
if "%OS%"=="WINNT" @endlocal
exit /B 0