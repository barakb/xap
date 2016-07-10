@echo off
rem This script provides the command and control utility for the 
rem GigaSpaces Technologies lookup-service.
rem The lookup-service script starts a Lookup Service using the LookupServiceFactory utility.

rem 		XAP_LUS_OPTIONS 	- Extended java options that are proprietary defined for Lookup Instance such as heap size, system properties or other JVM arguments.
rem										- These settings can be overridden externally to this script.

@rem set XAP_LUS_OPTIONS
set XAP_COMPONENT_OPTIONS=%XAP_LUS_OPTIONS%

@call "%~dp0\setenv.bat"
rem Use local variables
rem setlocal

@echo Starting a Reggie Jini Lookup Service instance
@set XAP_HOME=%~dp0\..

set TITLE="Lookup Service started on [%computername%]"Groo
@title %TITLE%

set COMMAND_LINE=%JAVACMD% %JAVA_OPTIONS% %XAP_COMPONENT_OPTIONS% %XAP_OPTIONS% -classpath %GS_JARS% com.gigaspaces.internal.lookup.LookupServiceFactory

echo.
echo.
echo Starting lookup-service with line:
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