@echo off

@rem The call to setenv.bat can be commented out if necessary.
@call "%~dp0\setenv.bat"

@echo Starting a Processing Unit Instance
set COMMAND_LINE=%JAVACMD% %JAVA_OPTIONS% %XAP_OPTIONS% -Djava.security.policy=%XAP_SECURITY_POLICY% -classpath %PRE_CLASSPATH%;%GS_JARS%;%SPRING_JARS%;%POST_CLASSPATH% org.openspaces.pu.container.standalone.StandaloneProcessingUnitContainer %*

set TITLE="Processing Unit Instance ["%*"] started on [%computername%]"
@title %TITLE%

echo.
echo.
echo Starting puInstance with line:
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