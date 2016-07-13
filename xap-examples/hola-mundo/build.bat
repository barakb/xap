@echo off

call "%~dp0..\..\..\bin\setenv.bat"
set DIR_NAME=%~dp0
rem maven needed to upgraded to 3.2.5
if not defined M2_HOME set M2_HOME=%XAP_HOME%\tools\maven\apache-maven-3.2.5

if "%1" == "clean" (
  cd %DIR_NAME%
  call "%M2_HOME%\bin\mvn" clean
  cd %CD%
) else (
	if "%1" == "compile" (
	    cd %DIR_NAME%
		call "%M2_HOME%\bin\mvn" compile
		cd %CD%
	) else (
		if "%1" == "package" (
		    cd %DIR_NAME%		
			call "%M2_HOME%\bin\mvn" package
			cd %CD%
		) else (
			if "%1" == "run-translator" (
				call ..\..\bin\pu-instance.bat -path %DIR_NAME%\translator\target\hola-mundo-translator.jar
			) else (
				if "%1" == "run-feeder" (
				    call ..\..\bin\pu-instance.bat -path %DIR_NAME%\feeder\target\hola-mundo-feeder.jar
				) else (
				    if "%1" == "run-partitioned-translator" (
                        call ..\..\bin\pu-instance.bat -path %DIR_NAME%\translator\target\hola-mundo-translator.jar -cluster schema=partitioned total_members=2,0
                    ) else (
                        if "%1" == "intellij" (
                            xcopy %DIR_NAME%\runConfigurations %DIR_NAME%\.idea\runConfigurations\
                        ) else (
                            echo.
                            echo "Error: Invalid input command %1"
                            echo.
                            echo The available commands are:
                            echo.
                            echo clean                      --^> Cleans all output dirs
                            echo compile                    --^> Builds all (don't create jars)
                            echo package                    --^> Builds the distribution jars
                            echo run-translator             --^> Starts the translator (single data-grid)
                            echo run-feeder                 --^> Starts the phrase feeder
                            echo run-partitioned-translator --^> Starts a partitioned translator (data-grid of 2 partitions)
                            echo intellij                   --^> Copies run configuration into existing .idea IntelliJ IDE folder
                            echo.
                        )
                    )
				)
			)
		)
	)
)
 



