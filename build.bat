@echo off
echo Building XAP...
pushd %~dp0
call mvn clean install %*
popd
echo Finished building XAP
