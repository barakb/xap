@echo off
set SHA=
for /f "delims=" %%a in ('git rev-parse HEAD') do @set SHA=%%a
echo [ \"xap-open\":\"%SHA%\" ] > xap-open-metadata.txt