@echo off
setlocal
set "NODE_SKIP_PLATFORM_CHECK=1"
pushd "%~dp0"
if errorlevel 1 exit /b 1
node "%~dp0test.js" %*
set "TEST_EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %TEST_EXIT_CODE%
