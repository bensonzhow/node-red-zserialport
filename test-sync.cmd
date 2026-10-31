@echo off
setlocal
pushd "%~dp0"
if errorlevel 1 exit /b 1
powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File "%~dp0test-sync.ps1" %*
set "TEST_EXIT_CODE=%ERRORLEVEL%"
echo Sync probe exit code: %TEST_EXIT_CODE%
popd
exit /b %TEST_EXIT_CODE%
