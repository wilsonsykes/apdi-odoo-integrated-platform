@echo off
setlocal EnableExtensions EnableDelayedExpansion

REM Usage:
REM   setup_guest_smb.cmd [Mode] [SourceHost] [ShareName] [SharePath]
REM Example:
REM   setup_guest_smb.cmd Both 192.168.2.177 Users "C:\Users\Public\Merchandise Pictures"

set "MODE=%~1"
if "%MODE%"=="" set "MODE=Both"
set "SOURCE_HOST=%~2"
if "%SOURCE_HOST%"=="" set "SOURCE_HOST=192.168.2.177"
set "SHARE_NAME=%~3"
if "%SHARE_NAME%"=="" set "SHARE_NAME=Users"
set "SHARE_PATH=%~4"
if "%SHARE_PATH%"=="" set "SHARE_PATH=C:\Users\Public\Merchandise Pictures"

echo.
echo Mode       : %MODE%
echo SourceHost : %SOURCE_HOST%
echo ShareName  : %SHARE_NAME%
echo SharePath  : %SHARE_PATH%
echo.

net session >nul 2>&1
if not "%errorlevel%"=="0" (
  echo [ERROR] Please run this CMD as Administrator.
  exit /b 1
)

echo [1/5] Enabling discovery and file sharing firewall groups...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Set-NetFirewallRule -DisplayGroup 'Network Discovery' -Enabled True -ErrorAction SilentlyContinue; " ^
  "Set-NetFirewallRule -DisplayGroup 'File and Printer Sharing' -Enabled True -ErrorAction SilentlyContinue"

echo [2/5] Starting required services...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$svc='fdPHost','FDResPub','SSDPSRV','upnphost','LanmanServer','LanmanWorkstation'; " ^
  "foreach($s in $svc){Set-Service -Name $s -StartupType Automatic -ErrorAction SilentlyContinue; Start-Service -Name $s -ErrorAction SilentlyContinue}"

if /I "%MODE%"=="Source" goto :SOURCE
if /I "%MODE%"=="Both" goto :SOURCE
goto :CLIENT

:SOURCE
echo [3/5] Applying source machine guest share settings...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$p='%SHARE_PATH%'; $sn='%SHARE_NAME%'; " ^
  "if(!(Test-Path $p)){New-Item -ItemType Directory -Path $p -Force | Out-Null}; " ^
  "New-ItemProperty -Path 'HKLM:\SYSTEM\CurrentControlSet\Control\Lsa' -Name forceguest -PropertyType DWord -Value 1 -Force | Out-Null; " ^
  "Set-SmbServerConfiguration -EnableAuthenticateUserSharing $false -Force | Out-Null; " ^
  "icacls $p /grant 'Everyone:(RX)' /T /C | Out-Null; " ^
  "$parent=Split-Path $p -Parent; " ^
  "$sh=Get-SmbShare -Name $sn -ErrorAction SilentlyContinue; " ^
  "if(!$sh){New-SmbShare -Name $sn -Path $parent -ReadAccess 'Everyone' | Out-Null} else {Grant-SmbShareAccess -Name $sn -AccountName 'Everyone' -AccessRight Read -Force | Out-Null}"

:CLIENT
if /I "%MODE%"=="Client" goto :CLIENTONLY
if /I "%MODE%"=="Both" goto :CLIENTONLY
goto :TEST

:CLIENTONLY
echo [4/5] Enabling client insecure guest SMB logon...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "Set-SmbClientConfiguration -EnableInsecureGuestLogons $true -Force | Out-Null"

:TEST
echo [5/5] Testing UNC reachability...
set "UNC_PATH=\\%SOURCE_HOST%\%SHARE_NAME%\Public\Merchandise Pictures"
echo UNC: %UNC_PATH%
powershell -NoProfile -ExecutionPolicy Bypass -Command "Test-Path '%UNC_PATH%'"

echo.
echo Done.
echo If Test-Path returned True, use this in dashboard:
echo   %UNC_PATH%
echo.
endlocal
exit /b 0

