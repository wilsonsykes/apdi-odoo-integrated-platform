# 09 - SMB and Network Share Setup

## Current Working Source Path
`\\192.168.2.177\Users\Public\Merchandise Pictures`

## Why IP Path is Used
- hostname resolution for `mpc2` may fail on APDI server
- IP UNC path avoids DNS/NetBIOS dependency

## Validation Commands (on APDI server)
```powershell
Test-NetConnection 192.168.2.177 -Port 445
Test-Path "\\192.168.2.177\Users\Public\Merchandise Pictures"
```

## Access Modes
### Preferred
- Authenticated SMB account access

### Fallback (current in some environments)
- Guest/no-auth SMB with insecure guest logon enabled
- Use only in trusted internal LAN

## If `Test-Path` is False
1. verify source share exists and is published
2. verify share and NTFS permissions
3. verify firewall and SMB services
4. use IP UNC instead of hostname

## If Dashboard Cannot Click During Long Tasks
- Streamlit request is still running
- restart Streamlit process and refresh client browser
