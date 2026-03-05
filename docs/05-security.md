# 05 - Security Guidelines

## Secrets
- Keep Odoo and PostgreSQL credentials in `.env` only.
- Never hardcode secrets in scripts.
- Never commit `.env`.

## Odoo Access
- Use dedicated API user.
- Limit model permissions to required scope.

## PostgreSQL Access
- Prefer least-privilege DB role for runtime operations.
- Ensure backup/restore for `apdireports`, including `sync_state`, `sync_runs`, and `product_images`.

## SMB/Network Share Security
Two modes exist for image source access:

### Recommended
- Authenticated SMB account for share access.

### Current fallback (less secure)
- Insecure guest SMB logon enabled on client/server.
- Use only on trusted internal network.
- Document and review this exception periodically.

## Dashboard Exposure
- If dashboard is LAN-only/no-auth, restrict by network ACL/firewall.
- Limit port `8501` access to trusted subnets.

## Logging
- Avoid printing secrets.
- Avoid full sensitive payload dumps.
- Keep operational logs with retention policy.
