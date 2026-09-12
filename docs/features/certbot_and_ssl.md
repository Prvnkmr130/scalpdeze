# Certbot, SSL & Domain Configuration Guide

This guide details the dual-stage SSL architecture in DeltaZero26, prerequisites for production domain verification, automated issuance via Let's Encrypt / Certbot, manual testing commands, and troubleshooting procedures for Cloud VPS deployments.

---

## 1. SSL Architecture Overview

DeltaZero26 implements a **zero-downtime, dual-mode SSL termination system**:

```
                  ┌────────────────────────────────────────┐
                  │          Inbound Web Traffic           │
                  │   HTTP (Port 80) / HTTPS (Port 443)   │
                  └───────────────────┬────────────────────┘
                                      │
                                      ▼
                  ┌────────────────────────────────────────┐
                  │                 Nginx                  │
                  │  (Reverse Proxy / SSL Termination)     │
                  └───────┬────────────────────────┬───────┘
                          │                        │
       /.well-known/acme-challenge/       All other traffic
                          │                        │
                          ▼                        ▼
       ┌──────────────────────────────┐  ┌─────────────────┐
       │   Docker Volume: certbot_    │  │  Django ASGI /  │
       │           webroot            │  │  Uvicorn (8000) │
       │     (/var/www/certbot)       │  └─────────────────┘
       └──────────────┬───────────────┘
                      │
                      ▼
       ┌──────────────────────────────┐
       │      Certbot Container       │
       │    (certbot/certbot:latest)  │
       │ Issues Let's Encrypt cert to │
       │   /etc/letsencrypt/live/     │
       └──────────────────────────────┘
```

### Dual-Stage Certificate Resolution:
1. **Stage 1 (Initial Boot / Local Fallback)**:
   - On container startup, Nginx checks if a valid Let's Encrypt certificate exists in `/etc/letsencrypt/live/*/fullchain.pem`.
   - If not found (e.g. initial setup or local development), Nginx falls back to the bundled self-signed SSL certificate (`/etc/nginx/ssl/nginx-selfsigned.crt`), allowing all trading services to start without blocking.
2. **Stage 2 (Automated Let's Encrypt Binding)**:
   - The secondary `certbot` container attempts the ACME webroot challenge against Let's Encrypt.
   - Once issued, certificates are persisted in the named volume `certbot_certs` (`/etc/letsencrypt`).
   - On Nginx startup or reload, [`supervisord.conf`](file:///c:/Users/Admin/Documents/deltazero26/supervisord.conf#L52) automatically switches the active Nginx configuration to the real Let's Encrypt certificate.

---

## 2. VPS Prerequisites

Before deploying and requesting certificates on your cloud VPS:

1. **DNS A-Record Configuration**:
   - Point your apex domain (`yourdomain.com`) and subdomains (`www.yourdomain.com`) to your VPS public IPv4 address.
   - Verify propagation using `dig` or `nslookup`:
     ```bash
     dig +short yourdomain.com
     ```
2. **Firewall & Security Group Rules**:
   - Allow inbound TCP traffic on **Port 80** (HTTP for ACME verification) and **Port 443** (HTTPS).
   - In UFW:
     ```bash
     sudo ufw allow 80/tcp
     sudo ufw allow 443/tcp
     sudo ufw reload
     ```
3. **Environment Variables (`.env`)**:
   - Ensure the following variables are configured:
     ```ini
     DOMAIN_NAME=yourdomain.com
     LETSENCRYPT_EMAIL=your-email@example.com
     ```

---

## 3. Production Deployment & Issuance

### A. Automatic Issuance via Docker Compose
When starting the platform:
```bash
docker compose up -d
```
The `certbot` container executes automatically and verifies domain ownership via `/var/www/certbot/.well-known/acme-challenge/`.

### B. Checking Certbot Status
Inspect the container logs to verify successful issuance:
```bash
docker compose logs certbot
```
Expected output:
```
Successfully received certificate.
Certificate is saved at: /etc/letsencrypt/live/yourdomain.com/fullchain.pem
Key is saved at:         /etc/letsencrypt/live/yourdomain.com/privkey.pem
✅ Certbot init complete
```

---

## 4. Manual Testing & Renewal Commands

If you need to re-issue, test in dry-run mode, or manually trigger renewal:

### Dry-Run Test (Simulate Certificate Issuance)
```bash
docker compose run --rm certbot certonly --webroot -w /var/www/certbot \
  --email your-email@example.com --agree-tos --no-eff-email --dry-run \
  -d yourdomain.com -d www.yourdomain.com
```

### Force Certificate Issuance / Renewal
```bash
docker compose run --rm certbot certonly --webroot -w /var/www/certbot \
  --email your-email@example.com --agree-tos --no-eff-email --force-renewal \
  -d yourdomain.com -d www.yourdomain.com
```

### Reload Nginx to Apply New Certificates
```bash
docker compose exec app supervisorctl restart nginx
```

---

## 5. Automated Certificate Renewal Cron

Let's Encrypt certificates are valid for 90 days. To schedule automated bi-monthly renewals on your host VPS:

1. Open crontab:
   ```bash
   sudo crontab -e
   ```
2. Add the renewal job (runs every Sunday at 03:30 AM):
   ```cron
   30 3 * * 0 cd /path/to/deltazero26 && docker compose run --rm certbot renew --webroot -w /var/www/certbot --quiet && docker compose exec -T app supervisorctl restart nginx
   ```

---

## 6. Troubleshooting Common Issues

| Issue / Error | Cause | Resolution |
| :--- | :--- | :--- |
| **`Connection refused` on Port 80** | Cloud firewall or UFW blocking Port 80. | Open inbound port 80 on AWS Security Group / DigitalOcean Cloud Firewall and run `sudo ufw allow 80/tcp`. |
| **`DNS problem: NXDOMAIN looking up A for...`** | DNS A-record not yet propagated or incorrect IP. | Run `dig +short yourdomain.com` to confirm DNS matches your VPS IP before running Certbot. |
| **`404 Not Found` during ACME challenge** | Nginx not routing `/.well-known/acme-challenge/` to `/var/www/certbot`. | Verify `certbot_webroot` volume is mounted in both `app` and `certbot` services in `docker-compose.yml`. |
| **`Too many requests` (Rate Limit)** | Exceeded Let's Encrypt limit (5 failed attempts per hour). | Run with `--dry-run` flag first until verified, or use a temporary subdomain. |
