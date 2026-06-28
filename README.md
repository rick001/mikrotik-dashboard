# MikroTik Network Dashboard

A lightweight, real-time network monitoring dashboard for MikroTik routers with dual-WAN PCC load balancing. Built with FastAPI + Chart.js, deployed via Docker on a local server.

![Dashboard Preview](https://github.com/rick001/mikrotik-dashboard/raw/main/preview.png)

## Features

- **WAN Status** — live UP/DOWN badges, ISP-assigned IPs, gateway ping latency (pinged from the server, not the router)
- **Interface Traffic** — live Kbps/Mbps speeds, session totals, boot totals, sparkline charts per WAN
- **PCC Load Distribution** — active connection counts and % split per WAN, derived purely from connection-mark tracking (not mangle counters)
- **Failover Events** — last 10 WAN up/down events parsed from RouterOS logs
- **Connection Sessions** — TCP, UDP, Established, and New/sec from the connection tracking table
- **System Health** — CPU gauge, RAM gauge, temperature, active DHCP clients, storage
- **Combined Throughput** — 5-minute rolling chart with Kbps/Mbps auto-scaling

## Architecture

```
MikroTik hEX (RouterOS 7.x)
        │  REST API (port 80)
        ▼
Ubuntu Server (192.168.10.2)
  └── Docker container
        ├── FastAPI backend  :1999/api/stats
        └── Static HTML/JS  :1999/
```

The FastAPI backend polls the MikroTik REST API every 5 seconds and serves aggregated data to the frontend. The frontend fetches `/api/stats` every 5 seconds and updates the UI without page reloads.

## Requirements

- MikroTik router running RouterOS 7.x with REST API enabled
- Docker + Docker Compose on the monitoring server
- Network connectivity from the server to the router's LAN IP

## MikroTik Setup

### 1. Create a read-only API user

```routeros
/user group add name=dashboard policy=read,api,rest-api
/user add name=dashboard group=dashboard password=your_password
```

### 2. Verify Netwatch for WAN monitoring

If you already have a dual-WAN PCC setup, Netwatch entries are likely already configured. Check first:

```routeros
/tool netwatch print
```

You need one entry per WAN with a comment containing `WAN1` and `WAN2` — the dashboard matches on these strings. Example of a correctly configured setup:

```
# TYPE    HOST          TIMEOUT  INTERVAL  STATUS
0 simple  172.28.62.1   3s       10s       up     ;;; WAN1 health check
1 simple  192.168.29.1  3s       10s       up     ;;; WAN2 health check
```

> The host can be anything (gateway IP, 8.8.8.8, 1.1.1.1) — what matters is that the comment contains `WAN1` or `WAN2`.

If Netwatch is **not** configured, add entries manually:

```routeros
/tool netwatch add host=8.8.8.8 interval=10s comment="WAN1 health check"
/tool netwatch add host=1.1.1.1 interval=10s comment="WAN2 health check"
```

In a PCC setup, you also typically have static routes to force each probe through its respective WAN interface so monitoring is accurate. Check with:

```routeros
/ip route print where dst-address=8.8.8.8 or dst-address=1.1.1.1
```

If missing, add them (substitute your gateway IPs):

```routeros
/ip route add dst-address=8.8.8.8/32 gateway=<WAN1-gateway> comment="WAN1 health check route"
/ip route add dst-address=1.1.1.1/32 gateway=<WAN2-gateway> comment="WAN2 health check route"
```

### 3. Verify REST API access

```bash
curl -u dashboard:your_password http://192.168.10.1/rest/system/resource
```

## Installation

### 1. Clone the repo

```bash
git clone https://github.com/rick001/mikrotik-dashboard.git
cd mikrotik-dashboard
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:

```env
MIKROTIK_HOST=192.168.10.1
MIKROTIK_USER=dashboard
MIKROTIK_PASS=your_password
POLL_INTERVAL=5
```

### 3. Build and run

```bash
docker compose up -d --build
```

Dashboard is now available at `http://<server-ip>:1999`

### Updating

```bash
git pull
docker compose down && docker compose build --no-cache && docker compose up -d
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `MIKROTIK_HOST` | `192.168.10.1` | Router LAN IP |
| `MIKROTIK_USER` | `dashboard` | RouterOS username |
| `MIKROTIK_PASS` | _(required)_ | RouterOS password |
| `POLL_INTERVAL` | `5` | Poll interval in seconds |

## Resource Usage

Designed to be lightweight on the router:

| Metric | Value |
|---|---|
| REST API calls per poll | 8 concurrent (interfaces, resource, health, netwatch, leases, routes, addresses, mangle) |
| Connection tracking fetch | Every 30s |
| Log fetch | Every 15s |
| Router CPU impact | < 1% on hEX (tested on RB750Gr3) |
| No outbound calls from router | Gateway ping runs from the server, not RouterOS |

## Tested On

- **Router**: MikroTik hEX (RB750Gr3), RouterOS 7.23.1
- **Setup**: Dual-WAN PCC load balancing (SSWL static + JIO DHCP)
- **Server**: Ubuntu 22.04, Docker 24+

## Notes on Public IPs

Both ISPs in this setup use CGNAT, so the IPs shown under "ISP IP" are the addresses assigned to the router's WAN interfaces — not globally routable public IPs. This is expected and normal for most residential/CGNAT connections. The dashboard shows these accurately per-WAN from the `/ip/address` endpoint with no external lookups.

## License

MIT
