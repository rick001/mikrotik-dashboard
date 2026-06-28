import asyncio
import logging
import os
import re
import time
from collections import deque
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MIKROTIK_HOST  = os.getenv("MIKROTIK_HOST",  "192.168.10.1")
MIKROTIK_USER  = os.getenv("MIKROTIK_USER",  "dashboard")
MIKROTIK_PASS  = os.getenv("MIKROTIK_PASS",  "")
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL", "5"))
MAX_HISTORY    = 60   # 5 min at 5 s intervals
CONN_INTERVAL  = 6    # fetch connection tracking every N polls (30 s default)

app = FastAPI()

history: dict[str, deque] = {
    "timestamps": deque(maxlen=MAX_HISTORY),
    "ether1_rx":  deque(maxlen=MAX_HISTORY),
    "ether1_tx":  deque(maxlen=MAX_HISTORY),
    "ether2_rx":  deque(maxlen=MAX_HISTORY),
    "ether2_tx":  deque(maxlen=MAX_HISTORY),
    "cpu":        deque(maxlen=MAX_HISTORY),
    "ram":        deque(maxlen=MAX_HISTORY),
    "total_rx":   deque(maxlen=MAX_HISTORY),
    "total_tx":   deque(maxlen=MAX_HISTORY),
}

state: dict = {
    "prev_bytes":         {},
    "prev_time":          None,
    "session_start_bytes":{},
    "conn_counts":        {"wan1": 0, "wan2": 0},
    "poll_count":         0,
    "latest":             {},
}


# ── Helpers ────────────────────────────────────────────────────────────────

async def ping_ms(host: str) -> Optional[float]:
    """Ping host using system ping, return RTT in ms or None on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ping", "-c", "1", "-W", "2", host,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3.0)
        output = stdout.decode()
        m = re.search(r"time=([\d.]+)\s*ms", output)
        if m:
            return round(float(m.group(1)), 1)
    except Exception:
        pass
    return None


def parse_rtt_ms(s: str | None) -> float | None:
    """Convert RouterOS time string like '3ms', '1s200ms', '500us' → ms float."""
    if not s:
        return None
    ms = 0.0
    m = re.search(r'(\d+)s', s.replace("ms","").replace("us",""))
    if m:
        ms += int(m.group(1)) * 1000
    m = re.search(r'(\d+)ms', s)
    if m:
        ms += int(m.group(1))
    m = re.search(r'(\d+)us', s)
    if m:
        ms += int(m.group(1)) / 1000
    return round(ms, 1)


async def mt_get(client: httpx.AsyncClient, path: str, timeout: float = 4.0):
    try:
        r = await client.get(
            f"http://{MIKROTIK_HOST}/rest{path}",
            auth=(MIKROTIK_USER, MIKROTIK_PASS),
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()
        logger.warning(f"MT {path} → {r.status_code}")
    except Exception as e:
        logger.warning(f"MT {path} failed: {e}")
    return None


# ── Main poll loop ──────────────────────────────────────────────────────────

async def poll():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                now = time.time()
                state["poll_count"] += 1
                pc = state["poll_count"]

                # ── Concurrent fast-poll fetches ──────────────────────────
                (
                    interfaces, sysres, health,
                    netwatch, leases, routes,
                    addresses, mangle,
                ) = await asyncio.gather(
                    mt_get(client, "/interface"),
                    mt_get(client, "/system/resource"),
                    mt_get(client, "/system/health"),
                    mt_get(client, "/tool/netwatch"),
                    mt_get(client, "/ip/dhcp-server/lease"),
                    mt_get(client, "/ip/route"),
                    mt_get(client, "/ip/address"),
                    mt_get(client, "/ip/firewall/mangle"),
                    return_exceptions=False,
                )

                # Slow: connection tracking every CONN_INTERVAL polls
                if pc == 1 or pc % CONN_INTERVAL == 0:
                    all_conns = await mt_get(client, "/ip/firewall/connection", timeout=8.0)
                    if all_conns is not None:
                        w1 = w2 = tcp = udp = established = 0
                        prev_total = state.get("session_summary", {}).get("total", 0)
                        for c in all_conns:
                            mark  = c.get("connection-mark", "").upper()
                            proto = c.get("protocol", "").lower()
                            tstate = c.get("tcp-state", "").lower()
                            if "WAN1" in mark:
                                w1 += 1
                            elif "WAN2" in mark:
                                w2 += 1
                            if proto == "tcp":
                                tcp += 1
                            elif proto == "udp":
                                udp += 1
                            if tstate == "established":
                                established += 1
                        total = len(all_conns)
                        # new/sec = delta since last conn fetch
                        conn_dt = POLL_INTERVAL * CONN_INTERVAL
                        new_per_sec = round(max(0, total - prev_total) / conn_dt, 1) if prev_total else 0
                        state["conn_counts"] = {"wan1": w1, "wan2": w2}
                        state["session_summary"] = {
                            "total": total,
                            "tcp": tcp,
                            "udp": udp,
                            "established": established,
                            "new_per_sec": new_per_sec,
                        }

                # Slow: system log every 3 polls (15 s) for failover events
                failover_events = state.get("failover_events", [])
                if pc % 3 == 0:
                    logs = await mt_get(client, "/log", timeout=5.0)
                    if logs:
                        events = []
                        for entry in logs:
                            msg = entry.get("message", "")
                            if "WAN" in msg and (
                                "DOWN" in msg or "UP" in msg or
                                "restoring" in msg or "disabling" in msg
                            ):
                                events.append({
                                    "time": entry.get("time", ""),
                                    "message": msg,
                                })
                        failover_events = events[-10:]
                    state["failover_events"] = failover_events

                dt = (now - state["prev_time"]) if state["prev_time"] else 1.0

                # ── Interface speeds ──────────────────────────────────────
                speeds: dict = {}
                if interfaces:
                    for iface in interfaces:
                        name = iface.get("name", "")
                        rx = int(iface.get("rx-byte", 0))
                        tx = int(iface.get("tx-byte", 0))
                        prev = state["prev_bytes"].get(name)
                        if prev and dt > 0:
                            rx_mbps = max(0.0, (rx - prev["rx"]) * 8 / dt / 1_000_000)
                            tx_mbps = max(0.0, (tx - prev["tx"]) * 8 / dt / 1_000_000)
                        else:
                            rx_mbps = tx_mbps = 0.0
                        if name not in state["session_start_bytes"]:
                            state["session_start_bytes"][name] = {"rx": rx, "tx": tx}
                        start = state["session_start_bytes"][name]
                        speeds[name] = {
                            "rx_mbps":    round(rx_mbps, 3),
                            "tx_mbps":    round(tx_mbps, 3),
                            "rx_total":   rx,
                            "tx_total":   tx,
                            "session_rx": max(0, rx - start["rx"]),
                            "session_tx": max(0, tx - start["tx"]),
                            "running":    iface.get("running", "false") == "true",
                        }
                        state["prev_bytes"][name] = {"rx": rx, "tx": tx}

                state["prev_time"] = now

                # ── History ───────────────────────────────────────────────
                e1 = speeds.get("ether1", {})
                e2 = speeds.get("ether2", {})
                history["timestamps"].append(int(now * 1000))
                history["ether1_rx"].append(e1.get("rx_mbps", 0))
                history["ether1_tx"].append(e1.get("tx_mbps", 0))
                history["ether2_rx"].append(e2.get("rx_mbps", 0))
                history["ether2_tx"].append(e2.get("tx_mbps", 0))
                history["total_rx"].append(round(e1.get("rx_mbps", 0) + e2.get("rx_mbps", 0), 3))
                history["total_tx"].append(round(e1.get("tx_mbps", 0) + e2.get("tx_mbps", 0), 3))

                # ── System resource ───────────────────────────────────────
                sys_info: dict = {}
                cpu_load = ram_pct = 0
                if sysres:
                    total_mem = int(sysres.get("total-memory", 1))
                    free_mem  = int(sysres.get("free-memory", 0))
                    cpu_load  = int(sysres.get("cpu-load", 0))
                    ram_pct   = round((1 - free_mem / total_mem) * 100, 1)
                    sys_info  = {
                        "uptime":       sysres.get("uptime", ""),
                        "version":      sysres.get("version", ""),
                        "board":        sysres.get("board-name", ""),
                        "cpu_load":     cpu_load,
                        "ram_pct":      ram_pct,
                        "total_memory": total_mem,
                        "free_memory":  free_mem,
                        "total_hdd":    int(sysres.get("total-hdd-space", 0)),
                        "free_hdd":     int(sysres.get("free-hdd-space", 0)),
                        "cpu_count":    int(sysres.get("cpu-count", 1)),
                        "cpu_freq":     sysres.get("cpu-frequency", ""),
                    }
                history["cpu"].append(cpu_load)
                history["ram"].append(ram_pct)

                # ── Temperature ───────────────────────────────────────────
                temperature = None
                if health and isinstance(health, list):
                    for h in health:
                        if h.get("name") == "temperature":
                            temperature = h.get("value")
                            break

                # ── Public IPs from address table ─────────────────────────
                wan_ips: dict = {}
                if addresses:
                    for addr in addresses:
                        iface = addr.get("interface", "")
                        ip    = addr.get("address", "").split("/")[0]
                        if iface == "ether1":
                            wan_ips["wan1"] = ip
                        elif iface == "ether2":
                            wan_ips["wan2"] = ip

                # ── Netwatch: status ──────────────────────────────────────
                wan_status: dict = {"wan1": None, "wan2": None}
                gw_hosts: dict = {}
                if netwatch:
                    for nw in netwatch:
                        comment = nw.get("comment", "")
                        host    = nw.get("host", "")
                        entry = {
                            "status": nw.get("status", "unknown"),
                            "since":  nw.get("since", ""),
                            "host":   host,
                            "rtt_ms": None,  # filled below
                        }
                        if "WAN1" in comment:
                            wan_status["wan1"] = entry
                            gw_hosts["wan1"] = host
                        elif "WAN2" in comment:
                            wan_status["wan2"] = entry
                            gw_hosts["wan2"] = host

                # Ping gateways from Ubuntu every poll
                if gw_hosts:
                    rtt_w1, rtt_w2 = await asyncio.gather(
                        ping_ms(gw_hosts.get("wan1", "")) if gw_hosts.get("wan1") else asyncio.sleep(0),
                        ping_ms(gw_hosts.get("wan2", "")) if gw_hosts.get("wan2") else asyncio.sleep(0),
                    )
                    if wan_status["wan1"]: wan_status["wan1"]["rtt_ms"] = rtt_w1
                    if wan_status["wan2"]: wan_status["wan2"]["rtt_ms"] = rtt_w2

                # ── DHCP active count ─────────────────────────────────────
                dhcp_count = 0
                if leases:
                    dhcp_count = sum(1 for l in leases if l.get("status") == "bound")

                # ── Primary WAN ───────────────────────────────────────────
                primary_wan = "Load Balanced"
                if routes:
                    defaults = sorted(
                        [r for r in routes
                         if r.get("dst-address") == "0.0.0.0/0"
                         and r.get("active", "false") == "true"],
                        key=lambda x: int(x.get("distance", 99))
                    )
                    if defaults:
                        gw = defaults[0].get("gateway", "")
                        if "172.28" in gw:
                            primary_wan = "WAN1 · SSWL"
                        elif "192.168.29" in gw:
                            primary_wan = "WAN2 · JIO"

                # ── PCC distribution from active connection marks ─────────
                cc   = dict(state["conn_counts"])
                w1c  = cc.get("wan1", 0)
                w2c  = cc.get("wan2", 0)
                ctot = w1c + w2c
                pcc: dict = {
                    "wan1_count": w1c,
                    "wan2_count": w2c,
                    "wan1_pct":   round(w1c / ctot * 100) if ctot > 0 else None,
                    "wan2_pct":   round(w2c / ctot * 100) if ctot > 0 else None,
                    "ready":      ctot > 0,
                }

                state["latest"] = {
                    "timestamp":       int(now * 1000),
                    "speeds":          speeds,
                    "system":          sys_info,
                    "temperature":     temperature,
                    "wan_status":      wan_status,
                    "wan_ips":         wan_ips,
                    "primary_wan":     primary_wan,
                    "dhcp_count":      dhcp_count,
                    "pcc":             pcc,
                    "session_summary": dict(state.get("session_summary", {})),
                    "failover_events": list(state.get("failover_events", [])),
                    "history":         {k: list(v) for k, v in history.items()},
                }

            except Exception as e:
                logger.error(f"Poll error: {e}", exc_info=True)

            await asyncio.sleep(POLL_INTERVAL)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(poll())


@app.get("/api/stats")
async def get_stats():
    if not state["latest"]:
        return JSONResponse({"error": "warming up"}, status_code=503)
    return JSONResponse(state["latest"])


@app.get("/api/health")
async def api_health():
    return {"status": "ok"}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
