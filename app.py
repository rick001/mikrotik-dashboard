import asyncio
import logging
import os
import re
import time
from collections import deque
from datetime import datetime
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

WAN_LABELS = {
    "wan1": "WAN1 · SSWL",
    "wan2": "WAN2 · JIO",
}

# Exact messages emitted by RouterOS failover scripts (REST has no message~ regex).
FAILOVER_LOG_MESSAGES = [
    "WAN1 DOWN - disabling routes and mangle rules",
    "WAN1 UP - restoring routes and mangle rules",
    "WAN2 DOWN - disabling routes and mangle rules",
    "WAN2 UP - restoring routes and mangle rules",
]

DEDUP_WINDOW_SEC = 5

state: dict = {
    "prev_bytes":         {},
    "prev_time":          None,
    "session_start_bytes":{},
    "conn_counts":        {"wan1": 0, "wan2": 0},
    "poll_count":         0,
    "latest":             {},
    "failover_events":    deque(maxlen=40),
    "failover_event_count": 0,  # unique DOWNs from log seed + live DOWNs
    "failover_outages":   [],
    "failover_summary":   {"total": 0, "last_duration_sec": None, "last_duration": None, "open": "none"},
    "prev_wan_snapshot":  None,   # None until first baseline
    "prev_primary_wan":   None,
    "logs_seeded":        False,
    "log_seed_ok":        False,
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


async def mt_post(client: httpx.AsyncClient, path: str, body: dict, timeout: float = 20.0):
    try:
        r = await client.post(
            f"http://{MIKROTIK_HOST}/rest{path}",
            auth=(MIKROTIK_USER, MIKROTIK_PASS),
            json=body,
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()
        logger.warning(f"MT POST {path} → {r.status_code}")
    except Exception as e:
        logger.warning(f"MT POST {path} failed: {e}")
    return None


def _event_time(now: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(now))


def _parse_log_time(s: str) -> float | None:
    """Parse RouterOS log time to epoch seconds."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%b/%d/%Y %H:%M:%S", "%H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%H:%M:%S":
                today = datetime.now()
                dt = dt.replace(year=today.year, month=today.month, day=today.day)
            return dt.timestamp()
        except ValueError:
            continue
    return None


def _normalize_failover_message(msg: str) -> str:
    """Stable dedupe key: wan1|down, wan2|up, or pathing|<msg>."""
    m = msg or ""
    upper = m.upper()
    wan = None
    if "WAN1" in upper:
        wan = "wan1"
    elif "WAN2" in upper:
        wan = "wan2"
    if wan:
        if "DOWN" in upper:
            return f"{wan}|down"
        if "UP" in upper:
            return f"{wan}|up"
    if m.startswith("Pathing →") or m.startswith("Pathing ->"):
        return f"pathing|{m}"
    return f"other|{m}"


def _should_keep_failover_event(message: str, event_ts: float, recent: list) -> bool:
    """Keep unless same normalized key appears within DEDUP_WINDOW_SEC."""
    key = _normalize_failover_message(message)
    for prev in reversed(recent):
        prev_ts = prev.get("ts")
        if prev_ts is None:
            continue
        if event_ts - prev_ts > DEDUP_WINDOW_SEC:
            break
        if prev.get("key") == key:
            return False
    return True


def _format_duration(sec: float) -> str:
    sec = max(0, int(sec))
    days, rem = divmod(sec, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {mins}m"
    if mins > 0:
        return f"{mins}m" if secs == 0 else f"{mins}m {secs}s"
    return f"{secs}s"


def _format_outage_start(ts: float, time_str: str = "") -> str:
    if time_str and len(time_str) >= 16 and time_str[4] == "-":
        return time_str[:16]
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(ts))


def build_failover_outages(events, now: float) -> list[dict]:
    """Pair WAN DOWN→UP into outage incidents (last 10)."""
    open_map: dict[str, dict] = {}
    closed: list[dict] = []

    for e in sorted(events, key=lambda x: x.get("ts") or 0):
        key = e.get("key") or _normalize_failover_message(e.get("message", ""))
        if "|" not in key or key.startswith("pathing") or key.startswith("other"):
            continue
        wan, direction = key.split("|", 1)
        if wan not in WAN_LABELS:
            continue
        if direction == "down":
            open_map[wan] = e
        elif direction == "up" and wan in open_map:
            start = open_map.pop(wan)
            start_ts = float(start.get("ts") or 0)
            end_ts = float(e.get("ts") or now)
            closed.append({
                "wan": wan,
                "label": WAN_LABELS[wan],
                "started": _format_outage_start(start_ts, start.get("time", "")),
                "ended": _format_outage_start(end_ts, e.get("time", "")),
                "duration_sec": max(0, int(end_ts - start_ts)),
                "duration": _format_duration(end_ts - start_ts),
                "status": "recovered",
                "_ts": start_ts,
            })

    for wan, start in open_map.items():
        start_ts = float(start.get("ts") or 0)
        closed.append({
            "wan": wan,
            "label": WAN_LABELS[wan],
            "started": _format_outage_start(start_ts, start.get("time", "")),
            "ended": None,
            "duration_sec": max(0, int(now - start_ts)),
            "duration": _format_duration(now - start_ts),
            "status": "ongoing",
            "_ts": start_ts,
        })

    closed.sort(key=lambda o: o["_ts"])
    return closed[-10:]


def build_failover_summary(outages: list[dict], wan_status: dict | None = None) -> dict:
    total = int(state.get("failover_event_count", 0))
    last = outages[-1] if outages else None
    open_labels = [o["label"] for o in outages if o.get("status") == "ongoing"]
    if not open_labels and wan_status:
        for key, label in WAN_LABELS.items():
            entry = wan_status.get(key)
            if entry and entry.get("status") == "down":
                open_labels.append(label)
    return {
        "total": total,
        "last_duration_sec": last["duration_sec"] if last else None,
        "last_duration": last["duration"] if last else None,
        "open": ", ".join(open_labels) if open_labels else "none",
    }


def _public_failover_outages(outages: list[dict]) -> list[dict]:
    return [
        {
            "wan": o["wan"],
            "label": o["label"],
            "started": o["started"],
            "ended": o.get("ended"),
            "duration_sec": o["duration_sec"],
            "duration": o["duration"],
            "status": o["status"],
        }
        for o in outages
    ]


def _refresh_failover_views(now: float, wan_status: dict | None = None) -> None:
    outages = build_failover_outages(list(state["failover_events"]), now)
    state["failover_outages"] = outages
    state["failover_summary"] = build_failover_summary(outages, wan_status)


def _append_failover_event(message: str, now: float) -> None:
    """Append a UI event with 5s-window dedupe. Only DOWN increments count."""
    if not _should_keep_failover_event(message, now, list(state["failover_events"])):
        return
    state["failover_events"].append({
        "time": _event_time(now),
        "message": message,
        "ts": now,
        "key": _normalize_failover_message(message),
    })
    if "DOWN" in message:
        state["failover_event_count"] = int(state.get("failover_event_count", 0)) + 1
    _refresh_failover_views(now)


async def seed_failover_from_logs(client: httpx.AsyncClient) -> None:
    """One-shot filtered log seed — exact script messages only, not full /log dump."""
    if state.get("logs_seeded"):
        return
    state["logs_seeded"] = True

    # 4-way OR in RouterOS query stack: ((a|b)|c)|d
    query = [f"message={m}" for m in FAILOVER_LOG_MESSAGES] + ["#|", "#|", "#|"]
    logs = await mt_post(
        client,
        "/log/print",
        {".proplist": ["time", "message"], ".query": query},
        timeout=20.0,
    )
    if not logs or not isinstance(logs, list):
        logger.warning("Failover log seed failed or empty; continuing with Netwatch-only history")
        return

    dated: list[dict] = []
    for entry in logs:
        t = entry.get("time", "") or ""
        msg = entry.get("message", "") or ""
        ts = _parse_log_time(t)
        if ts is None:
            continue
        dated.append({"time": t, "message": msg, "ts": ts})

    dated.sort(key=lambda e: e["ts"])

    kept: list[dict] = []
    for entry in dated:
        if not _should_keep_failover_event(entry["message"], entry["ts"], kept):
            continue
        kept.append({
            "time": entry["time"],
            "message": entry["message"],
            "ts": entry["ts"],
            "key": _normalize_failover_message(entry["message"]),
        })

    down_count = sum(1 for e in kept if "DOWN" in e["message"])
    state["failover_event_count"] = down_count
    state["failover_events"].clear()
    for e in kept[-40:]:
        state["failover_events"].append(e)
    state["log_seed_ok"] = True
    now = time.time()
    _refresh_failover_views(now)
    logger.info(
        "Failover log seed: %d raw → %d after %ds dedupe, %d DOWNs, %d outages",
        len(logs),
        len(kept),
        DEDUP_WINDOW_SEC,
        down_count,
        len(state["failover_outages"]),
    )


def _wan_snapshot(wan_status: dict) -> dict:
    snap = {}
    for key in ("wan1", "wan2"):
        entry = wan_status.get(key)
        if entry:
            snap[key] = {
                "status": entry.get("status", "unknown"),
                "since":  entry.get("since", ""),
            }
        else:
            snap[key] = None
    return snap


def record_failover_transitions(wan_status: dict, primary_wan: str, now: float) -> None:
    """Derive live Failover Events from Netwatch + pathing changes."""
    snap = _wan_snapshot(wan_status)
    prev = state["prev_wan_snapshot"]

    if prev is None:
        # Baseline: if log seed already filled history, skip mid-outage synthetic events.
        if not state.get("log_seed_ok"):
            for key, label in WAN_LABELS.items():
                cur = snap.get(key)
                if cur and cur["status"] == "down":
                    since = cur.get("since") or ""
                    msg = f"{label} DOWN"
                    if since:
                        msg = f"{msg} (since {since.replace('T', ' ')[:16]})"
                    _append_failover_event(msg, now)
        state["prev_wan_snapshot"] = snap
        state["prev_primary_wan"] = primary_wan
        return

    for key, label in WAN_LABELS.items():
        cur = snap.get(key)
        old = prev.get(key)
        if not cur or not old:
            continue
        if cur["status"] != old["status"] and cur["status"] in ("up", "down"):
            _append_failover_event(f"{label} {cur['status'].upper()}", now)

    prev_primary = state["prev_primary_wan"]
    if prev_primary is not None and primary_wan != prev_primary:
        _append_failover_event(f"Pathing → {primary_wan}", now)

    state["prev_wan_snapshot"] = snap
    state["prev_primary_wan"] = primary_wan


# ── Main poll loop ──────────────────────────────────────────────────────────

async def poll():
    async with httpx.AsyncClient() as client:
        await seed_failover_from_logs(client)
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

                # ── Pathing mode from Netwatch (not route distance) ───────
                w1s = (wan_status.get("wan1") or {}).get("status")
                w2s = (wan_status.get("wan2") or {}).get("status")
                if w1s == "up" and w2s == "up":
                    primary_wan = "Load Balanced (both up)"
                elif w1s == "up" and w2s != "up":
                    primary_wan = "WAN1 · SSWL"
                elif w2s == "up" and w1s != "up":
                    primary_wan = "WAN2 · JIO"
                elif w1s == "down" and w2s == "down":
                    primary_wan = "Both WANs DOWN"
                else:
                    primary_wan = "Unknown"

                # ── Failover events from Netwatch / pathing transitions ───
                record_failover_transitions(wan_status, primary_wan, now)
                _refresh_failover_views(now, wan_status)

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
                    "failover_event_count": int(state.get("failover_event_count", 0)),
                    "failover_summary": dict(state.get("failover_summary") or {}),
                    "failover_outages": _public_failover_outages(
                        state.get("failover_outages") or []
                    ),
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
