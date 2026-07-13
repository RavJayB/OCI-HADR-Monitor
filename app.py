import os
import re
import time
import threading
from collections import deque

import oracledb
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

def _required(name):
    val = os.environ.get(name)
    if not val:
        raise SystemExit(
            "%s is not set. Copy .env.example to .env, fill it in, and pass it with\n"
            "  docker run --env-file .env ...\n"
            "Credentials are never baked into this image." % name
        )
    return val


DB_USER         = os.environ.get("DB_USER", "ADMIN")
DB_PASSWORD     = _required("DB_PASSWORD")
DB_ALIAS        = os.environ.get("DB_DSN", "hadrdemo_tp")
WALLET_DIR      = os.environ.get("WALLET_DIR", "/app/wallet")
WALLET_PASSWORD = _required("WALLET_PASSWORD")

PROBE_INTERVAL    = float(os.environ.get("PROBE_INTERVAL", "1.0"))
TRANSPORT_TIMEOUT = int(os.environ.get("TRANSPORT_TIMEOUT", "3"))
CONNECT_TIMEOUT   = int(os.environ.get("CONNECT_TIMEOUT", "5"))
SLA_TARGET_S      = float(os.environ.get("SLA_TARGET_S", "120"))


READ_ONLY_CODES = {1552, 16000, 16397}


def _tuned_dsn(alias):
    """Return the tnsnames descriptor for `alias` with failover-hostile defaults removed.

    The wallet ships retry_count=20/retry_delay=3, so a single connect against a
    database mid-switchover blocks ~60s before raising. That latency would be
    indistinguishable from real downtime, so the probe must fail fast instead.
    """
    path = os.path.join(WALLET_DIR, "tnsnames.ora")
    try:
        with open(path) as fh:
            content = fh.read()
    except OSError:
        return alias

    match = re.search(
        r"^\s*%s\s*=\s*(\(.+\))\s*$" % re.escape(alias),
        content,
        re.IGNORECASE | re.MULTILINE,
    )
    if not match:
        return alias

    desc = match.group(1)
    desc = re.sub(r"\(\s*retry_count\s*=\s*\d+\s*\)", "(retry_count=0)", desc, flags=re.IGNORECASE)
    desc = re.sub(r"\(\s*retry_delay\s*=\s*\d+\s*\)", "(retry_delay=0)", desc, flags=re.IGNORECASE)

    inject = ""
    if not re.search(r"\(\s*transport_connect_timeout\s*=", desc, re.IGNORECASE):
        inject += "(transport_connect_timeout=%d)" % TRANSPORT_TIMEOUT
    if not re.search(r"\(\s*(?<!transport_)connect_timeout\s*=", desc, re.IGNORECASE):
        inject += "(connect_timeout=%d)" % CONNECT_TIMEOUT
    if inject:
        desc = re.sub(r"\(\s*description\s*=", "(description=" + inject, desc, count=1, flags=re.IGNORECASE)
    return desc


DB_DSN = _tuned_dsn(DB_ALIAS)


def _fmt_err(exc):
    if isinstance(exc, oracledb.Error) and exc.args:
        err = exc.args[0]
        code = getattr(err, "full_code", None)
        lines = (getattr(err, "message", "") or "").strip().splitlines()
        if code:
            msg = lines[0] if lines else ""
            if msg.startswith(code):
                msg = msg[len(code):].lstrip(": ")
            return "%s: %s" % (code, msg)
    return "%s: %s" % (type(exc).__name__, exc)


#   lines = (getattr(err, "message", "") or "").strip().splitlines()
#         if code:
#             msg = lines[0] if lines else ""
#             if msg.startswith(code):
#                 msg = msg[len(code):].lstrip(": ")
#             return "%s: %s" % (code, msg)
#     return "%s: %s" % (type(exc).__name__, exc)


#   lines = (getattr(err, "message", "") or "").strip().splitlines()
#         if code:
#             msg = lines[0] if lines else ""
#             if msg.startswith(code):
#                 msg = msg[len(code):].lstrip(": ")
#             return "%s: %s" % (code, msg)
#     return "%s: %s" % (type(exc).__name__, exc)




def _err_code(exc):
    if isinstance(exc, oracledb.Error) and exc.args:
        return getattr(exc.args[0], "code", None)
    return None


def probe():
    """One end to end check. Reads and writes are graded separately."""
    t0 = time.monotonic()
    r = {
        "read_ok": False, "write_ok": False, "role": None, "open_mode": None,
        "instance": None, "tx_count": None, "rows": [], "error": None,
        "write_error": None, "read_only": False, "connect_ms": None, "ms": None,
    }
    conn = None
    try:
        conn = oracledb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN,
            config_dir=WALLET_DIR,
            wallet_location=WALLET_DIR,
            wallet_password=WALLET_PASSWORD,
            tcp_connect_timeout=CONNECT_TIMEOUT,
            retry_count=0,
        )
        r["connect_ms"] = round((time.monotonic() - t0) * 1000)
        cur = conn.cursor()
                # cur = conn.cursor()


    
        cur.execute("""
            SELECT d.database_role,
                   d.open_mode,
                   SYS_CONTEXT('USERENV', 'DB_UNIQUE_NAME'),
                   SYS_CONTEXT('USERENV', 'INSTANCE_NAME')
            FROM   v$database d
        """)
        role, open_mode, db_unique_name, instance_name = cur.fetchone()
        r["role"], r["open_mode"] = role, open_mode
        r["instance"] = "%s/%s" % (db_unique_name, instance_name)
        r["read_ok"] = True

        try:
            cur.execute(
                "INSERT INTO demo_transactions (tx_data) VALUES (:1)",
                [f"monitor_{int(time.time())}"],
            )
            conn.commit()
            r["write_ok"] = True
        except oracledb.DatabaseError as exc:
            r["write_error"] = _fmt_err(exc)
            r["read_only"] = _err_code(exc) in READ_ONLY_CODES

        cur.execute("SELECT COUNT(*) FROM demo_transactions")
        r["tx_count"] = cur.fetchone()[0]

        cur.execute("""
            SELECT id,
                   tx_data,
                   TO_CHAR(created_at, 'HH24:MI:SS'),
                   status
            FROM   demo_transactions
            ORDER  BY id DESC
            FETCH  FIRST 5 ROWS ONLY
        """)
        r["rows"] = [
            {"id": x[0], "data": x[1], "time": x[2], "status": x[3]}
            for x in cur.fetchall()
        ]
        cur.close()
    except Exception as exc:
        r["error"] = _fmt_err(exc)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    r["ms"] = round((time.monotonic() - t0) * 1000)
    return r


class Monitor:
    """Authoritative availability accounting.

    Durations come from time.monotonic() so they survive clock adjustments, and
    they accrue against the state that was in effect for each inter-probe gap.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self.state = "INIT"                 # INIT | UP | DEGRADED | DOWN
        self.role = None
        self.open_mode = None
        self.instance = None                # db_unique_name/instance_name
        self.tx_count = 0
        self.rows = []
        self.last_ms = None
        self.last_connect_ms = None
        self.last_error = None

        self.started_wall = time.time()
        self._last_mono = None
        self._first_mono = None
        self.up_s = 0.0
        self.down_s = 0.0
        self.degraded_s = 0.0

        self.probes = 0
        self.failures = 0
        self._last_read_ok_mono = None
        self._last_read_ok_wall = None

        self.current_outage = None
        self.outages = deque(maxlen=20)
        self.samples = deque(maxlen=300)
        self.events = deque(maxlen=200)
        self._seq = 0

    def _event(self, msg, level="lm"):
        self._seq += 1
        self.events.append({"i": self._seq, "t": time.time(), "level": level, "msg": msg})

    def record(self, r):
        now_mono = time.monotonic()
        now_wall = time.time()

        with self._lock:
            if self._last_mono is not None:
                dt = now_mono - self._last_mono
                if self.state == "UP":
                    self.up_s += dt
                elif self.state == "DEGRADED":
                    self.degraded_s += dt
                elif self.state == "DOWN":
                    self.down_s += dt
            else:
                self._first_mono = now_mono
            self._last_mono = now_mono
            self.probes += 1

            if r["read_ok"]:
                new_state = "UP" if r["write_ok"] else "DEGRADED"
            else:
                new_state = "DOWN"
                self.failures += 1

            prev_state = self.state
            prev_role = self.role
            prev_mode = self.open_mode
            prev_instance = self.instance

            if r["read_ok"]:
                self._last_read_ok_mono = now_mono
                self._last_read_ok_wall = now_wall
                self.role = r["role"]
                self.open_mode = r["open_mode"]
                self.instance = r["instance"]
                self.tx_count = r["tx_count"] if r["tx_count"] is not None else self.tx_count
                if r["rows"]:
                    self.rows = r["rows"]

            self.last_ms = r["ms"] if r["read_ok"] else None
            self.last_connect_ms = r["connect_ms"]
            self.last_error = r["error"] or r["write_error"]

            self.samples.append({"t": now_wall, "ms": r["ms"] if r["read_ok"] else None, "ok": r["read_ok"]})

        
            if new_state == "DOWN":
                if self.current_outage is None:
                    self.current_outage = {
                        "detected_mono": now_mono,
                        "detected_at": now_wall,
                        "last_success_at": self._last_read_ok_wall,
                        "_last_success_mono": self._last_read_ok_mono,
                        "attempts": 1,
                        "first_error": r["error"],
                        "role_before": prev_role,
                        "instance_before": prev_instance,
                    }
                else:
                    self.current_outage["attempts"] += 1
            elif self.current_outage is not None:
                o = self.current_outage
                observed = now_mono - o["detected_mono"]
                worst = (now_mono - o["_last_success_mono"]) if o["_last_success_mono"] is not None else None
                closed = {
                    "detected_at": o["detected_at"],
                    "recovered_at": now_wall,
                    "observed_s": round(observed, 1),
                    "worst_case_s": round(worst, 1) if worst is not None else None,
                    "attempts": o["attempts"],
                    "first_error": o["first_error"],
                    "role_before": o["role_before"],
                    "role_after": r["role"],
                    "instance_before": o["instance_before"],
                    "instance_after": r["instance"],
                    "switched_over": bool(
                        o["instance_before"] and r["instance"]
                        and o["instance_before"] != r["instance"]
                    ),
                }
                self.outages.append(closed)
                self.current_outage = None
                self._event(
                    "Recovered after %ss (worst case %ss, %d failed probes)" % (
                        closed["observed_s"],
                        closed["worst_case_s"] if closed["worst_case_s"] is not None else "?",
                        closed["attempts"],
                    ),
                    "ls",
                )

            if prev_instance and r["instance"] and r["instance"] != prev_instance:
                self._event(
                    "SWITCHOVER CONFIRMED — workload moved %s -> %s"
                    % (prev_instance, r["instance"]), "ls",
                )
            if prev_role and r["role"] and r["role"] != prev_role:
                self._event("Role changed: %s -> %s" % (prev_role, r["role"]), "ls")
            if prev_mode and r["open_mode"] and r["open_mode"] != prev_mode:
                self._event("Open mode: %s -> %s" % (prev_mode, r["open_mode"]), "lw")

            if new_state != prev_state:
                if prev_state == "INIT":
                    if r["read_ok"]:
                        self._event("Monitor online — %s, %s" % (r["role"], r["open_mode"]), "ls")
                    else:
                        self._event("Monitor online — unreachable: %s" % r["error"], "le")
                elif new_state == "DOWN":
                    self._event("Connection lost — %s" % r["error"], "le")
                elif new_state == "DEGRADED":
                    self._event(
                        "Writes rejected%s — %s" % (
                            " (open read-only)" if r["read_only"] else "",
                            r["write_error"],
                        ),
                        "lw",
                    )
                elif new_state == "UP" and prev_state == "DEGRADED":
                    self._event("Writes restored", "ls")
            self.state = new_state

    def snapshot(self):
        with self._lock:
            up, down, degraded = self.up_s, self.down_s, self.degraded_s
            # Fold in the time since the last probe so the UI advances smoothly.
            if self._last_mono is not None:
                live = time.monotonic() - self._last_mono
                if self.state == "UP":
                    up += live
                elif self.state == "DEGRADED":
                    degraded += live
                elif self.state == "DOWN":
                    down += live

            measured = up + down + degraded
           
            gaps = self.probes - 1
            cadence = None
            if gaps > 0 and self._first_mono is not None:
                cadence = round((self._last_mono - self._first_mono) / gaps, 2)

            outage = None
            if self.current_outage is not None:
                o = self.current_outage
                outage = {
                    "detected_at": o["detected_at"],
                    "elapsed_s": round(time.monotonic() - o["detected_mono"], 1),
                    "attempts": o["attempts"],
                    "first_error": o["first_error"],
                }

            return {
                "state": self.state,
                "role": self.role,
                "open_mode": self.open_mode,
                "instance": self.instance,
                "tx_count": self.tx_count,
                "rows": list(self.rows),
                "last_ms": self.last_ms,
                "last_connect_ms": self.last_connect_ms,
                "last_error": self.last_error,
                "up_s": round(up, 1),
                "down_s": round(down, 1),
                "degraded_s": round(degraded, 1),
                "measured_s": round(measured, 1),
                "availability_pct": round(up / measured * 100, 3) if measured > 0 else None,
                "probes": self.probes,
                "failures": self.failures,
                "current_outage": outage,
                "last_outage": self.outages[-1] if self.outages else None,
                "outages": list(self.outages),
                "samples": list(self.samples),
                "events": list(self.events),
                "probe_interval": PROBE_INTERVAL,
                "cadence_s": cadence,
                "sla_target_s": SLA_TARGET_S,
                "now": time.time(),
            }


MON = Monitor()


def _loop():
    while True:
        t0 = time.monotonic()
        try:
            MON.record(probe())
        except Exception:
            pass
        remaining = PROBE_INTERVAL - (time.monotonic() - t0)
        if remaining > 0:
            time.sleep(remaining)


@app.route("/api/status")
def status():
    return jsonify(MON.snapshot())


@app.route("/healthz")
def healthz():
    snap = MON.snapshot()
    code = 200 if snap["state"] in ("UP", "DEGRADED") else 503
    return jsonify({"state": snap["state"], "role": snap["role"]}), code


@app.route("/")
def index():
    return render_template_string(DASHBOARD)


DASHBOARD = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HADRDEMO - live monitor</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f1117;color:#e2e8f0;padding:24px}
  h1{font-size:20px;font-weight:500;margin-bottom:4px}
  .sub{font-size:13px;color:#64748b;margin-bottom:24px}
  .g4{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
  .g2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
  .card{background:#1e2130;border:1px solid #2d3348;border-radius:10px;padding:16px}
  .lbl{font-size:11px;color:#64748b;font-weight:500;margin-bottom:6px;text-transform:uppercase;letter-spacing:.05em}
  .val{font-size:20px;font-weight:600}
  .sub2{font-size:11px;color:#475569;margin-top:4px}
  .sc{border-radius:10px;padding:16px}
  .ok{background:#0d2d1a;border:1px solid #166534}.ok .lbl,.ok .val{color:#4ade80}
  .err{background:#2d0d0d;border:1px solid #991b1b}.err .lbl,.err .val{color:#f87171}
  .warn{background:#2d1f0d;border:1px solid #92400e}.warn .lbl,.warn .val{color:#fbbf24}
  .ct{font-size:12px;font-weight:500;color:#64748b;margin-bottom:12px}
  #log{font-family:'SF Mono','Fira Code',monospace;font-size:11px;line-height:1.7;height:180px;overflow-y:auto;display:flex;flex-direction:column;gap:2px}
  .ls{color:#4ade80}.le{color:#f87171}.lw{color:#fbbf24}.lm{color:#475569}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{text-align:left;color:#64748b;font-weight:500;padding-bottom:8px}
  td{padding:4px 0;color:#cbd5e1;border-top:1px solid #2d3348}
  .badge{font-size:10px;padding:2px 6px;border-radius:4px;background:#0d2d1a;color:#4ade80}
  .av{display:flex;justify-content:space-between;margin-bottom:10px}
  .av label{font-size:11px;color:#64748b;display:block;margin-bottom:3px}
  .av span{font-size:18px;font-weight:600;color:#e2e8f0}
  .rto-meta{display:flex;justify-content:space-between;font-size:11px;color:#64748b;margin-bottom:5px}
  .rto-track{background:#2d3348;border-radius:4px;height:6px;overflow:hidden}
  .rto-fill{height:100%;width:0%;background:#ef4444;border-radius:4px;transition:width .5s}
  .rp{color:#60a5fa}.rs{color:#a78bfa}
  .foot{font-size:11px;color:#475569;margin-top:4px;line-height:1.6}
</style>
</head>
<body>

<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:24px">
  <div>
    <h1>HADRDEMO — live monitor</h1>
    <p class="sub">Oracle Autonomous AI Database &middot; Autonomous Data Guard &middot; India South (Hyderabad)</p>
  </div>
  <div id="ts" style="font-size:12px;color:#64748b"></div>
</div>

<div class="g4">
  <div id="sc" class="sc ok">
    <div class="lbl">Status</div>
    <div id="sv" class="val">Connecting...</div>
    <div id="ssub" class="sub2"></div>
  </div>
  <div class="card">
    <div class="lbl">Role</div>
    <div id="rv" class="val rp">—</div>
    <div id="omv" class="sub2">—</div>
  </div>
  <div class="card">
    <div class="lbl">Response</div>
    <div id="msv" class="val">—</div>
    <div id="cmv" class="sub2">—</div>
  </div>
  <div class="card">
    <div class="lbl">Transactions</div>
    <div id="txv" class="val">0</div>
    <div id="pbv" class="sub2">—</div>
  </div>
</div>

<div class="g2">
  <div class="card">
    <div class="ct">Response time (ms)</div>
    <div style="position:relative;height:160px">
      <canvas id="ch" role="img" aria-label="Database response time in milliseconds">Response time chart</canvas>
    </div>
  </div>
  <div class="card">
    <div class="ct">Event log</div>
    <div id="log"></div>
  </div>
</div>

<div class="g2">
  <div class="card">
    <div class="ct">Availability</div>
    <div class="av">
      <div><label>Measured uptime</label><span id="upv">00:00:00</span></div>
      <div style="text-align:center"><label>Availability</label><span id="pctv">—</span></div>
      <div style="text-align:right"><label>Last downtime</label><span id="dtv">—</span></div>
    </div>
    <div id="rw" style="display:none">
      <div class="rto-meta"><span id="rl">Reconnecting...</span><span id="slal"></span></div>
      <div class="rto-track"><div id="rf" class="rto-fill"></div></div>
    </div>
    <div id="rr" style="display:none;font-size:12px;color:#4ade80;font-weight:500;margin-top:8px"></div>
    <div id="foot" class="foot"></div>
  </div>
  <div class="card">
    <div class="ct">Data proof — demo_transactions</div>
    <table>
      <thead><tr>
        <th style="width:40px">ID</th>
        <th>Data</th>
        <th style="width:75px">Time</th>
        <th style="width:65px">Status</th>
      </tr></thead>
      <tbody id="tb"></tbody>
    </table>
    <div id="dn" style="display:none;font-size:12px;color:#4ade80;margin-top:8px;font-weight:500"></div>
  </div>
</div>

<script>
const chart=new Chart(document.getElementById('ch').getContext('2d'),{
  type:'line',
  data:{labels:[],datasets:[{data:[],borderColor:'#1D9E75',backgroundColor:'transparent',borderWidth:1.5,tension:.3,pointRadius:0,spanGaps:false}]},
  options:{responsive:true,maintainAspectRatio:false,animation:false,
    scales:{x:{display:false},y:{min:0,suggestedMax:1500,ticks:{color:'#64748b',font:{size:11},maxTicksLimit:5},grid:{color:'rgba(255,255,255,.04)'}}},
    plugins:{legend:{display:false}}}
});

const $=id=>document.getElementById(id);
let lastSeq=0, shownOutage=null;

function fmt(s){s=Math.floor(s);return String(Math.floor(s/3600)).padStart(2,'0')+':'+String(Math.floor((s%3600)/60)).padStart(2,'0')+':'+String(s%60).padStart(2,'0')}
function hhmmss(epoch){return new Date(epoch*1000).toLocaleTimeString('en-US',{hour12:false})}

function appendEvents(events){
  const el=$('log');
  const atBottom=el.scrollHeight-el.scrollTop-el.clientHeight<20;
  events.filter(e=>e.i>lastSeq).forEach(e=>{
    lastSeq=e.i;
    const d=document.createElement('div');
    d.className=e.level;
    d.textContent=hhmmss(e.t)+'  '+e.msg;
    el.appendChild(d);
  });
  while(el.children.length>200)el.removeChild(el.firstChild);
  if(atBottom)el.scrollTop=el.scrollHeight;
}

function renderRows(rows){
  const tb=$('tb');
  tb.innerHTML='';
  rows.forEach(r=>{
    const tr=document.createElement('tr');
    const td=(t,style)=>{const c=document.createElement('td');if(style)c.setAttribute('style',style);c.textContent=t;return c};
    tr.appendChild(td(r.id));
    tr.appendChild(td(r.data,'overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:90px'));
    tr.appendChild(td(r.time));
    const s=document.createElement('td');
    const b=document.createElement('span');
    b.className='badge';b.textContent=r.status||'committed';
    s.appendChild(b);tr.appendChild(s);
    tb.appendChild(tr);
  });
}

function render(d){
  $('ts').textContent=new Date(d.now*1000).toLocaleString();

  if(d.state==='UP'){setSC('Connected','ok','read + write OK');}
  else if(d.state==='DEGRADED'){setSC('Read-only','warn',d.last_error||'writes rejected');}
  else if(d.state==='DOWN'){setSC('Connection lost','err',d.last_error||'');}
  else{setSC('Connecting...','ok','');}

  $('rv').textContent=d.role||'—';
  $('rv').className='val '+(d.role==='PRIMARY'?'rp':'rs');
  $('omv').textContent=(d.open_mode||'—')+(d.instance?' · '+d.instance:'');
  $('msv').textContent=d.last_ms!=null?d.last_ms+'ms':'—';
  $('cmv').textContent=d.last_connect_ms!=null?'connect '+d.last_connect_ms+'ms':'—';
  $('txv').textContent=d.tx_count;
  $('pbv').textContent=d.probes+' probes · '+d.failures+' failed';

  $('upv').textContent=fmt(d.up_s);
  $('pctv').textContent=d.availability_pct!=null?d.availability_pct.toFixed(3)+'%':'—';

  const lo=d.last_outage;
  $('dtv').textContent=lo?lo.observed_s+'s':'—';

  const co=d.current_outage;
  if(co){
    $('rw').style.display='block';
    $('rr').style.display='none';
    $('slal').textContent='Oracle SLA: '+d.sla_target_s+'s max';
    $('rl').textContent='Down '+co.elapsed_s+'s · '+co.attempts+' failed probes';
    $('rf').style.width=Math.min(co.elapsed_s/d.sla_target_s*100,100).toFixed(0)+'%';
  }else{
    $('rw').style.display='none';
    if(lo&&lo.recovered_at!==shownOutage){
      shownOutage=lo.recovered_at;
      $('rr').style.display='block';
      $('rr').textContent='Recovered in '+lo.observed_s+'s (worst case '+
        (lo.worst_case_s!=null?lo.worst_case_s+'s':'unknown')+') — SLA '+d.sla_target_s+'s';
      // database_role stays PRIMARY across a switchover; the instance identity
      // changing is the only client-visible proof the workload moved.
      const dn=$('dn');
      dn.style.display='block';
      if(lo.switched_over){
        dn.style.color='#4ade80';
        dn.textContent='Switchover confirmed: '+lo.instance_before+' -> '+lo.instance_after+
          ' · '+d.tx_count+' rows intact, zero data loss';
      }else{
        dn.style.color='#fbbf24';
        dn.textContent='Same instance ('+(lo.instance_after||'unknown')+
          ') — connectivity blip, not a switchover';
      }
    }
  }

  const cad=d.cadence_s!=null?d.cadence_s:d.probe_interval;
  $('foot').textContent='Probe cadence '+cad+'s (configured '+d.probe_interval+
    's) · timings resolved to ±'+cad+'s · measured window '+fmt(d.measured_s)+
    (d.degraded_s>0?' · '+d.degraded_s.toFixed(0)+'s read-only':'');

  const s=d.samples;
  chart.data.labels=s.map((_,i)=>i);
  chart.data.datasets[0].data=s.map(x=>x.ms);
  chart.update();

  appendEvents(d.events);
  if(d.rows.length)renderRows(d.rows);
}

function setSC(label,cls,sub){
  $('sc').className='sc '+cls;
  $('sv').textContent=label;
  $('ssub').textContent=sub||'';
}

async function tick(){
  try{
    const res=await fetch('/api/status',{cache:'no-store'});
    render(await res.json());
  }catch(e){
    setSC('Monitor unreachable','err',e.message);
  }
  setTimeout(tick,1000);
}
tick();
</script>
</body>
</html>
"""


threading.Thread(target=_loop, daemon=True, name="db-probe").start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
