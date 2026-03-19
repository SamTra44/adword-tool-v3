from flask import Flask, request, jsonify, send_from_directory, session, redirect, url_for
from flask_cors import CORS
import requests as req
import os, sqlite3, threading, time, uuid
from datetime import datetime

app = Flask(__name__, static_folder="static")
CORS(app)
app.secret_key = os.environ.get("SECRET_KEY", "adword_v3_secret_x9k2m")

# ── CONFIG ───────────────────────────────────────────────
API_KEY    = os.environ.get("SMM_API_KEY", "877f4a9fcf5d5770b86f97867beea5bc")
API_URL    = "https://honestsmm.com/api/v2"
SERVICES   = {
    "1554": "SV4 FB Live Stream Views | 90 Min",
    "1236": "FB Live Stream Views | 90 Min | Instant Start"
}
USERS = {
    "admin":  {"password": os.environ.get("ADMIN_PASS", "Admin@123"),  "role": "admin"},
    "rozmin": {"password": os.environ.get("ROZMIN_PASS", "Secure@123"), "role": "user"},
}
DB_PATH = os.environ.get("DB_PATH", "adword.db")
# ─────────────────────────────────────────────────────────

# ── DATABASE ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS balance_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            txn_id TEXT UNIQUE NOT NULL,
            amount_usd REAL NOT NULL,
            added_by TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            order_num INTEGER NOT NULL,
            link TEXT NOT NULL,
            service_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            status TEXT NOT NULL,
            smm_order_id TEXT,
            cost_usd REAL DEFAULT 0,
            created_at TEXT NOT NULL,
            username TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_sessions (
            id TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            link TEXT NOT NULL,
            service_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            total_orders INTEGER NOT NULL,
            gap_minutes INTEGER NOT NULL,
            placed INTEGER DEFAULT 0,
            success INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """)
        db.commit()

init_db()

# ── BALANCE HELPERS ──────────────────────────────────────
def get_balance():
    with get_db() as db:
        row = db.execute("SELECT COALESCE(SUM(amount_usd),0) as bal FROM balance_logs").fetchone()
        return round(float(row["bal"]), 4)

def get_smm_balance():
    try:
        r = req.post(API_URL, data={"key": API_KEY, "action": "balance"}, timeout=10)
        d = r.json()
        if "balance" in d:
            return float(d["balance"])
    except:
        pass
    return None

# ── BACKGROUND ORDER WORKER ──────────────────────────────
active_jobs = {}  # session_id -> threading.Event (stop signal)

def order_worker(session_id):
    stop_ev = active_jobs.get(session_id)

    with get_db() as db:
        job = db.execute("SELECT * FROM job_sessions WHERE id=?", (session_id,)).fetchone()
        if not job:
            return

    link      = job["link"]
    svc       = job["service_id"]
    qty       = job["quantity"]
    total     = job["total_orders"]
    gap_sec   = job["gap_minutes"] * 60
    username  = job["username"]

    for i in range(1, total + 1):
        if stop_ev and stop_ev.is_set():
            break

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        try:
            r = req.post(API_URL, data={
                "key": API_KEY, "action": "add",
                "service": svc, "link": link, "quantity": qty
            }, timeout=15)
            d = r.json()

            if "order" in d:
                smm_id = str(d["order"])
                with get_db() as db:
                    db.execute("""INSERT INTO orders
                        (session_id, order_num, link, service_id, quantity, status, smm_order_id, created_at, username)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                        (session_id, i, link, svc, qty, "success", smm_id, now, username))
                    db.execute("""UPDATE job_sessions SET placed=placed+1, success=success+1,
                        updated_at=? WHERE id=?""", (now, session_id))
                    db.commit()

            elif "error" in d:
                err = d["error"].lower()
                is_balance = any(w in err for w in ["balance","fund","credit","insufficient"])
                with get_db() as db:
                    db.execute("""INSERT INTO orders
                        (session_id, order_num, link, service_id, quantity, status, smm_order_id, created_at, username)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                        (session_id, i, link, svc, qty,
                         "balance_low" if is_balance else "failed",
                         d["error"], now, username))
                    db.execute("""UPDATE job_sessions SET placed=placed+1, failed=failed+1,
                        updated_at=? WHERE id=?""", (now, session_id))
                    db.commit()
                if is_balance:
                    break
            else:
                with get_db() as db:
                    db.execute("""INSERT INTO orders
                        (session_id, order_num, link, service_id, quantity, status, smm_order_id, created_at, username)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                        (session_id, i, link, svc, qty, "failed", "unknown", now, username))
                    db.execute("""UPDATE job_sessions SET placed=placed+1, failed=failed+1,
                        updated_at=? WHERE id=?""", (now, session_id))
                    db.commit()

        except Exception as e:
            now2 = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            with get_db() as db:
                db.execute("""INSERT INTO orders
                    (session_id, order_num, link, service_id, quantity, status, smm_order_id, created_at, username)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (session_id, i, link, svc, qty, "error", str(e), now2, username))
                db.execute("""UPDATE job_sessions SET placed=placed+1, failed=failed+1,
                    updated_at=? WHERE id=?""", (now2, session_id))
                db.commit()

        if i < total and not (stop_ev and stop_ev.is_set()):
            stop_ev.wait(gap_sec)

    # Mark done
    final_status = "stopped" if (stop_ev and stop_ev.is_set()) else "completed"
    now3 = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as db:
        db.execute("UPDATE job_sessions SET status=?, updated_at=? WHERE id=?",
                   (final_status, now3, session_id))
        db.commit()
    active_jobs.pop(session_id, None)

# ── AUTH ─────────────────────────────────────────────────
def login_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("username"):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get("role") != "admin":
            return jsonify({"error": "Admin only"}), 403
        return f(*args, **kwargs)
    return decorated

# ── ROUTES — STATIC ──────────────────────────────────────
@app.route("/")
def index():
    if not session.get("username"):
        return send_from_directory("static", "login.html")
    if session.get("role") == "admin":
        return send_from_directory("static", "admin.html")
    return send_from_directory("static", "dashboard.html")

@app.route("/dashboard")
def dashboard():
    if not session.get("username"):
        return send_from_directory("static", "login.html")
    return send_from_directory("static", "dashboard.html")

# ── AUTH ROUTES ───────────────────────────────────────────
@app.route("/login", methods=["POST"])
def login():
    data = request.json
    u = data.get("username","").strip()
    p = data.get("password","").strip()
    if u in USERS and USERS[u]["password"] == p:
        session["username"] = u
        session["role"]     = USERS[u]["role"]
        return jsonify({"success": True, "role": USERS[u]["role"]})
    return jsonify({"success": False, "error": "Invalid credentials"})

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"success": True})

@app.route("/me")
@login_required
def me():
    smm_bal = get_smm_balance()
    return jsonify({
        "username": session["username"],
        "role":     session["role"],
        "balance":  get_balance(),
        "smm_balance": smm_bal
    })

# ── ORDER ROUTES ──────────────────────────────────────────
@app.route("/start-session", methods=["POST"])
@login_required
def start_session():
    data     = request.json
    link     = data.get("link","").strip()
    svc      = str(data.get("service_id","1554")).strip()
    qty      = data.get("quantity", 0)
    total    = data.get("total_orders", 1)
    gap      = data.get("gap_minutes", 1)

    if "facebook.com" not in link:
        return jsonify({"error": "Valid Facebook link required"}), 400
    if svc not in SERVICES:
        return jsonify({"error": "Invalid service"}), 400
    if not (20 <= int(qty) <= 5000):
        return jsonify({"error": "Quantity must be 20-5000"}), 400
    if int(total) < 1:
        return jsonify({"error": "Total orders must be at least 1"}), 400
    if int(gap) < 1:
        return jsonify({"error": "Gap must be at least 1 minute"}), 400

    sid = str(uuid.uuid4())
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    with get_db() as db:
        db.execute("""INSERT INTO job_sessions
            (id, username, link, service_id, quantity, total_orders, gap_minutes,
             placed, success, failed, status, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,0,0,0,'running',?,?)""",
            (sid, session["username"], link, svc, int(qty),
             int(total), int(gap), now, now))
        db.commit()

    stop_ev = threading.Event()
    active_jobs[sid] = stop_ev
    threading.Thread(target=order_worker, args=(sid,), daemon=True).start()

    return jsonify({"success": True, "session_id": sid})

@app.route("/session-status/<sid>")
@login_required
def session_status(sid):
    with get_db() as db:
        job = db.execute("SELECT * FROM job_sessions WHERE id=?", (sid,)).fetchone()
        if not job:
            return jsonify({"error": "Not found"}), 404

        # Last 10 orders
        orders = db.execute("""SELECT order_num, status, smm_order_id, created_at
            FROM orders WHERE session_id=? ORDER BY order_num DESC LIMIT 10""",
            (sid,)).fetchall()

    return jsonify({
        "session_id":   job["id"],
        "status":       job["status"],
        "placed":       job["placed"],
        "success":      job["success"],
        "failed":       job["failed"],
        "total":        job["total_orders"],
        "orders":       [dict(o) for o in orders]
    })

@app.route("/stop-session/<sid>", methods=["POST"])
@login_required
def stop_session(sid):
    if sid in active_jobs:
        active_jobs[sid].set()
    return jsonify({"success": True})

@app.route("/my-sessions")
@login_required
def my_sessions():
    with get_db() as db:
        jobs = db.execute("""SELECT * FROM job_sessions
            WHERE username=? ORDER BY created_at DESC LIMIT 20""",
            (session["username"],)).fetchall()
    return jsonify([dict(j) for j in jobs])

# ── ADMIN ROUTES ──────────────────────────────────────────
@app.route("/admin/add-balance", methods=["POST"])
@login_required
@admin_required
def add_balance():
    data   = request.json
    txn_id = data.get("txn_id","").strip()
    amount = float(data.get("amount", 0))

    if not txn_id:
        return jsonify({"error": "Transaction ID required"}), 400
    if amount <= 0:
        return jsonify({"error": "Amount must be positive"}), 400

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db() as db:
            db.execute("""INSERT INTO balance_logs (txn_id, amount_usd, added_by, created_at)
                VALUES (?,?,?,?)""", (txn_id, amount, session["username"], now))
            db.commit()
        return jsonify({"success": True, "new_balance": get_balance()})
    except sqlite3.IntegrityError:
        return jsonify({"error": "Transaction ID already used!"}), 400

@app.route("/admin/balance-history")
@login_required
@admin_required
def balance_history():
    with get_db() as db:
        logs = db.execute("""SELECT * FROM balance_logs
            ORDER BY created_at DESC""").fetchall()
    return jsonify([dict(l) for l in logs])

@app.route("/admin/all-orders")
@login_required
@admin_required
def all_orders():
    date_from = request.args.get("from","")
    date_to   = request.args.get("to","")
    with get_db() as db:
        if date_from and date_to:
            rows = db.execute("""SELECT * FROM orders
                WHERE created_at >= ? AND created_at <= ?
                ORDER BY created_at DESC""",
                (date_from + " 00:00:00", date_to + " 23:59:59")).fetchall()
        else:
            rows = db.execute("""SELECT * FROM orders
                ORDER BY created_at DESC LIMIT 500""").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route("/admin/export-excel")
@login_required
@admin_required
def export_excel():
    import io
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        return jsonify({"error": "openpyxl not installed"}), 500

    date_from = request.args.get("from","")
    date_to   = request.args.get("to","")

    with get_db() as db:
        if date_from and date_to:
            orders = db.execute("""SELECT * FROM orders
                WHERE created_at >= ? AND created_at <= ?
                ORDER BY created_at ASC""",
                (date_from + " 00:00:00", date_to + " 23:59:59")).fetchall()
        else:
            orders = db.execute("SELECT * FROM orders ORDER BY created_at ASC").fetchall()

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Orders Report"

    # Header style
    hdr_fill = PatternFill("solid", fgColor="1a6ff5")
    hdr_font = Font(color="FFFFFF", bold=True)

    headers = ["#", "Date", "Username", "Service", "Link", "Quantity", "Status", "SMM Order ID"]
    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = Alignment(horizontal="center")

    # Data rows
    for ri, row in enumerate(orders, 2):
        svc_name = SERVICES.get(row["service_id"], row["service_id"])
        ws.cell(row=ri, column=1, value=ri-1)
        ws.cell(row=ri, column=2, value=row["created_at"])
        ws.cell(row=ri, column=3, value=row["username"])
        ws.cell(row=ri, column=4, value=f"#{row['service_id']} {svc_name}")
        ws.cell(row=ri, column=5, value=row["link"])
        ws.cell(row=ri, column=6, value=row["quantity"])
        ws.cell(row=ri, column=7, value=row["status"])
        ws.cell(row=ri, column=8, value=row["smm_order_id"])

    # Auto width
    for col in ws.columns:
        max_len = max(len(str(c.value or "")) for c in col)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    from flask import send_file
    fname = f"orders_{date_from or 'all'}_{date_to or 'all'}.xlsx"
    return send_file(buf, as_attachment=True,
                     download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.route("/admin/stats")
@login_required
@admin_required
def admin_stats():
    with get_db() as db:
        total_orders  = db.execute("SELECT COUNT(*) as c FROM orders").fetchone()["c"]
        success_orders = db.execute("SELECT COUNT(*) as c FROM orders WHERE status='success'").fetchone()["c"]
        total_views   = db.execute("SELECT COALESCE(SUM(quantity),0) as s FROM orders WHERE status='success'").fetchone()["s"]
        this_month    = db.execute("""SELECT COUNT(*) as c FROM orders
            WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')""").fetchone()["c"]
        month_views   = db.execute("""SELECT COALESCE(SUM(quantity),0) as s FROM orders
            WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now')
            AND status='success'""").fetchone()["s"]
        balance_added = db.execute("SELECT COALESCE(SUM(amount_usd),0) as s FROM balance_logs").fetchone()["s"]

    return jsonify({
        "total_orders":   total_orders,
        "success_orders": success_orders,
        "total_views":    int(total_views),
        "this_month_orders": this_month,
        "this_month_views":  int(month_views),
        "balance_added":  round(float(balance_added), 4),
        "current_balance": get_balance()
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
