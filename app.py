from flask import Flask, render_template, request, redirect, session, jsonify, url_for
import requests
import json
import time
import re
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import unquote, quote

# ---------------- APP ----------------
app = Flask(__name__)
# ⚠️ Production: ใช้ Environment Variable แทน (ห้ามใช้ค่าธรรมดาใน Production)
import os
app.secret_key = os.environ.get('SECRET_KEY', 'cpf_nurse_development_only')

# ⭐ ใส่ URL ของ Google Apps Script ที่ Deploy แล้ว
GAS_URL = "https://script.google.com/macros/s/AKfycbx8CTkhx73DptbxSyOWe9rOzfNrfvClTJhB_1-l_jX2gPjrxWROP9wByfmxXzYhu2wS2A/exec"

# ===== TIMEZONE PATCH (Asia/Bangkok) =====
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

TH_TZ = ZoneInfo("Asia/Bangkok") if ZoneInfo else None


def th_now():
    return datetime.now(TH_TZ) if TH_TZ else datetime.now()


def _unwrap_rows(payload):
    """
    รองรับหลายรูปแบบที่ GAS อาจส่งกลับ:
    - list
    - dict ที่มี key: data / items / rows / result
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for k in ("data", "items", "rows", "result"):
            if k in payload and isinstance(payload[k], list):
                return payload[k]
    return []


def _parse_any_datetime(value):
    """
    รองรับรูปแบบ:
    - YYYY-MM-DD
    - YYYY-MM-DD HH:MM[:SS]
    - YYYY-MM-DDTHH:MM[:SS]
    - ISO: 2026-02-09T17:00:00.000Z / +00:00
    """
    s = str(value or "").strip()
    if not s:
        return None

    # date only
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            d = datetime.strptime(s, "%Y-%m-%d")
            if TH_TZ:
                d = d.replace(tzinfo=TH_TZ)
            return d
        except:
            return None

    # normalize Z
    s2 = s.replace("Z", "+00:00")

    # fromisoformat รองรับ T/space ได้ (เมื่อเป็นรูปแบบถูกต้อง)
    try:
        dt = datetime.fromisoformat(s2)
        return dt
    except:
        pass

    # strptime fallback
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if TH_TZ:
                dt = dt.replace(tzinfo=TH_TZ)
            return dt
        except:
            continue

    return None


def normalize_visit_date_for_store(value):
    """
    เก็บเป็นฟอร์แมตมาตรฐานในชีต:
    YYYY-MM-DD HH:MM:SS (เวลาไทย)
    """
    dt = _parse_any_datetime(value)
    if dt is None:
        dt = th_now()

    # ถ้ามี tz -> แปลงเป็นไทย
    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone(TH_TZ) if TH_TZ else dt.astimezone()
        except:
            pass
    else:
        # ไม่มี tz ให้ถือว่าเป็นเวลาไทย
        if TH_TZ:
            dt = dt.replace(tzinfo=TH_TZ)

    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_visit_date_for_display(value, with_seconds=False):
    """
    แสดงผลให้ผู้ใช้เห็นเป็นเวลาไทย อ่านง่าย
    """
    dt = _parse_any_datetime(value)
    if dt is None:
        return str(value or "")

    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone(TH_TZ) if TH_TZ else dt.astimezone()
        except:
            pass
    else:
        if TH_TZ:
            dt = dt.replace(tzinfo=TH_TZ)

    return dt.strftime("%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M")


def visit_date_for_input(value):
    """
    แปลงเป็นค่าให้ <input type='datetime-local'> ใช้
    """
    dt = _parse_any_datetime(value)
    if dt is None:
        return ""
    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone(TH_TZ) if TH_TZ else dt.astimezone()
        except:
            pass
    else:
        if TH_TZ:
            dt = dt.replace(tzinfo=TH_TZ)
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


# ============================================
# GOOGLE SHEETS API HELPERS
# ============================================

# cache หลักของ list
_GAS_CACHE = {}

# cache ของ search(table, field, value)
_GAS_SEARCH_CACHE = {}

# cache เฉพาะรายชื่อยาแยกตามกลุ่ม (เพื่อหน้า /medicine/list/<group>)
_GROUP_MEDS_CACHE = {}


def gas_cache_invalidate(table=None):
    """ล้าง cache เพื่อให้ข้อมูลใหม่แสดงทันทีหลังมีการเขียนข้อมูล"""
    if table is None:
        _GAS_CACHE.clear()
        _GAS_SEARCH_CACHE.clear()
        _GROUP_MEDS_CACHE.clear()
        return

    # ลบเฉพาะ list cache ของ table นั้น ๆ
    for k in list(_GAS_CACHE.keys()):
        if k[0] == table:
            _GAS_CACHE.pop(k, None)

    # ลบเฉพาะ search cache ของ table นั้น ๆ
    for k in list(_GAS_SEARCH_CACHE.keys()):
        if k[0] == table:
            _GAS_SEARCH_CACHE.pop(k, None)

    # medicine/other_item เปลี่ยน -> รายการหน้า group/list อาจเปลี่ยน
    if table in ("medicine", "other_item"):
        _GROUP_MEDS_CACHE.clear()


def gas_list_raw(table, limit=1000):
    """(RAW) ดึงข้อมูลทั้งหมดจาก Sheet แบบไม่ cache"""
    try:
        r = requests.get(GAS_URL, params={
            "action": "list",
            "table": table,
            "limit": limit
        }, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"gas_list error: {e}")
        return {"ok": False, "data": [], "message": str(e)}


def gas_list_cached(table, limit=5000, ttl=20):
    """ดึงข้อมูลแบบมี cache TTL สั้น ๆ กันการดึงชีตซ้ำ"""
    key = (table, limit)
    now = time.time()

    if key in _GAS_CACHE:
        ts, res = _GAS_CACHE[key]
        if now - ts < ttl:
            return res

    res = gas_list_raw(table, limit)

    # cache เฉพาะผลลัพธ์ที่ ok
    if isinstance(res, dict) and res.get("ok"):
        _GAS_CACHE[key] = (now, res)

    return res


def gas_list(table, limit=1000):
    """(DEFAULT) ให้ทุกจุดในระบบที่เรียก gas_list ได้ cache อัตโนมัติ"""
    return gas_list_cached(table, limit=limit, ttl=20)


def norm_text(s):
    return " ".join(str(s or "").strip().split())


def norm_key(s: str) -> str:
    s = str(s or "").strip().lower()
    # unify dash
    for ch in ["–", "—", "−"]:
        s = s.replace(ch, "-")
    # remove ALL whitespace (แก้เคส "HTC" มี/ไม่มีช่องว่าง)
    s = re.sub(r"\s+", "", s)
    return s


def gas_get(table, row_id):
    """ดึงข้อมูลตาม ID"""
    try:
        r = requests.get(GAS_URL, params={
            "action": "get",
            "table": table,
            "id": str(row_id)
        }, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"gas_get error: {e}")
        return {"ok": False, "data": None, "message": str(e)}


def gas_search(table, field, value):
    """ค้นหาข้อมูลตามฟิลด์ (RAW no-cache)"""
    try:
        r = requests.get(GAS_URL, params={
            "action": "search",
            "table": table,
            "field": field,
            "value": value
        }, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"gas_search error: {e}")
        return {"ok": False, "data": [], "message": str(e)}


def gas_search_cached(table, field, value, ttl=15):
    """
    cache search ระยะสั้น เพื่อลดรอบเรียก GAS ซ้ำ ๆ โดยเฉพาะหน้า list
    """
    key = (table, str(field).strip(), str(value).strip())
    now = time.time()

    if key in _GAS_SEARCH_CACHE:
        ts, res = _GAS_SEARCH_CACHE[key]
        if now - ts < ttl:
            return res

    res = gas_search(table, field, value)
    if isinstance(res, dict) and res.get("ok"):
        _GAS_SEARCH_CACHE[key] = (now, res)
    return res


def gas_append(table, payload):
    """เพิ่มข้อมูลใหม่"""
    try:
        r = requests.post(GAS_URL, json={
            "action": "append",
            "table": table,
            "payload": payload
        }, timeout=30)
        r.raise_for_status()
        res = r.json()

        # ✅ เขียนสำเร็จ -> ล้าง cache ของ table นี้
        if isinstance(res, dict) and res.get("ok"):
            gas_cache_invalidate(table)

        return res
    except Exception as e:
        print(f"gas_append error: {e}")
        return {"ok": False, "message": str(e)}


def gas_update(table, row_id, payload):
    """แก้ไขข้อมูลตาม ID"""
    try:
        r = requests.post(GAS_URL, json={
            "action": "update",
            "table": table,
            "id": str(row_id),
            "payload": payload
        }, timeout=30)
        r.raise_for_status()
        res = r.json()

        # ✅ อัปเดตสำเร็จ -> ล้าง cache ของ table นี้
        if isinstance(res, dict) and res.get("ok"):
            gas_cache_invalidate(table)

        return res
    except Exception as e:
        print(f"gas_update error: {e}")
        return {"ok": False, "message": str(e)}


def gas_update_field(table, row_id, field, value):
    """อัปเดตฟิลด์เดียว"""
    try:
        r = requests.post(GAS_URL, json={
            "action": "update_field",
            "table": table,
            "id": str(row_id),
            "field": field,
            "value": value
        }, timeout=30)
        r.raise_for_status()
        res = r.json()

        # ✅ อัปเดตสำเร็จ -> ล้าง cache ของ table นี้
        if isinstance(res, dict) and res.get("ok"):
            gas_cache_invalidate(table)

        return res
    except Exception as e:
        print(f"gas_update_field error: {e}")
        return {"ok": False, "message": str(e)}


def gas_delete(table, row_id):
    """ลบข้อมูลตาม ID"""
    try:
        r = requests.post(GAS_URL, json={
            "action": "delete",
            "table": table,
            "id": str(row_id)
        }, timeout=30)
        r.raise_for_status()
        res = r.json()

        # ✅ ลบสำเร็จ -> ล้าง cache ของ table นี้
        if isinstance(res, dict) and res.get("ok"):
            gas_cache_invalidate(table)

        return res
    except Exception as e:
        print(f"gas_delete error: {e}")
        return {"ok": False, "message": str(e)}


def _to_int(v, default=0):
    try:
        return int(float(str(v).replace(",", "").strip()))
    except:
        return default


def _to_float(v, default=0.0):
    try:
        return float(str(v).replace(",", "").strip())
    except:
        return default


def _wants_json_response():
    accept = (request.headers.get("Accept") or "").lower()
    xrw = (request.headers.get("X-Requested-With") or "").lower()
    return request.is_json or ("application/json" in accept) or (xrw == "xmlhttprequest")


def _get_lots_by_field_fast(table, field, value, limit=5000):
    """
    เร็วกว่า list ทั้งตาราง:
    - พยายามใช้ gas_search ก่อน (แบบ cache ระยะสั้น)
    - ถ้าไม่ได้ ค่อย fallback ไป list + filter
    """
    sr = gas_search_cached(table, field, value, ttl=10)
    if isinstance(sr, dict) and sr.get("ok"):
        rows = _unwrap_rows(sr)
        if isinstance(rows, list):
            return rows

    lr = gas_list(table, limit)
    rows = _unwrap_rows(lr)
    out = []
    for r in rows:
        if str(r.get(field, "")).strip() == str(value).strip():
            out.append(r)
    return out


def get_medicines_by_group_fast(group, ttl=20):
    """
    helper สำหรับหน้า /medicine/list/<group>
    - ใช้ group cache
    - ใช้ gas_search(group_name) ก่อน
    - fallback gas_list + filter
    """
    g = str(group or "").strip()
    if not g:
        return []

    key = norm_key(g)
    now = time.time()

    if key in _GROUP_MEDS_CACHE:
        ts, meds = _GROUP_MEDS_CACHE[key]
        if now - ts < ttl:
            return meds

    meds = []

    # พยายาม search ก่อน
    sr = gas_search_cached("medicine", "group_name", g, ttl=ttl)
    if isinstance(sr, dict) and sr.get("ok"):
        for m in _unwrap_rows(sr):
            m_type = str(m.get("type", "")).strip().lower()
            m_group = str(m.get("group_name", "")).strip()
            if m_type == "medicine" and m_group == g:
                meds.append(m)

    # fallback เผื่อ GAS action=search ไม่ได้ผลบางช่วง
    if not meds:
        res = gas_list("medicine", 5000)
        if res.get("ok"):
            for m in res.get("data", []):
                m_type = str(m.get("type", "")).strip().lower()
                m_group = str(m.get("group_name", "")).strip()
                if m_type == "medicine" and m_group == g:
                    meds.append(m)

    meds.sort(key=lambda x: str(x.get("name", "")).strip().lower())
    _GROUP_MEDS_CACHE[key] = (now, meds)
    return meds


# ============================================
# AUTH DECORATORS
# ============================================

def login_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if "username" not in session:
            return redirect("/")
        return f(*args, **kwargs)
    return wrap


def admin_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if session.get("role") != "admin":
            return redirect("/menu")
        return f(*args, **kwargs)
    return wrap


def catalog_required(f):
    """
    อนุญาตเฉพาะผู้ที่ล็อกอินและมี role = admin หรือ user
    (ตาม requirement ใหม่: user ทั่วไปก็เพิ่ม/ลบได้)
    """
    @wraps(f)
    def wrap(*args, **kwargs):
        if "username" not in session:
            return redirect("/")
        role = str(session.get("role", "")).strip().lower()
        if role not in ("admin", "user"):
            return redirect("/menu")
        return f(*args, **kwargs)
    return wrap


# ============================================
# TEST ROUTES
# ============================================

@app.get("/test-gas")
def test_gas():
    """ทดสอบการเชื่อมต่อ GAS"""
    res = gas_list("users", 5)
    return jsonify(res)


@app.get("/fix-admin")
def fix_admin():
    """สร้าง Admin สำรองกรณีเข้าไม่ได้"""
    all_users = gas_list("users", 1000)
    found = False
    if all_users.get("ok"):
        for u in all_users.get("data", []):
            if str(u.get("username", "")).lower() == "admin":
                found = True
                break

    if found:
        return "<h1>Admin user already exists!</h1> <p>User: admin / Pass: 111</p> <a href='/'>Go to Login</a>"

    payload = {
        "username": "admin",
        "password": "111",
        "name": "Admin Recovery",
        "dept": "IT",
        "role": "admin"
    }
    res = gas_append("users", payload)
    return f"<h1>Created Admin!</h1> <pre>{res}</pre> <a href='/'>Go to Login</a>"


# ============================================
# LOGIN / LOGOUT
# ============================================

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        res = gas_list("users", 1000)
        found_user = None

        if res.get("ok"):
            for user in res.get("data", []):
                if str(user.get("username", "")).strip().lower() == username.lower():
                    found_user = user
                    break

        if found_user:
            if str(found_user.get("password", "")).strip() == password:
                session["username"] = found_user["username"]
                session["role"] = found_user.get("role", "user")
                session["user_name"] = found_user.get("name", "")
                return redirect("/menu")

        return render_template("login.html", error="ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง")

    return render_template("login.html")


# ============================================
# MENU
# ============================================

@app.route("/menu")
@login_required
def menu():
    return render_template("menu.html", role=session.get("role"))


# ============================================
# USER MANAGEMENT (ADMIN)
# ============================================

@app.route("/users", methods=["GET", "POST"])
@admin_required
def users():
    if request.method == "POST":
        payload = {
            "username": request.form.get("username", "").strip(),
            "password": request.form.get("password", "").strip(),
            "name": request.form.get("name", "").strip(),
            "dept": request.form.get("dept", "").strip(),
            "role": request.form.get("role", "user").strip()
        }
        gas_append("users", payload)

    res = gas_list("users", 1000)
    users_list = res.get("data", []) if res.get("ok") else []
    return render_template("users.html", users=users_list)


@app.route("/users/delete/<int:id>")
@admin_required
def delete_user(id):
    res = gas_get("users", id)
    if res.get("ok") and res.get("data"):
        if res["data"].get("role") != "admin":
            gas_delete("users", id)
    return redirect("/users")


# ============================================
# MEDICINE TYPE / GROUP
# ============================================

SYMPTOM_GROUPS = [
    "ระบบทางเดินหายใจ", "ระบบย่อยอาหาร", "กล้ามเนื้อ", "ระบบสมอง",
    "ผิวหนัง", "อายุรกรรม", "ระบบขับถ่าย", "ระบบสืบพันธุ์",
    "ตา หู ช่องปาก", "คอ", "จมูก", "ทำแผล",
    "อุบัติเหตุในงาน", "อุบัติเหตุนอกงาน", "อื่นๆ"
]


@app.route("/medicine_type")
def medicine_type():
    return render_template("medicine_type.html")


@app.route("/supply")
def supply_list():
    res = gas_list("medicine", 5000)
    supplies = []
    if res.get("ok"):
        for m in res.get("data", []):
            m_type = str(m.get("type", "")).strip().lower()
            if m_type == "supply":
                supplies.append(m)
    return render_template("supply_list.html", supplies=supplies)


@app.route("/medicine/group")
def medicine_group():
    return render_template("medicine_group.html", groups=SYMPTOM_GROUPS)


@app.route("/supply/add", methods=["POST"])
@catalog_required
def supply_add():
    name = norm_text(request.form.get("name", ""))

    if not name:
        return "กรอกชื่อเวชภัณฑ์", 400

    res = gas_list("medicine", 5000)
    if res.get("ok"):
        for m in res.get("data", []):
            m_type = str(m.get("type", "")).strip().lower()
            m_name = norm_text(m.get("name", ""))
            if m_type == "supply" and m_name.lower() == name.lower():
                return redirect("/supply")

    payload = {
        "type": "supply",
        "group_name": "เวชภัณฑ์",
        "name": name,
        "benefit": "",
        "min_qty": 0,
        "qty": 0,
        "expire_date": "",
        "used": 0
    }

    r = gas_append("medicine", payload)
    if not r.get("ok"):
        return f"เพิ่มไม่สำเร็จ: {r}", 500

    return redirect("/supply")


# ============================================
# MEDICINE LIST
# ============================================

@app.route("/medicine/list/<path:group>")
def medicine_list(group):
    group = unquote(group).strip()

    if group == "อื่นๆ":
        res = gas_list("other_item", 5000)
        items = res.get("data", []) if res.get("ok") else []
        items.sort(key=lambda x: str(x.get("name", "")).strip().lower())
        return render_template("medicine_other.html", group=group, items=items)

    meds = get_medicines_by_group_fast(group, ttl=25)
    return render_template("medicine_list.html", medicines=meds, meds=meds, group=group)


@app.get("/api/prefetch/medicine_group/<path:group>")
@login_required
def api_prefetch_medicine_group(group):
    """
    endpoint สำหรับ warm cache ตอน hover/touch link ในหน้า medicine_group
    """
    group = unquote(group).strip()

    if group == "อื่นๆ":
        res = gas_list("other_item", 5000)
        rows = _unwrap_rows(res)
        return jsonify({"ok": True, "group": group, "count": len(rows)})

    meds = get_medicines_by_group_fast(group, ttl=30)
    return jsonify({"ok": True, "group": group, "count": len(meds)})


@app.route("/other/add_item", methods=["POST"])
@login_required
def other_add_item():
    item_name = norm_text(request.form.get("item_name"))

    if not item_name:
        return "กรอกชื่อรายการ", 400

    res = gas_list("other_item", 5000)
    if not res.get("ok"):
        return f"อ่านชีต other_item ไม่สำเร็จ: {res}", 500

    rows = res.get("data", [])

    for r in rows:
        nm = norm_text(r.get("name") or r.get("item_name") or r.get("ชื่อรายการ"))
        if nm.lower() == item_name.lower():
            return redirect("/medicine/list/" + quote("อื่นๆ"))

    payload = {
        "type": "other",
        "group_name": "อื่นๆ",
        "name": item_name,
        "benefit": "",
        "min_qty": 0,
        "qty": 0,
        "expire_date": "",
        "used": 0,
        "created_at": th_now().strftime("%Y-%m-%d %H:%M:%S")
    }

    add_res = gas_append("other_item", payload)
    if not add_res.get("ok"):
        return f"เพิ่มรายการอื่นๆ ไม่สำเร็จ: {add_res}", 500

    return redirect("/medicine/list/" + quote("อื่นๆ"))


@app.get("/debug/other_item")
@login_required
def debug_other_item():
    return jsonify(gas_list("other_item", 20))


@app.route("/other/item/<int:item_id>/delete", methods=["POST"])
@catalog_required
def other_delete_item(item_id):
    item_res = gas_get("other_item", item_id)
    if not item_res.get("ok") or not item_res.get("data"):
        return redirect("/medicine/list/" + quote("อื่นๆ"))

    item_name = str(item_res["data"].get("name", "")).strip()

    lots_res = _get_lots_by_field_fast("other_lot", "item_name", item_name, limit=5000)
    for l in lots_res:
        if str(l.get("item_name", "")).strip().lower() == item_name.lower():
            gas_delete("other_lot", l.get("id"))

    gas_delete("other_item", item_id)
    return redirect("/medicine/list/" + quote("อื่นๆ"))


@app.route("/medicine/<int:med_id>/delete", methods=["POST"])
@catalog_required
def medicine_delete(med_id):
    med_res = gas_get("medicine", med_id)
    group_name = ""
    mtype = ""

    if med_res.get("ok") and med_res.get("data"):
        group_name = str(med_res["data"].get("group_name", "")).strip()
        mtype = str(med_res["data"].get("type", "")).strip().lower()

    lots = _get_lots_by_field_fast("medicine_lot", "medicine_id", str(med_id), limit=10000)
    for l in lots:
        gas_delete("medicine_lot", l.get("id"))

    gas_delete("medicine", med_id)

    if mtype == "supply":
        return redirect("/supply")

    if group_name:
        return redirect("/medicine/list/" + quote(group_name))
    return redirect("/medicine/group")


@app.route("/other/<path:item_name>")
@login_required
def other_item_detail(item_name):
    item_name = norm_text(unquote(item_name))

    check = gas_search_cached("other_item", "name", item_name, ttl=20)
    exists = False
    if isinstance(check, dict) and check.get("ok"):
        rows = _unwrap_rows(check)
        exists = any(norm_text(r.get("name", "")).lower() == item_name.lower() for r in rows)

    if not exists:
        all_items = gas_list("other_item", 5000)
        if all_items.get("ok"):
            exists = any(norm_text(r.get("name", "")).lower() == item_name.lower()
                         for r in all_items.get("data", []))

    if not exists:
        return redirect(url_for("medicine_list", group="อื่นๆ"))

    lots = _get_lots_by_field_fast("other_lot", "item_name", item_name, limit=10000)
    lots = [l for l in lots if norm_text(l.get("item_name", "")).lower() == item_name.lower()]
    lots.sort(key=lambda x: str(x.get("expire_date", "")))

    back_url = url_for("medicine_list", group="อื่นๆ")
    return render_template("other_item_lot.html",
                           group="อื่นๆ",
                           item_name=item_name,
                           lots=lots,
                           back_url=back_url)


@app.route("/other/<path:item_name>/add_lot", methods=["POST"])
@catalog_required
def other_add_lot(item_name):
    item_name = unquote(item_name).strip()

    src = request.get_json(silent=True) or request.form
    expire_date = (src.get("expire_date") or "").strip()
    qty = _to_int(src.get("qty"), 0)
    price = _to_float(src.get("price"), 0.0)

    if not expire_date or qty <= 0 or price <= 0:
        if _wants_json_response():
            return jsonify({"success": False, "message": "ข้อมูลไม่ครบหรือไม่ถูกต้อง"}), 400
        return "ข้อมูลไม่ครบหรือไม่ถูกต้อง", 400

    rows = _get_lots_by_field_fast("other_lot", "item_name", item_name)

    existing = None
    for lot in rows:
        if str(lot.get("expire_date", "")).strip() == expire_date:
            existing = lot
            break

    if existing:
        new_qty_total = _to_int(existing.get("qty_total"), 0) + qty
        new_qty_remain = _to_int(existing.get("qty_remain"), 0) + qty
        new_price_per_lot = _to_float(existing.get("price_per_lot"), 0.0) + price
        new_price_per_unit = (new_price_per_lot / new_qty_total) if new_qty_total > 0 else 0

        upd = gas_update("other_lot", existing["id"], {
            "qty_total": new_qty_total,
            "qty_remain": new_qty_remain,
            "price_per_lot": new_price_per_lot,
            "price_per_unit": round(new_price_per_unit, 4)
        })
        if not upd.get("ok"):
            if _wants_json_response():
                return jsonify({"success": False, "message": upd.get("message", "update failed")}), 500
            return "บันทึกไม่สำเร็จ", 500

        lot_obj = {
            "id": existing.get("id"),
            "lot_name": existing.get("lot_name"),
            "expire_date": expire_date,
            "qty_total": new_qty_total,
            "qty_remain": new_qty_remain,
            "price_per_lot": round(new_price_per_lot, 2),
            "price_per_unit": round(new_price_per_unit, 4),
        }
    else:
        lot_count = len(rows)
        lot_name = f"LOT {lot_count + 1}"
        price_per_unit = price / qty if qty > 0 else 0

        ap = gas_append("other_lot", {
            "item_name": item_name,
            "lot_name": lot_name,
            "expire_date": expire_date,
            "qty_total": qty,
            "qty_remain": qty,
            "price_per_lot": price,
            "price_per_unit": round(price_per_unit, 4),
            "created_at": th_now().strftime("%Y-%m-%d %H:%M:%S")
        })

        if not ap.get("ok"):
            if _wants_json_response():
                return jsonify({"success": False, "message": ap.get("message", "append failed")}), 500
            return "บันทึกไม่สำเร็จ", 500

        lot_id = ap.get("id")
        lot_obj = {
            "id": lot_id,
            "lot_name": lot_name,
            "expire_date": expire_date,
            "qty_total": qty,
            "qty_remain": qty,
            "price_per_lot": round(price, 2),
            "price_per_unit": round(price_per_unit, 4),
        }

    if _wants_json_response():
        return jsonify({"success": True, "lot": lot_obj})

    return redirect("/other/" + quote(item_name))


@app.route("/other_lot/<int:lot_id>/delete", methods=["POST"])
@catalog_required
def other_delete_lot(lot_id):
    lot_res = gas_get("other_lot", lot_id)
    if lot_res.get("ok") and lot_res.get("data"):
        item_name = str(lot_res["data"].get("item_name", "")).strip()
        gas_delete("other_lot", lot_id)
        return redirect("/other/" + quote(item_name))

    return redirect("/medicine/list/" + quote("อื่นๆ"))


# ============================================
# MEDICINE DETAIL & LOT
# ============================================

@app.route("/medicine/<int:med_id>")
def medicine_detail(med_id):
    med_res = gas_get("medicine", med_id)
    if not med_res.get("ok") or not med_res.get("data"):
        return "ไม่พบข้อมูล", 404

    med = med_res["data"]

    lots = _get_lots_by_field_fast("medicine_lot", "medicine_id", str(med_id), limit=10000)
    lots = [l for l in lots if str(l.get("medicine_id", "")) == str(med_id)]
    lots.sort(key=lambda x: x.get("expire_date", ""))

    mtype = str(med.get("type", "")).strip().lower()
    if mtype == "supply":
        back_url = url_for("supply_list")
    else:
        group_name = str(med.get("group_name", "")).strip()
        back_url = url_for("medicine_list", group=group_name) if group_name else url_for("medicine_group")

    return render_template("medicine_lot.html", med=med, lots=lots, back_url=back_url)


@app.route("/medicine/<int:med_id>/add_lot", methods=["POST"])
@catalog_required
def add_lot(med_id):
    src = request.get_json(silent=True) or request.form

    expire_date = (src.get("expire_date") or "").strip()
    qty = _to_int(src.get("qty"), 0)
    price = _to_float(src.get("price"), 0.0)
    item_name = norm_text(src.get("item_name", ""))

    if not expire_date or qty <= 0 or price <= 0:
        if _wants_json_response():
            return jsonify({"success": False, "message": "ข้อมูลไม่ครบหรือไม่ถูกต้อง"}), 400
        return "ข้อมูลไม่ครบหรือไม่ถูกต้อง", 400

    if not item_name:
        med_res = gas_get("medicine", med_id)
        if med_res.get("ok") and med_res.get("data"):
            item_name = norm_text(med_res["data"].get("name", ""))

    rows = _get_lots_by_field_fast("medicine_lot", "medicine_id", str(med_id))

    existing = None
    for lot in rows:
        if str(lot.get("expire_date", "")).strip() == expire_date:
            existing = lot
            break

    if existing:
        new_qty_total = _to_int(existing.get("qty_total"), 0) + qty
        new_qty_remain = _to_int(existing.get("qty_remain"), 0) + qty
        new_price_per_lot = _to_float(existing.get("price_per_lot"), 0.0) + price
        new_price_per_unit = (new_price_per_lot / new_qty_total) if new_qty_total > 0 else 0

        upd = gas_update("medicine_lot", existing["id"], {
            "qty_total": new_qty_total,
            "qty_remain": new_qty_remain,
            "price_per_lot": new_price_per_lot,
            "price_per_unit": round(new_price_per_unit, 4),
            "item_name": item_name
        })
        if not upd.get("ok"):
            if _wants_json_response():
                return jsonify({"success": False, "message": upd.get("message", "update failed")}), 500
            return "บันทึกไม่สำเร็จ", 500

        lot_obj = {
            "id": existing.get("id"),
            "lot_name": existing.get("lot_name"),
            "expire_date": expire_date,
            "qty_total": new_qty_total,
            "qty_remain": new_qty_remain,
            "price_per_lot": round(new_price_per_lot, 2),
            "price_per_unit": round(new_price_per_unit, 4),
        }
    else:
        lot_count = len(rows)
        lot_name = f"LOT {lot_count + 1}"
        price_per_unit = price / qty if qty > 0 else 0

        ap = gas_append("medicine_lot", {
            "medicine_id": med_id,
            "item_name": item_name,
            "lot_name": lot_name,
            "expire_date": expire_date,
            "qty_total": qty,
            "qty_remain": qty,
            "price_per_lot": price,
            "price_per_unit": round(price_per_unit, 4)
        })

        if not ap.get("ok"):
            if _wants_json_response():
                return jsonify({"success": False, "message": ap.get("message", "append failed")}), 500
            return "บันทึกไม่สำเร็จ", 500

        lot_id = ap.get("id")
        lot_obj = {
            "id": lot_id,
            "lot_name": lot_name,
            "expire_date": expire_date,
            "qty_total": qty,
            "qty_remain": qty,
            "price_per_lot": round(price, 2),
            "price_per_unit": round(price_per_unit, 4),
        }

    if _wants_json_response():
        return jsonify({"success": True, "lot": lot_obj})

    return redirect(f"/medicine/{med_id}")


@app.route("/lot/<int:lot_id>/delete", methods=["POST"])
def delete_lot(lot_id):
    lot_res = gas_get("medicine_lot", lot_id)
    if not lot_res.get("ok") or not lot_res.get("data"):
        return "ไม่พบ Lot", 404

    med_id = lot_res["data"].get("medicine_id")
    gas_delete("medicine_lot", lot_id)

    return redirect(f"/medicine/{med_id}")


# ============================================
# RECORD (เพิ่มยา/เวชภัณฑ์)
# ============================================

@app.route("/record", methods=["GET", "POST"])
def record():
    if request.method == "POST":
        type_map = {"ยา": "medicine", "เวชภัณฑ์": "supply"}

        payload = {
            "type": type_map.get(request.form.get("type", ""), "medicine"),
            "group_name": request.form.get("group", "").strip(),
            "name": request.form.get("name", "").strip(),
            "benefit": request.form.get("benefit", "").strip(),
            "min_qty": int(request.form.get("min_qty", 0)),
            "qty": int(request.form.get("qty", 0)),
            "expire_date": request.form.get("expire_date", "").strip(),
            "used": int(request.form.get("used", 0))
        }

        res = gas_append("medicine", payload)
        if res.get("ok"):
            med_id = res.get("id")
            return redirect(f"/medicine/{med_id}")

        return "บันทึกไม่สำเร็จ", 500

    return render_template("record.html")


@app.route("/medicine/add", methods=["POST"])
@catalog_required
def medicine_add():
    group = norm_text(request.form.get("group", ""))
    name = norm_text(request.form.get("name", ""))
    mtype = norm_text(request.form.get("type", "medicine")).lower()

    if not group or not name:
        return "กรอกข้อมูลไม่ครบ", 400

    # ใช้ search+cache ก่อนเพื่อลด list ใหญ่
    maybe_dup = False
    sr = gas_search_cached("medicine", "group_name", group, ttl=20)
    if isinstance(sr, dict) and sr.get("ok"):
        for m in _unwrap_rows(sr):
            if norm_text(m.get("group_name", "")) == group and norm_text(m.get("name", "")).lower() == name.lower():
                maybe_dup = True
                break

    if maybe_dup:
        return redirect("/medicine/list/" + quote(group))

    # fallback safety
    if not maybe_dup:
        res = gas_list("medicine", 5000)
        if res.get("ok"):
            for m in res.get("data", []):
                if norm_text(m.get("group_name", "")) == group and norm_text(m.get("name", "")).lower() == name.lower():
                    return redirect("/medicine/list/" + quote(group))

    payload = {
        "type": mtype,
        "group_name": group,
        "name": name,
        "benefit": "",
        "min_qty": 0,
        "qty": 0,
        "expire_date": "",
        "used": 0
    }

    r = gas_append("medicine", payload)

    if not r.get("ok"):
        return f"เพิ่มไม่สำเร็จ: {r}", 500

    return redirect("/medicine/list/" + quote(group))


# ============================================
# TREATMENT
# ============================================

@app.route("/treatment_menu")
def treatment_menu():
    return render_template("treatment_menu.html")


@app.route("/treatment/register")
@login_required
def treatment_register():
    res = gas_list("treatment", 300)
    rows = res.get("data", []) if res.get("ok") else []
    return render_template("treatment_register.html", rows=rows)


@app.route("/treatment/form", methods=["GET", "POST"])
@login_required
def treatment_form():
    if request.method == "POST":
        try:
            medicine_json = request.form.get("medicine_json") or "[]"

            try:
                items = json.loads(medicine_json)
            except:
                items = []

            if not isinstance(items, list) or len(items) == 0:
                return "กรุณาเพิ่มยาอย่างน้อย 1 รายการ", 400

            form_group = (request.form.get("symptom_group") or request.form.get("group") or "").strip()

            # ตรวจ stock และตัด stock
            for it in items:
                lot_id = it.get("lot_id")
                qty = int(it.get("qty") or 0)

                if not lot_id or qty <= 0:
                    return "ข้อมูล Lot/จำนวนไม่ถูกต้อง", 400

                item_type = str(it.get("type") or it.get("item_type") or "").strip().lower()

                if not item_type and form_group in ("other", "อื่นๆ"):
                    item_type = "other"

                lot_table = "other_lot" if item_type in ("other", "other_item", "อื่นๆ") else "medicine_lot"

                lot_res = gas_get(lot_table, lot_id)
                if not lot_res.get("ok") or not lot_res.get("data"):
                    return "ไม่พบ Lot", 404

                lot = lot_res["data"]
                current_remain = int(lot.get("qty_remain", 0))

                if current_remain < qty:
                    return f"จำนวนคงเหลือไม่พอ (Lot {lot_id})", 400

                new_remain = current_remain - qty
                gas_update_field(lot_table, lot_id, "qty_remain", new_remain)

            # ===== PATCH: normalize visit_date เป็นเวลาไทยปัจจุบัน/ค่าที่กรอก =====
            visit_date_input = (request.form.get("visit_date") or request.form.get("date") or "").strip()
            visit_date = normalize_visit_date_for_store(visit_date_input)

            department = (request.form.get("department") or request.form.get("dept") or "").strip()
            symptom_group = (request.form.get("symptom_group") or request.form.get("group") or "").strip()
            symptom_detail = (request.form.get("symptom_detail") or request.form.get("detail") or "").strip()

            payload = {
                "visit_date": visit_date,
                "patient_name": request.form.get("patient_name", "").strip(),
                "department": department,
                "symptom_group": symptom_group,
                "symptom_detail": symptom_detail,
                "medicine": medicine_json,
                "allergy": request.form.get("allergy", "0"),
                "allergy_detail": request.form.get("allergy_detail", "").strip(),
                "occupational_disease": request.form.get("occupational_disease", "0"),
                "doctor_opinion": request.form.get("doctor_opinion", "").strip()
            }

            gas_append("treatment", payload)
            return redirect("/treatment/register")

        except Exception as e:
            return f"บันทึกไม่สำเร็จ: {e}", 500

    return render_template("treatment_form.html")


# ============================================
# TREATMENT API
# ============================================

@app.route("/api/treatment/<int:id>")
@login_required
def api_treatment_view(id):
    res = gas_get("treatment", id)
    if res.get("ok") and res.get("data"):
        d = dict(res["data"])
        raw = d.get("visit_date", "")
        d["visit_date_raw"] = raw
        d["visit_date_display"] = format_visit_date_for_display(raw, with_seconds=True)
        d["visit_date_input"] = visit_date_for_input(raw)
        # ให้ key เดิมยังใช้งานได้
        d["visit_date"] = d["visit_date_display"]
        return {"success": True, "data": d}
    return {"success": False}


@app.route("/api/treatment/edit/<int:id>", methods=["POST"])
@login_required
def api_treatment_edit(id):
    data = request.json or {}

    # ดึงข้อมูลเดิม (ใช้ทั้งคืน stock เดิม + fallback visit_date)
    old_res = gas_get("treatment", id)
    old_meds = []
    old_row = old_res.get("data") if old_res.get("ok") else None
    if old_row:
        try:
            old_meds = json.loads(old_row.get("medicine", "[]"))
        except:
            old_meds = []

    # คืน stock Lot เดิม
    for m in old_meds:
        lot_id = m.get("lot_id")
        if not lot_id:
            continue

        item_type = str(m.get("type") or m.get("item_type") or "").strip().lower()
        lot_table = "other_lot" if item_type in ("other", "other_item", "อื่นๆ") else "medicine_lot"

        lot_res = gas_get(lot_table, lot_id)
        if lot_res.get("ok") and lot_res.get("data"):
            current = int(lot_res["data"].get("qty_remain", 0))
            new_remain = current + int(m.get("qty", 0) or 0)
            gas_update_field(lot_table, lot_id, "qty_remain", new_remain)

    # ตัด stock ตามรายการใหม่
    try:
        new_meds = json.loads(data.get("medicine", "[]"))
    except:
        new_meds = []

    for m in new_meds:
        lot_id = m.get("lot_id")
        qty = int(m.get("qty", 0) or 0)
        if not lot_id or qty <= 0:
            continue

        item_type = str(m.get("type") or m.get("item_type") or "").strip().lower()
        lot_table = "other_lot" if item_type in ("other", "other_item", "อื่นๆ") else "medicine_lot"

        lot_res = gas_get(lot_table, lot_id)
        if lot_res.get("ok") and lot_res.get("data"):
            current = int(lot_res["data"].get("qty_remain", 0))
            if current < qty:
                return {"success": False, "message": "จำนวนคงเหลือไม่พอ"}
            new_remain = current - qty
            gas_update_field(lot_table, lot_id, "qty_remain", new_remain)

    # ===== PATCH: normalize visit_date ตอนแก้ไข =====
    incoming_visit = (data.get("visit_date") or "").strip() if isinstance(data.get("visit_date"), str) else data.get("visit_date")
    if incoming_visit:
        data["visit_date"] = normalize_visit_date_for_store(incoming_visit)
    else:
        if old_row and old_row.get("visit_date"):
            data["visit_date"] = normalize_visit_date_for_store(old_row.get("visit_date"))
        else:
            data["visit_date"] = normalize_visit_date_for_store("")

    gas_update("treatment", id, data)
    return {"success": True}


@app.route("/api/treatment/delete/<int:id>", methods=["DELETE"])
@login_required
def api_treatment_delete(id):
    old_res = gas_get("treatment", id)
    old_items = []
    if old_res.get("ok") and old_res.get("data"):
        try:
            old_items = json.loads(old_res["data"].get("medicine", "[]"))
        except:
            old_items = []

    for it in old_items:
        lot_id = it.get("lot_id")
        qty = int(it.get("qty") or 0)
        if not lot_id or qty <= 0:
            continue

        item_type = str(it.get("type") or it.get("item_type") or "").strip().lower()
        lot_table = "other_lot" if item_type in ("other", "other_item", "อื่นๆ") else "medicine_lot"

        lot_res = gas_get(lot_table, lot_id)
        if lot_res.get("ok") and lot_res.get("data"):
            current = int(lot_res["data"].get("qty_remain", 0))
            new_remain = current + qty
            gas_update_field(lot_table, lot_id, "qty_remain", new_remain)

    gas_delete("treatment", id)
    return {"success": True}


@app.route("/api/treatment_list")
@login_required
def treatment_list():
    res = gas_list("treatment", 1000)
    rows = res.get("data", []) if res.get("ok") else []

    data = []
    for r in rows:
        raw_visit = r.get("visit_date")
        display_visit = format_visit_date_for_display(raw_visit, with_seconds=False)

        data.append({
            "id": r.get("id"),
            "visit_date_raw": raw_visit,
            "visit_date_display": display_visit,
            "visit_date": display_visit,   # ใช้ key เดิมในหน้า register
            "patient_name": r.get("patient_name"),
            "symptom_group": r.get("symptom_group"),
            "medicine": r.get("medicine")
        })

    # เรียงใหม่ -> เก่า (ตาม datetime ที่ parse ได้)
    def _sort_key(x):
        dt = _parse_any_datetime(x.get("visit_date_raw"))
        if dt is None:
            return float("-inf")
        if dt.tzinfo is None and TH_TZ:
            dt = dt.replace(tzinfo=TH_TZ)
        try:
            return dt.timestamp()
        except:
            return float("-inf")

    data.sort(key=_sort_key, reverse=True)
    return jsonify(data)


# ============================================
# MEDICINE API
# ============================================

@app.get("/api/other_items")
@login_required
def api_other_items():
    res = gas_list("other_item", 5000)
    items = []
    if res.get("ok"):
        for r in res.get("data", []):
            name = norm_text(r.get("name", ""))
            if name:
                items.append({"id": r.get("id"), "name": name})
    items.sort(key=lambda x: x["name"].lower())
    return jsonify(items)


@app.get("/api/other_lots")
@login_required
def api_other_lots():
    item_name = norm_text(request.args.get("item_name", ""))
    if not item_name:
        return jsonify({"lots": []})

    rows = _get_lots_by_field_fast("other_lot", "item_name", item_name, limit=10000)

    lots = []
    for r in rows:
        if norm_text(r.get("item_name", "")).lower() != item_name.lower():
            continue
        if int(r.get("qty_remain", 0) or 0) > 0:
            lots.append({
                "id": r.get("id"),
                "name": r.get("lot_name"),
                "remain": r.get("qty_remain"),
                "price": r.get("price_per_unit")
            })

    return jsonify({"lots": lots})


@app.route("/api/medicine_list")
def api_medicine_list():
    mtype = request.args.get("type", "").strip().lower()
    res = gas_list("medicine", 2000)
    rows = []
    if res.get("ok"):
        for r in res.get("data", []):
            cur_type = str(r.get("type", "")).strip().lower()
            if not mtype:
                rows.append(r)
            elif cur_type == mtype:
                rows.append(r)

    return jsonify([{"id": r.get("id"), "name": r.get("name")} for r in rows])


@app.route("/api/medicine_id")
def api_medicine_id():
    name = (request.args.get("name") or "").strip()

    # ลอง search ตามชื่อก่อน (บาง GAS ไม่รองรับตรงเป๊ะ จึง fallback)
    sr = gas_search_cached("medicine", "name", name, ttl=20)
    if isinstance(sr, dict) and sr.get("ok"):
        target = norm_key(name)
        for m in _unwrap_rows(sr):
            if norm_key(m.get("name", "")) == target:
                return jsonify({"medicine_id": m.get("id")})

    res = gas_list("medicine", 1000)
    if res.get("ok"):
        target = norm_key(name)
        for m in res.get("data", []):
            if norm_key(m.get("name", "")) == target:
                return jsonify({"medicine_id": m.get("id")})

    return jsonify({"medicine_id": None})


@app.get("/api/medicine_items")
def api_medicine_items():
    group = (request.args.get("group") or "").strip()
    code = (request.args.get("code") or "").strip()

    if not group and not code:
        return jsonify({"items": []})

    # เคสปกติจาก treatment_form จะส่ง group มา -> ใช้ cache ต่อกลุ่มโดยตรง
    if group and not code:
        meds = get_medicines_by_group_fast(group, ttl=25)
        names = []
        seen = set()
        for r in meds:
            name = (r.get("name") or "").strip()
            if not name:
                continue
            k = name.lower()
            if k not in seen:
                seen.add(k)
                names.append(name)
        names.sort(key=lambda s: s.lower())
        return jsonify({"items": names})

    rows = _unwrap_rows(gas_list("medicine", limit=5000))

    names = []
    seen = set()

    for r in rows:
        g = (r.get("group_name") or "").strip()
        name = (r.get("name") or "").strip()
        if not name:
            continue

        if (group and g == group) or (code and g == code):
            key = name.lower()
            if key not in seen:
                seen.add(key)
                names.append(name)

    names.sort(key=lambda s: s.lower())

    return jsonify({"items": names})


@app.route("/api/medicine_lots")
def api_medicine_lots():
    medicine_id = (request.args.get("medicine_id") or "").strip()
    name = (request.args.get("name") or "").strip()

    if not medicine_id and name:
        med_res = gas_list("medicine", 5000)
        if med_res.get("ok"):
            target = norm_key(name)
            for m in med_res.get("data", []):
                if norm_key(m.get("name", "")) == target:
                    medicine_id = str(m.get("id"))
                    break

    if not medicine_id:
        return jsonify({"lots": []})

    rows = _get_lots_by_field_fast("medicine_lot", "medicine_id", str(medicine_id), limit=10000)
    lots = []
    for r in rows:
        if str(r.get("medicine_id", "")) == str(medicine_id):
            if int(r.get("qty_remain", 0) or 0) > 0:
                lots.append({
                    "id": r.get("id"),
                    "name": r.get("lot_name"),
                    "remain": r.get("qty_remain"),
                    "price": r.get("price_per_unit")
                })

    lots.sort(key=lambda x: str(x.get("name", "")))
    return jsonify({"lots": lots})


@app.route("/api/cut_stock", methods=["POST"])
def api_cut_stock():
    data = request.json
    lot_id = data.get("lot_id")
    qty = int(data.get("qty", 0) or 0)

    item_type = str(data.get("type") or data.get("item_type") or "").strip().lower()
    lot_table = "other_lot" if item_type in ("other", "other_item", "อื่นๆ") else "medicine_lot"

    lot_res = gas_get(lot_table, lot_id)
    if not lot_res.get("ok") or not lot_res.get("data"):
        return {"success": False, "message": "ไม่พบ Lot"}

    current = int(lot_res["data"].get("qty_remain", 0))
    if current < qty:
        return {"success": False, "message": "จำนวนคงเหลือไม่พอ"}

    new_remain = current - qty
    gas_update_field(lot_table, lot_id, "qty_remain", new_remain)
    return {"success": True}


# ============================================
# WASTE (ขยะติดเชื้อ)
# ============================================

@app.route("/waste")
@login_required
def waste_menu():
    return render_template("waste.html")


@app.route("/waste/add", methods=["GET", "POST"])
@login_required
def waste_add():
    if request.method == "POST":
        payload = {
            "company": request.form.get("company", "").strip(),
            "amount": request.form.get("amount", "").strip(),
            "date": request.form.get("date", "").strip(),
            "time": request.form.get("time", "").strip(),
            "place": request.form.get("place", "").strip(),
            "photo": request.form.get("photo", "")
        }

        gas_append("waste", payload)
        return redirect("/waste/register")

    return render_template("infectious_add.html")


@app.route("/waste/register")
@login_required
def waste_register():
    res = gas_list("waste", 1000)
    records = res.get("data", []) if res.get("ok") else []
    records.sort(key=lambda x: x.get("created_at", ""), reverse=True)
    return render_template("infectious_register.html", records=records)


@app.route("/waste/view/<int:id>")
@login_required
def waste_view(id):
    res = gas_get("waste", id)
    if not res.get("ok") or not res.get("data"):
        return "ไม่พบข้อมูล", 404
    return render_template("infectious_view.html", w=res["data"])


@app.route("/waste/edit/<int:id>", methods=["GET", "POST"])
@login_required
def waste_edit(id):
    if request.method == "POST":
        photo_new = request.form.get("photo", "").strip()

        old_res = gas_get("waste", id)
        old_photo = ""
        if old_res.get("ok") and old_res.get("data"):
            old_photo = old_res["data"].get("photo", "")

        payload = {
            "company": request.form.get("company", "").strip(),
            "amount": request.form.get("amount", "").strip(),
            "date": request.form.get("date", "").strip(),
            "time": request.form.get("time", "").strip(),
            "place": request.form.get("place", "").strip(),
            "photo": photo_new if photo_new else old_photo
        }

        gas_update("waste", id, payload)
        return redirect("/waste/register")

    res = gas_get("waste", id)
    if not res.get("ok") or not res.get("data"):
        return "ไม่พบข้อมูล", 404
    return render_template("infectious_edit.html", w=res["data"])


@app.route("/waste/delete/<int:id>")
@login_required
def waste_delete(id):
    gas_delete("waste", id)
    return redirect("/waste/register")


# ============================================
# DASHBOARD
# ============================================

def has_supply(medicine_json_text):
    try:
        items = json.loads(medicine_json_text or "[]")
        for it in items:
            t = (it.get("type") or it.get("item_type") or "").strip().lower()
            if t in ("เวชภัณฑ์", "supply", "supplies"):
                return True
        return False
    except:
        return False


@app.get("/api/dashboard/item_master")
@login_required
def api_dashboard_item_master():
    names = []
    seen = set()

    med_res = gas_list("medicine", 5000)
    if med_res.get("ok"):
        for m in med_res.get("data", []):
            name = norm_text(m.get("name", ""))
            if not name:
                continue
            k = norm_key(name)
            if k not in seen:
                seen.add(k)
                names.append(name)

    other_res = gas_list("other_item", 5000)
    if other_res.get("ok"):
        for o in other_res.get("data", []):
            name = norm_text(o.get("name") or o.get("item_name") or "")
            if not name:
                continue
            k = norm_key(name)
            if k not in seen:
                seen.add(k)
                names.append(name)

    names.sort(key=lambda s: s.lower())
    return jsonify({"items": names})


@app.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/dashboard/drug_summary")
@login_required
def dashboard_drug_summary():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    def norm(s):
        if s is None:
            return ""
        s = str(s).strip().lower()
        for ch in ["–", "—", "-", "−"]:
            s = s.replace(ch, "-")
        s = " ".join(s.split())
        return s

    med_res = gas_list("medicine", 5000)
    meds = med_res.get("data", []) if med_res.get("ok") else []

    other_res = gas_list("other_item", 5000)
    others = other_res.get("data", []) if other_res.get("ok") else []

    result = {}
    name_map = {}

    def add_master_name(display_name: str):
        display_name = str(display_name or "").strip()
        if not display_name:
            return
        key = norm(display_name)
        if key not in name_map:
            name_map[key] = display_name
        disp = name_map[key]
        if disp not in result:
            result[disp] = {"used": 0, "remain": 0, "has_used": False, "has_lot": False}

    for m in meds:
        add_master_name(m.get("name", ""))

    for o in others:
        add_master_name(o.get("name") or o.get("item_name") or "")

    def to_display(any_name):
        k = norm(any_name)
        return name_map.get(k)

    lot_res = gas_list("medicine_lot", 10000)
    lots = lot_res.get("data", []) if lot_res.get("ok") else []

    med_id_to_name = {str(m.get("id")): str(m.get("name", "")).strip() for m in meds}

    for lot in lots:
        med_id = str(lot.get("medicine_id", ""))
        med_name = med_id_to_name.get(med_id, "")
        disp = to_display(med_name)
        if disp and disp in result:
            result[disp]["remain"] += int(lot.get("qty_remain", 0) or 0)
            result[disp]["has_lot"] = True

    other_lot_res = gas_list("other_lot", 10000)
    other_lots = other_lot_res.get("data", []) if other_lot_res.get("ok") else []

    for lot in other_lots:
        item_name = str(lot.get("item_name", "")).strip()
        disp = to_display(item_name)
        if not disp:
            add_master_name(item_name)
            disp = to_display(item_name)

        if disp and disp in result:
            result[disp]["remain"] += int(lot.get("qty_remain", 0) or 0)
            result[disp]["has_lot"] = True

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) < 7:
            continue
        try:
            v_year = int(visit_date[:4])
            v_month = int(visit_date[5:7])
        except:
            continue

        if v_year != year or v_month != month:
            continue

        try:
            items = json.loads(t.get("medicine", "[]"))
        except:
            items = []

        if not isinstance(items, list):
            continue

        for it in items:
            name = str(it.get("name") or it.get("item_name") or "").strip()
            qty = int(it.get("qty", 0) or 0)
            if not name or qty <= 0:
                continue

            disp = to_display(name)
            if not disp:
                add_master_name(name)
                disp = to_display(name)

            if disp and disp in result:
                result[disp]["used"] += qty
                result[disp]["has_used"] = True

    return jsonify(result)


@app.route("/api/dashboard/monthly_cost")
@login_required
def api_dashboard_monthly_cost():
    year = request.args.get("year", type=int) or th_now().year

    months = [{"month": i, "drug": 0.0, "supply": 0.0, "other": 0.0, "total": 0.0} for i in range(1, 13)]

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    lot_res = gas_list("medicine_lot", 10000)
    med_lots = lot_res.get("data", []) if lot_res.get("ok") else []
    med_lot_cache = {str(l.get("id")): l for l in med_lots}

    other_lot_res = gas_list("other_lot", 10000)
    other_lots = other_lot_res.get("data", []) if other_lot_res.get("ok") else []
    other_lot_cache = {str(l.get("id")): l for l in other_lots}

    med_res = gas_list("medicine", 5000)
    meds = med_res.get("data", []) if med_res.get("ok") else []
    med_cache = {str(m.get("id")): m for m in meds}

    def norm_type(v):
        t = str(v or "").strip().lower()
        if t in ("other", "other_item", "อื่นๆ", "อื่น", "รายการอื่นๆ"):
            return "other"
        if t in ("supply", "supplies", "เวชภัณฑ์"):
            return "supply"
        return "medicine"

    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) < 7:
            continue

        try:
            v_year = int(visit_date[:4])
            v_month = int(visit_date[5:7])
        except:
            continue

        if v_year != year or v_month < 1 or v_month > 12:
            continue

        try:
            items = json.loads(t.get("medicine", "[]"))
        except:
            items = []

        if not isinstance(items, list):
            continue

        for it in items:
            lot_id = str(it.get("lot_id", "")).strip()
            qty = int(it.get("qty", 0) or 0)
            if not lot_id or qty <= 0:
                continue

            item_type = norm_type(it.get("type") or it.get("item_type"))

            if lot_id in other_lot_cache:
                item_type = "other"

            if item_type == "other":
                lot = other_lot_cache.get(lot_id)
                if not lot:
                    continue
                price_per_unit = float(lot.get("price_per_unit", 0) or 0)
                cost = price_per_unit * qty
                months[v_month - 1]["other"] += cost
                continue

            lot = med_lot_cache.get(lot_id)
            if not lot:
                continue

            price_per_unit = float(lot.get("price_per_unit", 0) or 0)
            med_id = str(lot.get("medicine_id", "")).strip()
            med = med_cache.get(med_id)

            mtype = str((med or {}).get("type", "")).strip().lower()
            if not mtype:
                mtype = "supply" if item_type == "supply" else "medicine"

            cost = price_per_unit * qty
            if mtype == "medicine":
                months[v_month - 1]["drug"] += cost
            elif mtype == "supply":
                months[v_month - 1]["supply"] += cost
            else:
                months[v_month - 1]["drug"] += cost

    for obj in months:
        obj["total"] = obj["drug"] + obj["supply"] + obj["other"]
        obj["drug"] = round(obj["drug"], 2)
        obj["supply"] = round(obj["supply"], 2)
        obj["other"] = round(obj["other"], 2)
        obj["total"] = round(obj["total"], 2)

    return jsonify({"year": year, "months": months})


@app.route("/api/dashboard/top5_month")
@login_required
def api_dashboard_top5_month():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    if not year or not month:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 7:
            try:
                v_year = int(visit_date[:4])
                v_month = int(visit_date[5:7])
            except:
                continue

            if v_year == year and v_month == month:
                try:
                    items = json.loads(t.get("medicine", "[]"))
                except:
                    continue

                if isinstance(items, list):
                    for item in items:
                        name = str(item.get("name") or item.get("item_name") or "").strip()
                        qty = int(item.get("qty", 0) or 0)
                        if name and qty > 0:
                            counter[name] = counter.get(name, 0) + qty

    top5 = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:5]
    return jsonify([{"name": k, "total": v} for k, v in top5])


@app.route("/api/dashboard/top5_year")
@login_required
def api_dashboard_top5_year():
    year = request.args.get("year", type=int)
    if not year:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 4:
            try:
                v_year = int(visit_date[:4])
            except:
                continue

            if v_year == year:
                try:
                    items = json.loads(t.get("medicine", "[]"))
                except:
                    continue

                if isinstance(items, list):
                    for item in items:
                        name = str(item.get("name") or item.get("item_name") or "").strip()
                        qty = int(item.get("qty", 0) or 0)
                        if name and qty > 0:
                            counter[name] = counter.get(name, 0) + qty

    top5 = sorted(counter.items(), key=lambda x: x[1], reverse=True)[:5]
    return jsonify([{"name": k, "total": v} for k, v in top5])


@app.route("/api/dashboard/dept_year")
@login_required
def api_dashboard_dept_year():
    year = request.args.get("year", type=int)
    if not year:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 4:
            try:
                v_year = int(visit_date[:4])
            except:
                continue

            if v_year == year:
                dept = str(t.get("department", "")).strip()
                if dept:
                    counter[dept] = counter.get(dept, 0) + 1

    result = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return jsonify([{"name": k, "total": v} for k, v in result])


@app.route("/api/dashboard/dept_month")
@login_required
def api_dashboard_dept_month():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    if not year or not month:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 7:
            try:
                v_year = int(visit_date[:4])
                v_month = int(visit_date[5:7])
            except:
                continue

            if v_year == year and v_month == month:
                dept = str(t.get("department", "")).strip()
                if dept:
                    counter[dept] = counter.get(dept, 0) + 1

    result = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return jsonify([{"name": k, "total": v} for k, v in result])


@app.route("/api/dashboard/symptom_year")
@login_required
def api_dashboard_symptom_year():
    year = request.args.get("year", type=int)
    if not year:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 4:
            try:
                v_year = int(visit_date[:4])
            except:
                continue

            if v_year == year:
                symptom = str(t.get("symptom_group", "")).strip()
                if symptom:
                    counter[symptom] = counter.get(symptom, 0) + 1

    result = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return jsonify([{"name": k, "total": v} for k, v in result])


@app.route("/api/dashboard/symptom_month")
@login_required
def api_dashboard_symptom_month():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    if not year or not month:
        return jsonify([])

    treat_res = gas_list("treatment", 10000)
    treatments = treat_res.get("data", []) if treat_res.get("ok") else []

    counter = {}
    for t in treatments:
        visit_date = str(t.get("visit_date", ""))
        if len(visit_date) >= 7:
            try:
                v_year = int(visit_date[:4])
                v_month = int(visit_date[5:7])
            except:
                continue

            if v_year == year and v_month == month:
                if has_supply(t.get("medicine", "")):
                    name = "เวชภัณฑ์"
                else:
                    name = str(t.get("symptom_group", "")).strip() or "อื่นๆ"
                counter[name] = counter.get(name, 0) + 1

    result = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return jsonify([{"name": k, "total": v} for k, v in result])


# ============================================
# MEDICAL CERTIFICATE
# ============================================

def _pick(src, *keys):
    for k in keys:
        if isinstance(src, dict):
            v = src.get(k)
        else:
            v = src.get(k)
        if v is not None:
            return str(v).strip()
    return ""


def build_medcert_payload(src):
    return {
        "title": _pick(src, "title"),
        "fullname": _pick(src, "fullname"),
        "address": _pick(src, "address"),
        "citizenId": _pick(src, "citizenId", "citizen_id", "citizenID"),
        "disease": _pick(src, "disease"),
        "disease_detail": _pick(src, "disease_detail"),
        "accident": _pick(src, "accident"),
        "accident_detail": _pick(src, "accident_detail"),
        "hospital": _pick(src, "hospital"),
        "hospital_detail": _pick(src, "hospital_detail"),
        "other_history": _pick(src, "other_history"),
        "requester_sign": _pick(src, "requester_sign"),
        "requester_date": _pick(src, "requester_date"),
        "hospital_name": _pick(src, "hospital_name"),
        "hospital_address": _pick(src, "hospital_address", "hospitalAddress"),
        "weight": _pick(src, "weight"),
        "height": _pick(src, "height"),
        "bp": _pick(src, "bp"),
        "pulse": _pick(src, "pulse"),
        "exam_date": _pick(src, "exam_date", "examDate"),
        "license": _pick(src, "license"),
        "certificate_no": _pick(src, "certificate_no", "cert_number", "certNo", "certificateNo"),
        "body_status": _pick(src, "body_status"),
        "body_detail": _pick(src, "body_detail"),
        "other_disease": _pick(src, "other_disease", "otherDisease"),
        "work_result": _pick(src, "work_result"),
        "doctor_name": _pick(src, "doctor_name"),
        "doctor_sign": _pick(src, "doctor_sign", "doctorSign"),
    }


@app.route("/medical_certificate")
@login_required
def medical_certificate_menu():
    return render_template("certificate_menu.html")


@app.route("/medical_certificate/form", methods=["GET", "POST"])
@login_required
def medical_certificate_form():
    if request.method == "POST":
        payload = build_medcert_payload(request.form)
        gas_append("medical_certificate", payload)
        return redirect("/medical_certificate/register")

    return render_template("certificate_form.html")


@app.route("/medical_certificate/register")
@login_required
def medical_certificate_register():
    res = gas_list("medical_certificate", 1000)
    records = res.get("data", []) if res.get("ok") else []
    return render_template("certificate_register.html", records=records)


@app.route("/medical_certificate/edit/<int:id>")
@login_required
def medical_certificate_edit_with_id(id):
    return render_template("certificate_edit.html", record_id=id)


@app.route("/medical_certificate/print")
@login_required
def medical_certificate_print_temp():
    return render_template("certificate_print.html", record=None)


@app.route("/medical_certificate/print/<int:id>")
@login_required
def medical_certificate_print(id):
    res = gas_get("medical_certificate", id)
    if not res.get("ok") or not res.get("data"):
        return "ไม่พบข้อมูลใบรับรองแพทย์", 404
    return render_template("certificate_print.html", record=res["data"])


# ============================================
# MEDICAL CERTIFICATE API
# ============================================

@app.route("/api/medical_certificate/add", methods=["POST"])
@login_required
def api_medical_certificate_add():
    data = request.json or {}
    payload = build_medcert_payload(data)

    res = gas_append("medical_certificate", payload)
    if res.get("ok"):
        return jsonify({"success": True, "id": res.get("id")})
    return jsonify({"success": False, "message": res.get("message", "Failed to save")})


@app.route("/api/medical_certificate/<int:id>")
@login_required
def api_medical_certificate_get(id):
    res = gas_get("medical_certificate", id)
    if res.get("ok") and res.get("data"):
        return jsonify({"success": True, "data": res["data"], "record": res["data"]})
    return jsonify({"success": False, "message": "Not found"})


@app.route("/api/medical_certificate/edit/<int:id>", methods=["POST"])
@login_required
def api_medical_certificate_edit(id):
    data = request.json or {}
    payload = build_medcert_payload(data)

    res = gas_update("medical_certificate", id, payload)
    if res.get("ok"):
        return jsonify({"success": True})
    return jsonify({"success": False, "message": res.get("message", "Failed to update")})


@app.route("/api/medical_certificate/delete/<int:id>", methods=["DELETE"])
@login_required
def api_medical_certificate_delete(id):
    res = gas_delete("medical_certificate", id)
    if res.get("ok"):
        return jsonify({"success": True})
    return jsonify({"success": False, "message": res.get("message", "Failed to delete")})


# ============================================
# RUN
# ============================================

if __name__ == "__main__":
    debug_mode = os.environ.get('FLASK_DEBUG', 'False') == 'True'
    app.run(debug=debug_mode, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
