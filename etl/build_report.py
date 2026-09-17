#!/usr/bin/env python3
"""
Albion Market Intel — ETL diario.
1) Descarga el último volcado diario de Albion Online Data Project (servidor Americas/west).
2) Lo restaura en MySQL local (tablas market_history, market_orders, market_stats).
   El volcado es un mysqldump (.tgz con un .sql): se carga en el MySQL del runner por streaming.
3) Calcula, para TODOS los ítems y TODAS las ciudades: unidades y plata negociadas (7 y 30 días),
   precio promedio, precio actual (orden de venta más barata / orden de compra más alta),
   ciudad más barata / más cara, tendencia. Genera docs/data/latest.json y docs/data/report.md.
El esquema de la base se introspecciona en tiempo de ejecución y se imprime en el log.
"""
import os, re, sys, json, tarfile, subprocess, datetime as dt, urllib.request, html, glob, time
from collections import defaultdict

BASE = os.environ.get("AODP_DB_URL", "https://www.albion-online-data.com/database/")
OUT = os.environ.get("OUT_DIR", "docs/data")
MY = dict(host=os.environ.get("MYSQL_HOST", "127.0.0.1"), port=int(os.environ.get("MYSQL_PORT", "3306")),
          user=os.environ.get("MYSQL_USER", "root"), password=os.environ.get("MYSQL_PASSWORD", "root"), database="aodp")
MYARGS = f"-h{MY['host']} -P{MY['port']} -u{MY['user']} -p{MY['password']} --protocol=tcp"
# Ids de localización de AODP -> nombre de ciudad
LOC = {"7": "Thetford", "1002": "Lymhurst", "2004": "Bridgewatch", "3003": "Black Market", "3005": "Caerleon", "3008": "Martlock",
       "4002": "Fort Sterling", "5003": "Brecilien", "FortSterling": "Fort Sterling", "BlackMarket": "Black Market"}
CITIES = ["Bridgewatch", "Fort Sterling", "Lymhurst", "Martlock", "Thetford", "Caerleon", "Brecilien", "Black Market"]
NOW = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

def log(*a): print("[etl]", *a, flush=True)

# ---------- 1. download ----------
UA = "Mozilla/5.0 (X11; Linux x86_64) AlbionMarketIntel/1.0 (+github actions)"

def fetch(url, timeout=180):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    return urllib.request.urlopen(req, timeout=timeout)

def list_index(url):
    """Lee un autoindex (nginx/apache) y devuelve [(nombre, fecha, tamaño)] tal como aparece."""
    t0 = time.time()
    page = fetch(url).read().decode("utf-8", "ignore")
    log(f"índice {url}: {len(page)} bytes en {time.time() - t0:.1f}s")
    rows = re.findall(r'href="([^"?]+)"[^\n]*?</a>\s*([0-9]{2}-\w{3}-[0-9]{4} [0-9:]+|[0-9-]+ [0-9:]+)?\s*([0-9.]+[KMGT]?|-)?', page)
    if not rows:
        rows = [(n, "", "") for n in re.findall(r'href="([^"?]+)"', page)]
    return [(html.unescape(n), d or "", z or "") for n, d, z in rows]

def latest_dump_url():
    for url in (BASE, BASE.rstrip("/") + "/backup/"):
        try:
            rows = list_index(url)
        except Exception as e:
            log("no se pudo leer", url, "->", repr(e)); continue
        dumps = sorted(r for r in rows if re.match(r"db_backup_.*\.tgz$", r[0]))
        hist = sorted(r for r in rows if "market_history" in r[0])
        log(f"{url}: {len(rows)} entradas; {len(dumps)} db_backup; {len(hist)} market_history")
        for r in dumps[-5:]: log("  db_backup:", r)
        for r in hist[-5:]: log("  market_history:", r)
        others = [r for r in rows if r not in dumps and r not in hist][:15]
        if others: log("  otros:", others)
        if dumps:
            name = dumps[-1][0]
            return url + name, name
    raise SystemExit("No se encontró ningún db_backup_*.tgz en " + BASE)

def download(url, path):
    if os.path.exists(path) and os.path.getsize(path) > 1e6: log("ya descargado", path); return
    sh("df -h . && free -m || true")
    log("descargando", url)
    t0 = time.time()
    sh(f"curl -fsSL --retry 3 --retry-delay 10 --connect-timeout 60 -A '{UA}' -o '{path}' '{url}'")
    log("descargado", os.path.getsize(path) // (1 << 20), "MB en", round(time.time() - t0), "s")
    sh("df -h . || true")

# ---------- 2. restore (MySQL) ----------
def sh(cmd, **kw):
    log("$", cmd); return subprocess.run(cmd, shell=True, check=True, executable="/bin/bash", **kw)

def out(cmd):
    return subprocess.run(cmd, shell=True, check=True, executable="/bin/bash", capture_output=True, text=True).stdout

def count_values(tuple_text):
    """Cuenta los valores del primer (...) de un INSERT, respetando comillas."""
    n, inq, depth = 1, False, 0
    for i, ch in enumerate(tuple_text):
        if inq:
            if ch == "\\": continue
            if ch == "'": inq = False
        elif ch == "'": inq = True
        elif ch == "(": depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0: return n
        elif ch == "," and depth == 1: n += 1
    return n

def sql_filter(n_orders):
    """Filtro de streaming del mysqldump (MariaDB) para que cargue en MySQL 8:
    - omite las órdenes expiradas (no se usan);
    - la columna generada updated_at_bin usa unix_timestamp(), prohibido en columnas generadas de MySQL 8:
      si el INSERT trae su valor se convierte en columna normal; si no, se reescribe con TIMESTAMPDIFF."""
    import sys
    inp, outp = sys.stdin.buffer, sys.stdout.buffer
    for line in inp:
        if line.startswith(b"INSERT INTO `market_orders_expired`"): continue
        if line.startswith(b"  `updated_at_bin`"):
            if n_orders >= 15: line = b"  `updated_at_bin` int(10) unsigned DEFAULT NULL,\n"
            else: line = line.replace(b"unix_timestamp(`updated_at`)", b"TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', `updated_at`)")
        outp.write(line)

def restore(tgz):
    listing = out(f"tar -tvzf '{tgz}'").strip().splitlines()
    log("miembros del tar:", listing[:30], "... total", len(listing))
    members = [l.split()[-1] for l in listing]
    sqls = [m for m in members if m.endswith(".sql")]
    if not sqls: raise SystemExit("El volcado no contiene ningún .sql: " + ", ".join(members[:20]))
    sql = sqls[0]
    log("sentencias CREATE TABLE:\n" + out(f"tar -xOzf '{tgz}' '{sql}' | grep -n -A24 '^CREATE TABLE' | grep -v '^[0-9]*-INSERT' | head -200"))
    first = out(f"tar -xOzf '{tgz}' '{sql}' | grep -m1 '^INSERT INTO `market_orders` VALUES' | head -c 3000")
    n_orders = count_values(first[first.index("(") :]) if "(" in first else 0
    log("primer INSERT de market_orders:", first[:400].rstrip(), "| valores por fila:", n_orders)
    sh(f"mysql {MYARGS} -e \"SELECT VERSION(); SET GLOBAL innodb_flush_log_at_trx_commit=0; SET GLOBAL sync_binlog=0; SET GLOBAL max_allowed_packet=1073741824; "
       f"SET GLOBAL time_zone='+00:00'; DROP DATABASE IF EXISTS albion; DROP DATABASE IF EXISTS {MY['database']}; CREATE DATABASE {MY['database']} CHARACTER SET utf8mb4;\"")
    t0 = time.time()
    # streaming: sin extraer a disco
    sh(f"(echo 'SET sql_log_bin=0; SET unique_checks=0; SET foreign_key_checks=0; SET time_zone=\"+00:00\";'; tar -xOzf '{tgz}' '{sql}' | python3 {os.path.abspath(__file__)} --filter {n_orders}) "
       f"| mysql {MYARGS} --max_allowed_packet=1G --force {MY['database']} 2>&1 | grep -v 'password on the command line' | head -n 60")
    log("carga terminada en", round(time.time() - t0), "s")
    sh("df -h . || true")
    # el volcado hace CREATE DATABASE/USE con su propio nombre: se localiza la base que tiene el historial
    db = out(f"mysql {MYARGS} -N -e \"select table_schema from information_schema.tables where table_name = 'market_history' and table_schema not in ('information_schema','performance_schema','mysql','sys') limit 1\" 2>/dev/null").strip()
    if db: MY["database"] = db
    log("base de datos con las tablas:", MY["database"])
    log("tablas y filas:\n" + out(f"mysql {MYARGS} -e \"select table_name, table_rows, round(data_length/1048576) as mb from information_schema.tables where table_schema='{MY['database']}'\" 2>/dev/null"))

# ---------- 3. compute ----------
def connect():
    import pymysql; return pymysql.connect(**MY)

def schema(cur):
    cur.execute("select table_name, column_name, data_type from information_schema.columns where table_schema=%s order by table_name, ordinal_position", (MY["database"],))
    s = defaultdict(list)
    for t, c, d in cur.fetchall(): s[t].append((c, d))
    for t, cols in s.items():
        log("tabla", t, [(c, d) for c, d in cols])
        try:
            cur.execute("select table_rows from information_schema.tables where table_schema=%s and table_name=%s", (MY["database"], t)); est = cur.fetchone()
            cur.execute(f"select * from `{t}` limit 3"); log("  filas~", est[0] if est else "?", "muestra:", cur.fetchall())
        except Exception as e:
            log("  no se pudo muestrear", t, repr(e))
    return s

def pick(cols, *cands):
    names = [c for c, _ in cols]
    for c in cands:
        if c in names: return c
    for c in cands:
        for n in names:
            if c in n: return n
    return None

CHEAP = ("T4_FIBER", "T4_ORE", "T4_WOOD", "T4_HIDE", "T4_ROCK", "T2_FIBER", "T3_ORE")
def detect_scale(cur, label, sql, params=()):
    """AODP guarda precios ×10000. Se comprueba con recursos baratos (< 1000 plata): sql debe devolver un precio unitario por fila."""
    cur.execute(sql, tuple(params) + CHEAP)
    vals = sorted(float(v[0]) for v in cur.fetchall() if v[0])
    med = vals[len(vals) // 2] if vals else None
    scale = 10000 if med and med > 5000 else 1
    log(f"escala de precios en {label}: mediana {med} ({len(vals)} valores) -> ÷{scale}")
    return scale

def norm_loc(loc):
    s = str(loc); return LOC.get(s, s)

def compute(cur, s):
    hist_t = "market_history" if "market_history" in s else next((t for t in s if "history" in t), None); ord_t = "market_orders" if "market_orders" in s else next((t for t in s if t.endswith("orders")), None)
    if not hist_t: raise SystemExit("No encuentro la tabla de historial. Tablas: " + ", ".join(s))
    H = s[hist_t]
    h_item = pick(H, "item_id", "item"); h_loc = pick(H, "location", "city"); h_ts = pick(H, "timestamp", "date", "time")
    h_amt = pick(H, "item_amount", "amount", "count"); h_sil = pick(H, "silver_amount", "silver"); h_scale = pick(H, "timescale", "aggregation"); h_q = pick(H, "quality_level", "quality")
    log("mapeo historial:", dict(item=h_item, loc=h_loc, ts=h_ts, amt=h_amt, sil=h_sil, scale=h_scale, q=h_q))
    cur.execute(f"select max(`{h_ts}`) from `{hist_t}`"); maxts = cur.fetchone()[0]; log("último timestamp historial:", maxts)
    ref = maxts if isinstance(maxts, dt.datetime) else NOW
    d30 = ref - dt.timedelta(days=30)
    scale_clause = ""
    if h_scale:
        cur.execute(f"select `{h_scale}`, count(*), sum(`{h_amt}`), min(`{h_ts}`), max(`{h_ts}`) from `{hist_t}` where `{h_ts}` >= %s group by 1", (d30,)); rows = cur.fetchall(); log("niveles de agregación (últimos 30 d: nivel, filas, unidades, desde, hasta):", rows)
        if rows:
            best = max(rows, key=lambda r: float(r[2] or 0))[0]; scale_clause = f"and `{h_scale}` = {int(best)}"; log("nivel de agregación elegido (más unidades cubiertas):", best)
    cur.execute(f"select `{h_loc}`, count(*) from `{hist_t}` where `{h_ts}` >= %s group by 1 order by 2 desc limit 30", (d30,)); log("localizaciones historial:", cur.fetchall())
    ph = ",".join(["%s"] * len(CHEAP))
    div = detect_scale(cur, hist_t, f"select sum(`{h_sil}`)/sum(`{h_amt}`) from `{hist_t}` where `{h_ts}` >= %s and `{h_item}` in ({ph}) group by `{h_item}`, `{h_loc}` having sum(`{h_amt}`) > 0", (d30,))
    q = f"""
      select `{h_item}`, `{h_loc}`,
        sum(case when `{h_ts}` >= %s then `{h_amt}` else 0 end) as u7, sum(case when `{h_ts}` >= %s then `{h_sil}` else 0 end) as s7,
        sum(`{h_amt}`) as u30, sum(`{h_sil}`) as s30,
        sum(case when `{h_ts}` >= %s then `{h_amt}` else 0 end) as u3, sum(case when `{h_ts}` >= %s then `{h_sil}` else 0 end) as s3,
        sum(case when `{h_ts}` < %s and `{h_ts}` >= %s then `{h_amt}` else 0 end) as up, sum(case when `{h_ts}` < %s and `{h_ts}` >= %s then `{h_sil}` else 0 end) as sp
      from `{hist_t}` where `{h_ts}` >= %s {scale_clause}
      group by 1,2"""
    d7, d3, d4 = ref - dt.timedelta(days=7), ref - dt.timedelta(days=3), ref - dt.timedelta(days=4)
    cur.execute(q, (d7, d7, d3, d3, d4, d7, d4, d7, d30))
    items = defaultdict(dict); skipped = defaultdict(int)
    for item, loc, u7, s7, u30, s30, u3, s3, up, sp in cur.fetchall():
        loc = norm_loc(loc)
        if loc not in CITIES: skipped[loc] += 1; continue
        if not u7 and not u30: continue
        u7, s7, u30, s30, u3, s3, up, sp = [float(x or 0) for x in (u7, s7, u30, s30, u3, s3, up, sp)]
        s7, s30, s3, sp = s7 / div, s30 / div, s3 / div, sp / div
        a7 = (s7 / u7) if u7 else None; a30 = (s30 / u30) if u30 else None
        a3 = (s3 / u3) if u3 else None; ap = (sp / up) if up else None
        items[item][loc] = dict(u7=int(u7), s7=int(s7), a7=round(a7) if a7 else None, u30=int(u30), s30=int(s30), a30=round(a30) if a30 else None,
                                trend=round((a3 - ap) / ap * 100, 1) if a3 and ap else None)
    log("ítems con historial:", len(items), "| localizaciones descartadas:", dict(sorted(skipped.items(), key=lambda kv: -kv[1])[:15]))
    # current orders
    if ord_t:
        O = s[ord_t]
        o_item = pick(O, "item_id", "item"); o_loc = pick(O, "location", "city"); o_price = pick(O, "unit_price_silver", "price"); o_type = pick(O, "auction_type", "type")
        o_upd = pick(O, "updated_at", "updated", "timestamp"); o_q = pick(O, "quality_level", "quality"); o_exp = pick(O, "expires"); o_del = pick(O, "deleted_at")
        log("mapeo órdenes:", dict(item=o_item, loc=o_loc, price=o_price, type=o_type, upd=o_upd, q=o_q, exp=o_exp))
        cur.execute(f"select max(`{o_upd}`), count(*) from `{ord_t}`"); oref, n = cur.fetchone(); log("órdenes:", n, "última actualización:", oref)
        oref = oref if isinstance(oref, dt.datetime) else ref
        cur.execute(f"select `{o_type}`, count(*) from `{ord_t}` group by 1"); log("tipos de orden:", cur.fetchall())
        exp_clause = (f"and (`{o_exp}` is null or `{o_exp}` > %s)" if o_exp else "") + (f" and `{o_del}` is null" if o_del else "")
        q_clause = f"and `{o_q}` in (1,2)" if o_q else ""
        odiv = detect_scale(cur, ord_t, f"select `{o_price}` from `{ord_t}` where `{o_type}` = 'offer' and `{o_upd}` >= %s and `{o_item}` in ({ph})", (oref - dt.timedelta(days=2),))
        params = (oref - dt.timedelta(days=2),) + ((oref,) if o_exp else ())
        cur.execute(f"""select `{o_item}`, `{o_loc}`,
                          min(case when `{o_type}` = 'offer' then `{o_price}` end), max(case when `{o_type}` = 'request' then `{o_price}` end), max(`{o_upd}`)
                        from `{ord_t}` where `{o_upd}` >= %s {exp_clause} {q_clause} group by 1,2""", params)
        n = 0
        for item, loc, smin, bmax, upd in cur.fetchall():
            loc = norm_loc(loc)
            if loc not in CITIES: continue
            d = items[item].setdefault(loc, {})
            if smin: d["sell"] = int(float(smin) / odiv)
            if bmax: d["buy"] = int(float(bmax) / odiv)
            d["upd"] = upd.isoformat() if hasattr(upd, "isoformat") else str(upd); n += 1
        log("precios actuales cargados:", n)
    return items, ref

# ---------- 4. write outputs ----------
def write(items, ref):
    os.makedirs(OUT, exist_ok=True)
    rows = []
    for item, cities in items.items():
        u7 = sum(c.get("u7", 0) for c in cities.values()); s7 = sum(c.get("s7", 0) for c in cities.values())
        if not u7: continue
        top_city = max(cities.items(), key=lambda kv: kv[1].get("u7", 0))
        sells = [(c, d["sell"]) for c, d in cities.items() if d.get("sell") and c != "Black Market"]
        cheapest = min(sells, key=lambda x: x[1]) if sells else None; dearest = max(sells, key=lambda x: x[1]) if sells else None
        bm = cities.get("Black Market", {}).get("buy")
        trends = [d["trend"] for d in cities.values() if d.get("trend") is not None and d.get("u7", 0) >= 20]
        rows.append(dict(item=item, u7=u7, s7=s7, avg7=round(s7 / u7), top_city=top_city[0], top_city_u7=top_city[1].get("u7", 0), top_city_avg=top_city[1].get("a7"),
                         cheapest=cheapest, dearest=dearest, bm_buy=bm, trend=round(sum(trends) / len(trends), 1) if trends else None,
                         cities={c: d for c, d in cities.items()}))
    by_units = sorted(rows, key=lambda r: -r["u7"]); by_silver = sorted(rows, key=lambda r: -r["s7"])
    data = dict(generated=NOW.isoformat() + "Z", data_until=(ref.isoformat() if hasattr(ref, "isoformat") else str(ref)), server="Americas (west)", source=BASE,
                items=len(rows), top_units=[slim(r) for r in by_units[:500]], top_silver=[slim(r) for r in by_silver[:500]])
    json.dump(data, open(f"{OUT}/latest.json", "w"), separators=(",", ":"))
    json.dump({r["item"]: r["cities"] for r in rows}, open(f"{OUT}/market.json", "w"), separators=(",", ":"))
    md = [f"# Albion Market Intel — informe diario ({NOW:%Y-%m-%d %H:%M} UTC)", f"Servidor Americas · datos hasta {data['data_until']} · {len(rows)} ítems con ventas en 7 días", "",
          "## Top 30 por unidades vendidas (7 días)", "", "| # | Ítem | Uds 7d | Plata 7d | Precio prom. | Donde más se vende | Más barato ahora | Más caro ahora | Mercado Negro compra | Tendencia |", "|--|--|--:|--:|--:|--|--|--|--:|--:|"]
    for i, r in enumerate(by_units[:30], 1): md.append(line(i, r))
    md += ["", "## Top 30 por plata movida (7 días)", "", "| # | Ítem | Uds 7d | Plata 7d | Precio prom. | Donde más se vende | Más barato ahora | Más caro ahora | Mercado Negro compra | Tendencia |", "|--|--|--:|--:|--:|--|--|--|--:|--:|"]
    for i, r in enumerate(by_silver[:30], 1): md.append(line(i, r))
    open(f"{OUT}/report.md", "w").write("\n".join(md))
    log("escrito", f"{OUT}/latest.json", f"{OUT}/market.json", f"{OUT}/report.md")

def slim(r): x = dict(r); x.pop("cities", None); return x
def f(n): return f"{n:,}".replace(",", ".") if isinstance(n, (int, float)) and n is not None else "—"
def line(i, r):
    ch = f"{r['cheapest'][0]} {f(r['cheapest'][1])}" if r["cheapest"] else "—"; de = f"{r['dearest'][0]} {f(r['dearest'][1])}" if r["dearest"] else "—"
    return f"| {i} | `{r['item']}` | {f(r['u7'])} | {f(r['s7'])} | {f(r['avg7'])} | {r['top_city']} ({f(r['top_city_u7'])}) | {ch} | {de} | {f(r['bm_buy'])} | {('%+.1f%%' % r['trend']) if r['trend'] is not None else '—'} |"

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--filter":
        sql_filter(int(sys.argv[2]) if len(sys.argv) > 2 else 0); sys.exit(0)
    url, name = latest_dump_url(); download(url, name); restore(name)
    con = connect(); cur = con.cursor(); s = schema(cur); items, ref = compute(cur, s); write(items, ref)
