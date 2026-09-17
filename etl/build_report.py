#!/usr/bin/env python3
"""
Albion Market Intel — ETL diario.
1) Descarga el último volcado diario de Albion Online Data Project (servidor Americas/west).
2) Lo restaura en Postgres local.
3) Calcula, para TODOS los ítems y TODAS las ciudades: unidades y plata negociadas (7 y 30 días),
   precio promedio, precio actual (orden de venta más barata / orden de compra más alta),
   ciudad más barata / más cara, tendencia. Genera docs/data/latest.json y docs/data/report.md.
El esquema de la base se introspecciona en tiempo de ejecución y se imprime en el log.
"""
import os, re, sys, json, tarfile, subprocess, datetime as dt, urllib.request, html, glob, time
from collections import defaultdict

BASE = os.environ.get("AODP_DB_URL", "https://www.albion-online-data.com/database/")
OUT = os.environ.get("OUT_DIR", "docs/data")
PG = dict(host="localhost", port=5432, user="postgres", password="postgres", dbname="aodp")
CITIES = ["Bridgewatch", "Fort Sterling", "Lymhurst", "Martlock", "Thetford", "Caerleon", "Brecilien", "Black Market"]
NOW = dt.datetime.utcnow()

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

# ---------- 2. restore ----------
def sh(cmd, **kw):
    log("$", cmd); return subprocess.run(cmd, shell=True, check=True, executable="/bin/bash", **kw)

def restore(tgz):
    os.makedirs("dump", exist_ok=True)
    with tarfile.open(tgz) as t:
        members = t.getmembers()
        log("miembros del tar:", [(m.name, m.size) for m in members[:30]], "... total", len(members))
        t.extractall("dump")
    os.remove(tgz)  # liberar disco antes de restaurar
    files = [p for p in glob.glob("dump/**/*", recursive=True) if os.path.isfile(p)]
    log("archivos en el volcado:", [(p, os.path.getsize(p)) for p in files[:20]])
    sh("df -h . || true")
    env = dict(os.environ, PGPASSWORD=PG["password"])
    sh(f"psql -h {PG['host']} -U {PG['user']} -c 'DROP DATABASE IF EXISTS {PG['dbname']}' postgres", env=env)
    sh(f"psql -h {PG['host']} -U {PG['user']} -c 'CREATE DATABASE {PG['dbname']}' postgres", env=env)
    sqls = [f for f in files if f.endswith(".sql")]
    dumps = [f for f in files if re.search(r"\.(dump|backup|pgdump|custom)$", f)]
    tocs = [f for f in files if os.path.basename(f) == "toc.dat"]
    if tocs:  # directory format
        sh(f"pg_restore -h {PG['host']} -U {PG['user']} -d {PG['dbname']} --no-owner --no-privileges -j 4 {os.path.dirname(tocs[0])} || true", env=env)
    elif dumps:
        sh(f"pg_restore -h {PG['host']} -U {PG['user']} -d {PG['dbname']} --no-owner --no-privileges -j 4 {dumps[0]} || true", env=env)
    elif sqls:
        sh(f"psql -h {PG['host']} -U {PG['user']} -d {PG['dbname']} -q -f {sqls[0]}", env=env)
    else:
        # maybe a single custom-format file without extension
        big = max(files, key=os.path.getsize)
        sh(f"pg_restore -h {PG['host']} -U {PG['user']} -d {PG['dbname']} --no-owner --no-privileges {big} || psql -h {PG['host']} -U {PG['user']} -d {PG['dbname']} -q -f {big}", env=env)

# ---------- 3. compute ----------
def connect():
    import psycopg2; return psycopg2.connect(**PG)

def schema(cur):
    cur.execute("select table_name, column_name, data_type from information_schema.columns where table_schema='public' order by table_name, ordinal_position")
    s = defaultdict(list)
    for t, c, d in cur.fetchall(): s[t].append((c, d))
    for t, cols in s.items():
        log("tabla", t, [(c, d) for c, d in cols])
        try:
            cur.execute(f"select reltuples::bigint from pg_class where relname = %s", (t,)); est = cur.fetchone()
            cur.execute(f'select * from "{t}" limit 3'); log("  filas~", est[0] if est else "?", "muestra:", cur.fetchall())
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

def compute(cur, s):
    hist_t = next((t for t in s if "history" in t), None); ord_t = next((t for t in s if "order" in t), None)
    if not hist_t: raise SystemExit("No encuentro la tabla de historial. Tablas: " + ", ".join(s))
    H = s[hist_t]
    h_item = pick(H, "item_id", "item"); h_loc = pick(H, "location", "city"); h_ts = pick(H, "timestamp", "date", "time")
    h_amt = pick(H, "item_amount", "amount", "count"); h_sil = pick(H, "silver_amount", "silver"); h_scale = pick(H, "timescale", "aggregation"); h_q = pick(H, "quality_level", "quality")
    log("mapeo historial:", dict(item=h_item, loc=h_loc, ts=h_ts, amt=h_amt, sil=h_sil, scale=h_scale, q=h_q))
    scale_clause = f"and {h_scale} = (select max({h_scale}) from {hist_t})" if h_scale else ""
    # distinct scales info
    if h_scale:
        cur.execute(f"select {h_scale}, count(*) from {hist_t} group by 1"); log("timescales:", cur.fetchall())
    cur.execute(f"select max({h_ts}) from {hist_t}"); maxts = cur.fetchone()[0]; log("último timestamp historial:", maxts)
    q = f"""
      select {h_item}, {h_loc},
        sum(case when {h_ts} >= %s then {h_amt} else 0 end) as u7, sum(case when {h_ts} >= %s then {h_sil} else 0 end) as s7,
        sum({h_amt}) as u30, sum({h_sil}) as s30,
        sum(case when {h_ts} >= %s then {h_amt} else 0 end) as u3, sum(case when {h_ts} >= %s then {h_sil} else 0 end) as s3,
        sum(case when {h_ts} < %s and {h_ts} >= %s then {h_amt} else 0 end) as up, sum(case when {h_ts} < %s and {h_ts} >= %s then {h_sil} else 0 end) as sp
      from {hist_t} where {h_ts} >= %s {scale_clause}
      group by 1,2"""
    ref = maxts if isinstance(maxts, dt.datetime) else NOW
    d7, d30, d3, d4, d7b = ref - dt.timedelta(days=7), ref - dt.timedelta(days=30), ref - dt.timedelta(days=3), ref - dt.timedelta(days=4), ref - dt.timedelta(days=7)
    cur.execute(q, (d7, d7, d3, d3, d4, d7b, d4, d7b, d30))
    items = defaultdict(dict)
    for item, loc, u7, s7, u30, s30, u3, s3, up, sp in cur.fetchall():
        loc = norm_loc(loc)
        if loc not in CITIES or not u7 and not u30: continue
        a7 = (s7 / u7) if u7 else None; a30 = (s30 / u30) if u30 else None
        a3 = (s3 / u3) if u3 else None; ap = (sp / up) if up else None
        items[item][loc] = dict(u7=int(u7 or 0), s7=int(s7 or 0), a7=round(a7) if a7 else None, u30=int(u30 or 0), s30=int(s30 or 0), a30=round(a30) if a30 else None,
                                trend=round((a3 - ap) / ap * 100, 1) if a3 and ap else None)
    log("ítems con historial:", len(items))
    # current orders
    if ord_t:
        O = s[ord_t]
        o_item = pick(O, "item_id", "item"); o_loc = pick(O, "location", "city"); o_price = pick(O, "unit_price_silver", "price"); o_type = pick(O, "auction_type", "type")
        o_upd = pick(O, "updated_at", "updated", "timestamp"); o_q = pick(O, "quality_level", "quality"); o_ench = pick(O, "enchantment_level", "enchant"); o_exp = pick(O, "expires")
        log("mapeo órdenes:", dict(item=o_item, loc=o_loc, price=o_price, type=o_type, upd=o_upd, q=o_q, ench=o_ench, exp=o_exp))
        exp_clause = f"and ({o_exp} is null or {o_exp} > now())" if o_exp else ""
        q_clause = f"and {o_q} in (1,2)" if o_q else ""
        cur.execute(f"""select {o_item}, {o_loc}, {o_type}, min({o_price}) filter (where {o_type} ilike 'offer'), max({o_price}) filter (where {o_type} ilike 'request'), max({o_upd})
                        from {ord_t} where {o_upd} >= %s {exp_clause} {q_clause} group by 1,2,3""", (ref - dt.timedelta(days=2),))
        for item, loc, typ, smin, bmax, upd in cur.fetchall():
            loc = norm_loc(loc)
            if loc not in CITIES: continue
            d = items[item].setdefault(loc, {})
            if smin: d["sell"] = int(smin / 10000) if smin > 1e7 else int(smin)  # AODP guarda precios ×10000
            if bmax: d["buy"] = int(bmax / 10000) if bmax > 1e7 else int(bmax)
            d["upd"] = upd.isoformat() if hasattr(upd, "isoformat") else str(upd)
    return items, ref

def norm_loc(loc):
    m = {"FortSterling": "Fort Sterling", "BlackMarket": "Black Market", "3005": "Caerleon", "Caerleon": "Caerleon"}
    s = str(loc); return m.get(s, s)

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
    url, name = latest_dump_url(); download(url, name); restore(name)
    con = connect(); cur = con.cursor(); s = schema(cur); items, ref = compute(cur, s); write(items, ref)
