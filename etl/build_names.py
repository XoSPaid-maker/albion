#!/usr/bin/env python3
"""Genera docs/data/names.json: nombres en español e inglés de los ítems que aparecen en el informe diario,
a partir del volcado público del juego (ao-bin-dumps). Se ejecuta tras el ETL; si falla, se conserva el archivo anterior."""
import json, os, sys, urllib.request
SRC = "https://raw.githubusercontent.com/ao-data/ao-bin-dumps/master/formatted/items.json"
OUT = os.environ.get("OUT_DIR", "docs/data")
def main():
    ids = set()
    for f in ("market.json", "latest.json"):
        p = os.path.join(OUT, f)
        if not os.path.exists(p): continue
        d = json.load(open(p))
        if f == "market.json": ids |= set(d.keys())
        else:
            for k in ("top_units", "top_silver"): ids |= {r["item"] for r in d.get(k, [])}
    bases = {i.split("@")[0] for i in ids}
    src = sys.argv[1] if len(sys.argv) > 1 else SRC
    data = json.load(open(src)) if os.path.exists(src) else json.load(urllib.request.urlopen(urllib.request.Request(src, headers={"User-Agent": "AlbionMarketIntel/1.0"}), timeout=120))
    names = {}
    for it in data:
        u = it.get("UniqueName"); ln = it.get("LocalizedNames") or {}
        if not u or u not in bases: continue
        es, en = ln.get("ES-ES"), ln.get("EN-US")
        if es or en: names[u] = [es or en, en or es]
    os.makedirs(OUT, exist_ok=True)
    json.dump(names, open(os.path.join(OUT, "names.json"), "w"), ensure_ascii=False, separators=(",", ":"))
    print(f"[names] {len(names)} nombres de {len(bases)} ítems -> {OUT}/names.json")
if __name__ == "__main__": main()
