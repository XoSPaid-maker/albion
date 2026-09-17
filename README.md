# Albion Market Intel

Terminal económica de Albion Online (servidor Americas): qué comprar, dónde, cuánto, qué craftear, dónde y dónde vender.

- `docs/index.html` — la aplicación (funciona sola en el navegador; en GitHub Pages además lee el informe diario).
- `etl/build_report.py` — descarga el volcado diario (mysqldump) de [Albion Online Data Project](https://www.albion-online-data.com/database/), lo carga en el MySQL del runner y calcula, para todos los ítems y ciudades, unidades y plata negociadas (7/30 días), precio promedio, precio actual, ciudad más barata/cara y tendencia.
- `.github/workflows/daily-report.yml` — lo ejecuta cada día a las 11:15 UTC (6:15 a. m. Bogotá), guarda `docs/data/latest.json`, `market.json`, `report.md` y `etl.log` (log de la última corrida, también si falla), y publica el sitio en GitHub Pages solo desde `main`.

## Puesta en marcha
1. Settings → Pages → Source: **GitHub Actions**.
2. Actions → *Albion daily market report* → **Run workflow** (la primera vez, para no esperar a la madrugada).
3. Abre `https://<usuario>.github.io/<repo>/`.

Datos: AODP (crowdsourced). Iconos: render.albiononline.com. Sin garantías; verifica en el juego antes de invertir.
