"""Construit le site statique pour GitHub Pages dans le dossier ./site

    python build_web.py

Le dossier produit contient :
  index.html               la version web (appelle directement https://api.deadlock-api.com)
  deadlock-tracker-pc.zip  la version PC (serveur local + lecture du cache Steam), en téléchargement
  .nojekyll, 404.html, README.md
"""
import os, re, shutil, sys, zipfile

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(ROOT, "site")
src = open(os.path.join(ROOT, "index.html"), encoding="utf-8").read()

web, n = re.subn(r'const BUILD = "local";', 'const BUILD = "web";', src)
if n != 1:
    sys.exit("BUILD introuvable dans index.html")
api = os.environ.get("DEADLOCK_API_BASE")
if api:
    web = web.replace('const API_DIRECT = "https://api.deadlock-api.com";', 'const API_DIRECT = "%s";' % api.rstrip("/"))

shutil.rmtree(OUT, ignore_errors=True)
os.makedirs(OUT)
open(os.path.join(OUT, "index.html"), "w", encoding="utf-8").write(web)
open(os.path.join(OUT, "404.html"), "w", encoding="utf-8").write(
    '<!doctype html><meta charset="utf-8"><meta http-equiv="refresh" content="0;url=./"><title>Deadlock Tracker</title><a href="./">Deadlock Tracker</a>')
open(os.path.join(OUT, ".nojekyll"), "w").close()

with zipfile.ZipFile(os.path.join(OUT, "deadlock-tracker-pc.zip"), "w", zipfile.ZIP_DEFLATED) as z:
    for f in ("index.html", "deadlock_tracker.py", "launch_tracker.bat"):
        z.write(os.path.join(ROOT, f), "deadlock-tracker/" + f)

for f in sorted(os.listdir(OUT)):
    print("%-26s %8d octets" % (f, os.path.getsize(os.path.join(OUT, f))))
