#!/usr/bin/env python3
"""Deadlock Tracker - serveur local.

Lance l'interface dans ton navigateur et sert de pont vers l'API communautaire
https://api.deadlock-api.com :

  * chaque réponse de l'API est mise en cache sur le disque (data/cache/*.json)
    avec une durée de validité adaptée au type de donnée ; si l'API ne répond
    pas, la dernière version en cache est servie ;
  * ta sauvegarde (compte, historique cumulé, notes, routes, réglages) est écrite
    dans data/save.json, avec une copie de sécurité à chaque changement
    (data/backups/, 15 dernières versions) ;
  * il détecte ton compte Steam sur ce PC (config/loginusers.vdf) ;
  * il lit le cache HTTP de Steam (appcache/httpcache) pour retrouver les parties
    Deadlock que tu as ouvertes en jeu, peut les envoyer à l'API (comme l'outil
    officiel deadlock-api-ingest) et reconstruit ton historique à partir du
    détail de chaque partie, même si l'API ne connaît pas encore ton compte ;
  * une synchronisation automatique tourne toutes les 10 minutes si tu l'actives.

Utilisation :
    python deadlock_tracker.py            # port 8787, ouvre le navigateur
    python deadlock_tracker.py --port 9000 --no-browser
    python deadlock_tracker.py --steam "D:\\Steam"   # si Steam n'est pas trouvé

Aucune dépendance : Python 3.8+ suffit.
"""

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_BASE = os.environ.get("DEADLOCK_API_BASE", "https://api.deadlock-api.com").rstrip("/")
ROOT = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(ROOT, "data")
CACHE_DIR = os.path.join(DATA, "cache")
BACKUP_DIR = os.path.join(DATA, "backups")
SAVE_FILE = os.path.join(DATA, "save.json")
LOCAL_FILE = os.path.join(DATA, "steam_local.json")
INDEX_FILE = os.path.join(ROOT, "index.html")
USER_AGENT = "DeadlockTracker/4.0 (local personal tracker)"
MAX_BACKUPS = 15
STEAM64_BASE = 76561197960265728
DEADLOCK_APP_ID = "1422450"
SYNC_INTERVAL_S = 10 * 60

TTL_RULES = [
    ("/metadata", 30 * 24 * 3600),
    ("/v1/assets/", 24 * 3600),
    ("/v1/analytics/", 6 * 3600),
    ("/match-history", 10 * 60),
    ("/rank", 30 * 60),
    ("/v1/players/hero-stats", 30 * 60),
    ("/v1/players/steam", 6 * 3600),
    ("/v1/builds", 6 * 3600),
    ("/patches", 3 * 3600),
]
DEFAULT_TTL = 3600

STEAM_OVERRIDE = None
_save_lock = threading.Lock()
_local_lock = threading.Lock()
_sync_lock = threading.Lock()
_inflight = {}
_inflight_lock = threading.Lock()
STATUS = {
    "last_sync": 0, "last_sync_result": None, "sync_running": False,
    "last_ingest": 0, "last_ingest_result": None, "save_mtime": 0, "next_sync": 0,
}



def ttl_for(path):
    for prefix, ttl in TTL_RULES:
        if prefix in path:
            return ttl
    return DEFAULT_TTL


def cache_key(path, query):
    pairs = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    pairs = [(k, v) for k, v in pairs if k != "_fresh"]
    canon = path + "?" + urllib.parse.urlencode(pairs)
    return hashlib.sha1(canon.encode("utf-8")).hexdigest(), canon


def read_cache(key):
    try:
        with open(os.path.join(CACHE_DIR, key + ".json"), "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_cache(key, canon, status, body_text):
    os.makedirs(CACHE_DIR, exist_ok=True)
    entry = {"url": canon, "fetched_at": time.time(), "status": status, "body": body_text}
    tmp = os.path.join(CACHE_DIR, key + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(entry, f, ensure_ascii=False)
    os.replace(tmp, os.path.join(CACHE_DIR, key + ".json"))


def http(method, url, body=None, timeout=45):
    data = None
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8"), {}
    except urllib.error.HTTPError as e:
        extra = {}
        ra = e.headers.get("Retry-After") if e.headers else None
        if ra:
            extra["Retry-After"] = ra
        return e.code, e.read().decode("utf-8", "replace"), extra


def api_get(path, params=None, fresh=False):
    """Appel API avec cache disque, utilisé par la synchro côté serveur."""
    query = urllib.parse.urlencode(params or {})
    key, canon = cache_key(path, query)
    entry = read_cache(key)
    if entry and not fresh and time.time() - entry["fetched_at"] < ttl_for(path):
        return entry["status"], json.loads(entry["body"])
    status, body, _ = http("GET", API_BASE + canon)
    if status == 200:
        write_cache(key, canon, status, body)
        return 200, json.loads(body)
    if entry and status in (429, 500, 502, 503, 504):
        return entry["status"], json.loads(entry["body"])
    return status, None



def load_save():
    try:
        with open(SAVE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def merge_saves(disk, incoming):
    """Fusion : rien de ce qui est déjà sur le disque n'est perdu côté historique."""
    if not disk:
        return incoming
    out = dict(incoming)
    if (disk.get("account") or {}).get("id") == (incoming.get("account") or {}).get("id"):
        m = dict(disk.get("matches") or {})
        for k, v in (incoming.get("matches") or {}).items():
            old = m.get(k) or {}
            m[k] = {**old, **v} if len(v) >= len(old) else {**v, **old}
        out["matches"] = m
    notes = dict(disk.get("notes") or {})
    notes.update(incoming.get("notes") or {})
    out["notes"] = {k: v for k, v in notes.items() if v}
    return out


def write_save(obj, merge=True):
    os.makedirs(DATA, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)
    with _save_lock:
        disk = load_save()
        if merge:
            obj = merge_saves(disk, obj)
        if disk is not None:
            stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
            shutil.copy2(SAVE_FILE, os.path.join(BACKUP_DIR, f"save-{stamp}.json"))
            for old in sorted(os.listdir(BACKUP_DIR))[:-MAX_BACKUPS]:
                try:
                    os.remove(os.path.join(BACKUP_DIR, old))
                except OSError:
                    pass
        tmp = SAVE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=1)
        os.replace(tmp, SAVE_FILE)
        STATUS["save_mtime"] = os.path.getmtime(SAVE_FILE)
    return obj


def cache_stats():
    n, size = 0, 0
    if os.path.isdir(CACHE_DIR):
        for name in os.listdir(CACHE_DIR):
            if name.endswith(".json"):
                n += 1
                size += os.path.getsize(os.path.join(CACHE_DIR, name))
    return {"files": n, "bytes": size, "dir": CACHE_DIR}



def steam_root():
    cands = []
    if STEAM_OVERRIDE:
        cands.append(STEAM_OVERRIDE)
    if sys.platform.startswith("win"):
        try:
            import winreg
            for hive, sub, name in ((winreg.HKEY_CURRENT_USER, r"Software\Valve\Steam", "SteamPath"),
                                    (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\Valve\Steam", "InstallPath"),
                                    (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Valve\Steam", "InstallPath")):
                try:
                    with winreg.OpenKey(hive, sub) as k:
                        cands.append(winreg.QueryValueEx(k, name)[0])
                except OSError:
                    pass
        except ImportError:
            pass
        for env in ("ProgramFiles(x86)", "ProgramFiles"):
            if os.environ.get(env):
                cands.append(os.path.join(os.environ[env], "Steam"))
        cands.append(r"C:\Steam")
    elif sys.platform == "darwin":
        cands.append(os.path.expanduser("~/Library/Application Support/Steam"))
    else:
        cands += [os.path.expanduser(p) for p in (
            "~/.steam/steam", "~/.local/share/Steam", "~/.steam/root",
            "~/.var/app/com.valvesoftware.Steam/.local/share/Steam", "~/snap/steam/common/.local/share/Steam")]
    for c in cands:
        if c and os.path.isdir(os.path.join(c, "config")):
            return os.path.normpath(c)
    return None


def steam_accounts():
    root = steam_root()
    if not root:
        return {"steam_path": None, "accounts": []}
    path = os.path.join(root, "config", "loginusers.vdf")
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            txt = f.read()
    except OSError:
        return {"steam_path": root, "accounts": []}
    accounts = []
    for m in re.finditer(r'"(7656119\d{10})"\s*\{(.*?)\}', txt, re.S):
        props = dict(re.findall(r'"([^"]+)"\s+"([^"]*)"', m.group(2)))
        sid = int(m.group(1))
        accounts.append({
            "steamid64": str(sid), "account_id": sid - STEAM64_BASE,
            "persona": props.get("PersonaName", ""),
            "most_recent": props.get("MostRecent") == "1", "timestamp": int(props.get("Timestamp", "0") or 0),
        })
    accounts.sort(key=lambda a: (not a["most_recent"], -a["timestamp"]))
    return {"steam_path": root, "accounts": accounts}


URL_RE = re.compile(rb"replay(\d+)\.valve\.net(/[^\s'\"\x00\r\n]*)")


def parse_replay_url(cluster, path):
    path = path.split("?", 1)[0]
    if DEADLOCK_APP_ID not in path:
        return None
    name = path.rsplit("/", 1)[-1]
    for suffix, kind in ((".meta.bz2", "metadata_salt"), (".dem.bz2", "replay_salt")):
        if name.endswith(suffix):
            mid, _, salt = name[: -len(suffix)].partition("_")
            if mid.isdigit() and salt.isdigit():
                return {"match_id": int(mid), "cluster_id": int(cluster), kind: int(salt)}
    return None


def load_local():
    try:
        with open(LOCAL_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError):
        d = {}
    d.setdefault("files", {})
    d.setdefault("matches", {})
    return d


def save_local(d):
    os.makedirs(DATA, exist_ok=True)
    tmp = LOCAL_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f)
    os.replace(tmp, LOCAL_FILE)


def scan_steam_cache():
    """Parcourt appcache/httpcache et retient les parties Deadlock trouvées."""
    root = steam_root()
    if not root:
        return {"ok": False, "error": "Steam introuvable sur ce PC. Relance avec --steam \"chemin\\vers\\Steam\".",
                "steam_path": None, "count": 0}
    cache_dir = os.path.join(root, "appcache", "httpcache")
    if not os.path.isdir(cache_dir):
        return {"ok": False, "error": "Le dossier appcache/httpcache de Steam est introuvable.", "steam_path": root, "count": 0}
    with _local_lock:
        local = load_local()
        files = local["files"]
        seen_now = set()
        new = 0
        for dirpath, _dirs, names in os.walk(cache_dir):
            for n in names:
                fp = os.path.join(dirpath, n)
                seen_now.add(fp)
                try:
                    mt = os.path.getmtime(fp)
                except OSError:
                    continue
                prev = files.get(fp)
                if prev and prev[0] == mt:
                    continue
                found = None
                try:
                    with open(fp, "rb") as f:
                        head = f.read(400)
                    for m in URL_RE.finditer(head):
                        r = parse_replay_url(m.group(1).decode(), m.group(2).decode("utf-8", "ignore"))
                        if r:
                            found = r
                            break
                except OSError:
                    pass
                files[fp] = [mt, found["match_id"] if found else None]
                if found:
                    key = str(found["match_id"])
                    e = local["matches"].get(key) or {"seen": time.time(), "ingested": 0}
                    if key not in local["matches"]:
                        new += 1
                    for k in ("cluster_id", "metadata_salt", "replay_salt"):
                        if found.get(k) is not None:
                            e[k] = found[k]
                    local["matches"][key] = e
        for fp in list(files):
            if fp not in seen_now:
                del files[fp]
        save_local(local)
        total = len(local["matches"])
        with_meta = sum(1 for e in local["matches"].values() if e.get("metadata_salt"))
    return {"ok": True, "steam_path": root, "cache_dir": cache_dir, "count": total, "with_metadata": with_meta, "new": new}


def ingest_salts():
    """Envoie à l'API les identifiants de parties trouvés dans le cache Steam."""
    scan = scan_steam_cache()
    if not scan.get("ok"):
        STATUS["last_ingest_result"] = scan
        return scan
    with _local_lock:
        local = load_local()
        todo = []
        for mid, e in local["matches"].items():
            want_meta = e.get("metadata_salt") and not (e.get("ingested", 0) & 1)
            want_rep = e.get("replay_salt") and not (e.get("ingested", 0) & 2)
            if want_meta or want_rep:
                todo.append((mid, {"match_id": int(mid), "cluster_id": e.get("cluster_id"),
                                   "metadata_salt": e.get("metadata_salt"), "replay_salt": e.get("replay_salt"),
                                   "username": "deadlock-tracker"}))
    sent, errors = 0, []
    for i in range(0, len(todo), 50):
        chunk = todo[i:i + 50]
        status, body, _ = http("POST", API_BASE + "/v1/matches/salts", [c[1] for c in chunk], timeout=60)
        if 200 <= status < 300:
            sent += len(chunk)
            with _local_lock:
                local = load_local()
                for mid, s in chunk:
                    e = local["matches"].get(mid, {})
                    e["ingested"] = e.get("ingested", 0) | (1 if s["metadata_salt"] else 0) | (2 if s["replay_salt"] else 0)
                    local["matches"][mid] = e
                save_local(local)
        else:
            errors.append("HTTP %d %s" % (status, (body or "")[:120]))
            break
    res = {"ok": not errors, "found": scan["count"], "sent": sent, "pending": len(todo) - sent,
           "errors": errors, "steam_path": scan.get("steam_path")}
    STATUS["last_ingest"] = time.time()
    STATUS["last_ingest_result"] = res
    return res


def entry_from_metadata(meta, account_id):
    info = meta.get("match_info") or meta
    me = next((p for p in info.get("players") or [] if p.get("account_id") == account_id), None)
    if not me:
        return None
    return {
        "match_id": info.get("match_id"), "hero_id": me.get("hero_id"), "start_time": info.get("start_time"),
        "match_duration_s": info.get("duration_s"), "match_result": info.get("winning_team", 0),
        "player_team": me.get("team", 0), "player_kills": me.get("kills", 0), "player_deaths": me.get("deaths", 0),
        "player_assists": me.get("assists", 0), "net_worth": me.get("net_worth", 0), "last_hits": me.get("last_hits", 0),
        "denies": me.get("denies", 0), "hero_level": me.get("level"), "match_mode": info.get("match_mode"),
        "game_mode": info.get("game_mode"), "player_match_outcome": me.get("player_match_outcome"),
        "source": "steam-cache",
        **({"brawl_score_team0": info["team_score"][0], "brawl_score_team1": info["team_score"][1]}
           if info.get("game_mode") == 4 and len(info.get("team_score") or []) >= 2 else {}),
    }


COMPACT_KEYS = ["match_id", "hero_id", "start_time", "match_duration_s", "match_result", "player_team", "player_kills",
                "player_deaths", "player_assists", "net_worth", "last_hits", "denies", "hero_level", "match_mode",
                "game_mode", "player_match_outcome", "ranked_delta", "ranked_display_badge", "abandoned_time_s", "team_abandoned",
                "brawl_score_team0", "brawl_score_team1"]


def run_sync(reason="manuel"):
    """Historique API + parties du cache Steam -> save.json."""
    if not _sync_lock.acquire(blocking=False):
        return {"ok": False, "error": "Une synchronisation est déjà en cours."}
    STATUS["sync_running"] = True
    try:
        save = load_save() or {}
        settings = save.get("settings") or {}
        acct = (save.get("account") or {}).get("id")
        res = {"ok": True, "reason": reason, "api_added": 0, "local_added": 0, "local_pending": 0,
               "api_status": None, "ingest": None, "errors": []}
        if settings.get("autoIngest"):
            res["ingest"] = ingest_salts()
        if not acct:
            res["errors"].append("Aucun compte connecté.")
            return res
        added = {}
        status, hist = api_get("/v1/players/%d/match-history" % acct, fresh=(reason != "auto"))
        res["api_status"] = status
        if status == 200 and isinstance(hist, list):
            for m in hist:
                added[str(m["match_id"])] = {k: m[k] for k in COMPACT_KEYS if k in m}
        elif status == 403:
            res["errors"].append("Ce compte est protégé côté API (403).")
        if settings.get("useSteamCache", True) and steam_root():
            scan_steam_cache()
            with _local_lock:
                local = load_local()
            known = set((save.get("matches") or {}).keys()) | set(added.keys())
            for mid, e in sorted(local["matches"].items(), key=lambda kv: -int(kv[0]))[:60]:
                if mid in known or e.get("not_mine") or e.get("meta_fail", 0) >= 30:
                    continue
                st, meta = api_get("/v1/matches/%s/metadata" % mid)
                if st == 200 and meta:
                    ent = entry_from_metadata(meta, acct)
                    with _local_lock:
                        local2 = load_local()
                        le = local2["matches"].get(mid, {})
                        if ent:
                            added[mid] = ent
                            res["local_added"] += 1
                        else:
                            le["not_mine"] = True
                        local2["matches"][mid] = le
                        save_local(local2)
                else:
                    res["local_pending"] += 1
                    with _local_lock:
                        local2 = load_local()
                        le = local2["matches"].get(mid, {})
                        le["meta_fail"] = le.get("meta_fail", 0) + 1
                        local2["matches"][mid] = le
                        save_local(local2)
        before = set((save.get("matches") or {}).keys())
        res["api_added"] = len([k for k in added if k not in before]) - res["local_added"]
        if added:
            save.setdefault("matches", {})
            save = dict(save)
            save["matches"] = {**save["matches"], **{k: {**save["matches"].get(k, {}), **v} for k, v in added.items()}}
            save["updated"] = int(time.time())
            write_save(save, merge=True)
        res["total"] = len((load_save() or {}).get("matches") or {})
        return res
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        STATUS["sync_running"] = False
        STATUS["last_sync"] = time.time()
        _sync_lock.release()


def sync_loop():
    time.sleep(20)
    while True:
        save = load_save() or {}
        if (save.get("settings") or {}).get("autoSync"):
            STATUS["last_sync_result"] = run_sync("auto")
        STATUS["next_sync"] = time.time() + SYNC_INTERVAL_S
        time.sleep(SYNC_INTERVAL_S)


def local_summary():
    with _local_lock:
        local = load_local()
    ms = local["matches"]
    return {
        "count": len(ms),
        "ingested": sum(1 for e in ms.values() if e.get("ingested")),
        "mine_unknown": sum(1 for e in ms.values() if not e.get("not_mine")),
        "not_mine": sum(1 for e in ms.values() if e.get("not_mine")),
        "match_ids": sorted((int(k) for k in ms), reverse=True)[:200],
    }


class Handler(BaseHTTPRequestHandler):
    server_version = "DeadlockTracker/2.0"

    def log_message(self, fmt, *args):
        msg = fmt % args
        if ("/api/" in msg and "/assets/" not in msg) or " 5" in msg[-6:]:
            sys.stderr.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), msg))

    def _send(self, status, body, ctype="application/json; charset=utf-8", headers=None):
        data = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _json(self, status, obj, headers=None):
        self._send(status, json.dumps(obj, ensure_ascii=False), headers=headers)

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = parsed.path
        if path in ("/", "/index.html"):
            try:
                with open(INDEX_FILE, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except OSError:
                self._send(404, "index.html introuvable a cote du script", "text/plain; charset=utf-8")
            return
        if path == "/local/ping":
            self._json(200, {"ok": True, "version": 4, "cache": cache_stats(), "save": os.path.exists(SAVE_FILE)})
            return
        if path == "/local/save":
            self._json(200, {"save": load_save(), "mtime": STATUS["save_mtime"]})
            return
        if path == "/local/status":
            self._json(200, {**{k: v for k, v in STATUS.items()}, "interval_s": SYNC_INTERVAL_S,
                             "steam_path": steam_root(), "local": local_summary()})
            return
        if path == "/local/steam/accounts":
            self._json(200, steam_accounts())
            return
        if path.startswith("/api/"):
            self._proxy(path[4:], parsed.query)
            return
        self._send(404, "introuvable", "text/plain; charset=utf-8")

    def do_POST(self):
        path = urllib.parse.urlsplit(self.path).path
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        if path == "/local/save":
            try:
                obj = json.loads(raw.decode("utf-8"))
            except ValueError:
                self._json(400, {"error": "JSON invalide"})
                return
            merged = write_save(obj)
            self._json(200, {"ok": True, "saved_at": time.time(), "mtime": STATUS["save_mtime"],
                             "matches": len(merged.get("matches") or {})})
            return
        if path == "/local/cache/clear":
            removed = 0
            if os.path.isdir(CACHE_DIR):
                for name in os.listdir(CACHE_DIR):
                    try:
                        os.remove(os.path.join(CACHE_DIR, name))
                        removed += 1
                    except OSError:
                        pass
            self._json(200, {"ok": True, "removed": removed})
            return
        if path == "/local/steam/scan":
            self._json(200, scan_steam_cache())
            return
        if path == "/local/ingest":
            self._json(200, ingest_salts())
            return
        if path == "/local/sync":
            res = run_sync("manuel")
            STATUS["last_sync_result"] = res
            self._json(200, res)
            return
        self._json(404, {"error": "introuvable"})

    def _proxy(self, path, query):
        if not (path.startswith("/v1/") or path.startswith("/v2/")):
            self._json(400, {"error": "chemin API non autorise"})
            return
        fresh = "_fresh=1" in query
        key, canon = cache_key(path, query)
        entry = read_cache(key)
        now = time.time()
        if entry and not fresh and now - entry["fetched_at"] < ttl_for(path):
            self._send(entry["status"], entry["body"], headers={
                "X-Cache": "HIT", "X-Fetched-At": str(int(entry["fetched_at"]))})
            return
        with _inflight_lock:
            ev = _inflight.get(key)
            owner = ev is None
            if owner:
                ev = _inflight[key] = threading.Event()
        if not owner:
            ev.wait(90)
            entry2 = read_cache(key)
            if entry2 and entry2["fetched_at"] >= now - 1:
                self._send(entry2["status"], entry2["body"], headers={
                    "X-Cache": "HIT", "X-Fetched-At": str(int(entry2["fetched_at"]))})
                return
        try:
            try:
                status, body, extra = http("GET", API_BASE + canon, timeout=90)
            except Exception as exc:
                if entry:
                    self._send(entry["status"], entry["body"], headers={
                        "X-Cache": "STALE", "X-Fetched-At": str(int(entry["fetched_at"]))})
                else:
                    self._json(502, {"error": "API injoignable : %s" % exc})
                return
            if status == 200:
                write_cache(key, canon, status, body)
                self._send(200, body, headers={"X-Cache": "MISS", "X-Fetched-At": str(int(time.time()))})
            elif entry and status >= 429:
                h = {"X-Cache": "STALE", "X-Fetched-At": str(int(entry["fetched_at"])), "X-Upstream-Status": str(status)}
                h.update(extra)
                self._send(entry["status"], entry["body"], headers=h)
            else:
                self._send(status, body or json.dumps({"error": "HTTP %d" % status}), headers=extra)
        finally:
            if owner:
                with _inflight_lock:
                    _inflight.pop(key, None)
                ev.set()


def main():
    global STEAM_OVERRIDE
    ap = argparse.ArgumentParser(description="Deadlock Tracker - serveur local")
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--no-browser", action="store_true")
    ap.add_argument("--steam", help="Dossier d'installation de Steam, si non détecté")
    args = ap.parse_args()
    STEAM_OVERRIDE = args.steam

    os.makedirs(CACHE_DIR, exist_ok=True)
    if os.path.exists(SAVE_FILE):
        STATUS["save_mtime"] = os.path.getmtime(SAVE_FILE)
    try:
        srv = ThreadingHTTPServer((args.host, args.port), Handler)
    except OSError:
        print("Le port %d est deja utilise : le tracker tourne peut-etre deja." % args.port)
        print("Ouvre http://localhost:%d/ ou relance avec --port 8788." % args.port)
        if not args.no_browser:
            webbrowser.open("http://localhost:%d/" % args.port)
        return
    threading.Thread(target=sync_loop, daemon=True).start()
    url = "http://localhost:%d/" % args.port
    print("Deadlock Tracker en marche sur", url)
    print("Steam :", steam_root() or "non trouve (option --steam)")
    print("Sauvegarde :", SAVE_FILE)
    print("Laisse cette fenetre ouverte. Ctrl+C pour arreter.")
    if not args.no_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nArret.")
        srv.server_close()


if __name__ == "__main__":
    main()
