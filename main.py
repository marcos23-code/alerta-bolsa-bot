import time
import os
import json
import threading
import telebot
import requests
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]

ARCHIVO_GRUPOS    = "grupos.json"
ARCHIVO_HISTORIAL = "historial.json"
MAX_HISTORIAL     = 500       # máximo de entradas guardadas
MAX_CALLS_POR_MINUTO = 58   # máximo seguro con Finnhub gratis
PAUSA_ENTRE_CICLOS = 12     # más rápido
# ─────────────────────────────────────────────
# GRUPOS DE EMPRESAS
# Cada worker escanea su grupo de forma independiente.
# El rate limiter global asegura que entre todos no superan 55 calls/min.
# ─────────────────────────────────────────────
GRUPOS_DEFAULT = {
    "BIOTECH_FDA": [
        # Ampliada con más empresas de alto catalizador (FDA, Fase 2/3, etc.)
        "ACRX", "ADMA", "ADTX", "AGRX", "ALDX", "ALVR", "AMPIO", "ANAB",
        "APDN", "ARDX", "ARQT", "ATXI", "AVDL", "AVXL", "AXSM", "BCRX",
        "BLUE", "BNGO", "BPMC", "BTAI", "CADX", "CBAY", "CERS", "CHMA",
        "CLVS", "CMPS", "CPRX", "CRSP", "CYTH", "DVAX", "EDSA", "ENLV",
        "ETNB", "EVAX", "FOLD", "FREQ", "GALT", "GOSS", "GTHX", "HRTX",
        "IDYA", "IMAB", "IMGN", "IMMP", "IMMU", "IMVT", "INVA", "IPSC",
        "ISEE", "JANX", "KDNY", "KPTI", "KRTX", "LGND", "LNTH", "LPCN",
        "LQDA", "LYRA", "MDGL", "MDXG", "MGTA", "MGNX", "MIRM", "MNKD",
        "MORF", "MYOV", "NBIX", "NCNA", "NKTR", "NRBO", "NTLA", "NVAX",
        "NVCR", "OCGN", "ONTX", "OPNT", "OTIC", "PAVM", "PCVX", "PLRX",
        "PMVP", "PRAX", "PRVA", "PRTK", "PTGX", "RCKT", "RGEN", "RKDA",
        "RLAY", "RNAC", "RPTX", "RXDX", "RYTM", "SAGE", "SEER", "SENS",
        "SGMO", "SRPT", "STRO", "SVRA", "SYRS", "TGTX", "TNXP", "TRIL",
        "TUNE", "TVTX", "TYME", "TYRA", "URGN", "VBIV", "VCNX", "VKTX",
        "VNDA", "VXRT", "XBIT", "XENE", "XNCR", "XLRN", "YMAB", "ZLAB",
        "ZYME", "AGEN", "AGIO", "ACAD", "AUPH", "AKBA", "ALKS", "AMRN",
        "ARWR", "ASND", "ATRC", "BCYC", "BDTX", "BNTX", "CAPR", "CCXI",
        "CERE", "CGEN", "COGT", "DCPH", "DRTX", "DYNE", "EDIT", "FATE",
        "FIXX", "FMTX", "FUSN", "GLYC", "HIMS", "HRMY", "IBRX", "IMCR",
        "INFI", "INSM", "IOVA", "ITCI", "JNCE", "KALA", "KEZR", "KMDA",
        "KROS", "KYMR", "AMRX", "CCCC", "CNST", "ELOX", "GMAB", "ICAD",
        "ITOS", "LIFE", "QNRX", "NRXP", "VTGN", "PRQR", "CLYM", "GUTS",
        "OSTX", "LRMR", "SNGX", "BDRX", "TTOO", "LGVN", "KTRA", "UNCY",
        "OTLK", "SLS", "IOBT", "TLSA", "CABA", "CDTX", "CGEM", "CNCE",
        "PEGY", "HLTH", "HOOK", "HGEN", "ANIP", "ICCM", "LHCG", "MRTX",
        "VSTM", "CRBP", "SPPI", "ATOS", "CLRB", "LCTX", "ONTX", "SAVA",
        "SNDX", "KOD", "TGTX", "MGNX", "ADCT", "MGTA", "RIGL", "SCPH",
        "VRCA", "ARDS", "CLSD", "LPTX", "MBRX", "NERV", "OVID", "PDSB",
        "RVPH", "SGBX", "SONN", "TFFP", "VIRI", "WINT", "XCUR", "ACHV",
        "CLGN", "GNLX", "LGVN", "SNGX", "TNXP", "VRAX", "BDRX", "ATNF",
        "BRTX", "CLNN", "DRMA", "ENOB", "FOXO", "GCTK", "HSDT", "LUCY",
        "MYNZ", "NUWE", "ONCR", "PHIO", "PTIX", "QLI", "RSLS", "SINT",
        "SONN", "STSS", "SXTP", "TIVC", "VAPO", "WINT", "XLO", "ATOS",
        "SAVA", "VSTM", "CRBP", "SPPI", "CLRB", "LCTX", "ONTX", "SNDX",
        "KOD", "TGTX", "MGNX", "ADCT", "MGTA", "RIGL", "SCPH", "VRCA",
        "ARDS", "CLSD", "LPTX", "MBRX", "NERV", "OVID", "PDSB", "RVPH",
    ],
    "TECH_GROWTH": [
        "ACMR", "APPS", "ASTS", "BBAI", "BFLY", "BIGC", "BLZE", "BRZE",
        "CFLT", "CLBT", "CODA", "CRCT", "CWAN", "DNMR", "DUOL", "ENVX",
        "ESTC", "FROG", "GETY", "GTLB", "HOLO", "IONQ", "IRBT", "JOBY",
        "KVYO", "MAPS", "MNDY", "MNTV", "MRIN", "OSCR", "OUST", "PRPL",
        "PSFE", "QUBT", "RKLB", "SEMR", "SMMT", "SPCE", "STEM", "TASK",
        "TOST", "UPST", "XPOF", "ZI", "ASAN", "AVPT", "DOMO", "SUMO",
        "PUBM", "MGNI", "NCNO", "ALRM", "SQSP", "SPT", "ALKT", "TMDX",
        "NTNX", "CRDO", "PERI", "LSPD", "DDOG", "SNOW", "PLTR", "RBLX",
        "DKNG", "PENN", "SKLZ", "AMBA", "FORM", "ERII", "MRAM", "WEAV",
        "GKOS", "SILK", "SWAV", "TELA", "AXNX", "NSTG", "CTSO", "LIVN",
        "PSTV", "CEMI", "BHVN", "DAVE", "OPEN", "LMND", "SOFI", "AFRM",
        "HOOD", "CLOV", "ACHR", "VSCO", "BKKT", "TPST", "PRCT", "AIOT",
        "SOUN", "RGTI", "QBTS", "AISP", "PEGY", "LAZR", "QS", "VLN",
        "MVIS", "LUNR", "SERV", "BBAI", "RKLB", "SPCE", "JOBY", "ASTS",
    ],
    "EV_SPACE_CRYPTO": [
        "CHPT", "BLNK", "GOEV", "SOLO", "AYRO", "IDEX", "FSR", "MULN",
        "LCID", "RIVN", "FFIE", "REE", "HYZN", "FREY", "ZEV", "EVGO",
        "PTRA", "NKLA", "WKHS", "HYLN", "ARVL", "HYZN", "ASTR", "MNTS",
        "IRDM", "KTOS", "AVAV", "MRCY", "MAXN", "MSTR", "MARA", "RIOT",
        "HUT", "BTBT", "CLSK", "COIN", "CAN", "BTDR", "CIFR", "WULF",
        "IREN", "CORZ", "SMCI", "NVDA", "AMD", "INTC", "MRVL", "QCOM",
    ],
    "FINTECH_MEME_INTL": [
        "MQ", "STEP", "RMBS", "TPVG", "PFLT", "GLAD", "HTGC", "GAIN",
        "CURO", "TREE", "GCMG", "PSFE", "CGC", "ACB", "CRON", "SNDL",
        "OGI", "TLRY", "FLGC", "NIO", "XPEV", "LI", "BIDU", "JD", "GRAB",
        "SE", "FUTU", "MELI", "GLOB", "VTEX", "STNE", "PAGS", "ARCO",
        "LOMA", "VIST", "BIOX", "CAAP", "GME", "AMC", "EXPR", "PROG",
        "SPRT", "ATER", "BBIG", "OPAD", "SDC", "BBBY", "IRNT", "OPEN",
        "CLOV", "SKLZ", "HOOD", "SPCE", "WKHS", "NKLA", "HYLN", "GOEV",
        "CHPT",
    ],
    # "CUSTOM": [] # stocks añadidos por el usuario con /agregar

# ─────────────────────────────────────────────
# FRASES DE ALTO IMPACTO
# ─────────────────────────────────────────────
PALABRAS_ALTO_IMPACTO = [
    "fda approval", "fda approves", "fda clears", "fda grants approval",
    "breakthrough therapy designation", "fast track designation",
    "priority review granted", "pdufa", "nda approval", "bla approval",
    "accelerated approval",
    "phase 3 success", "phase 3 positive", "positive pivotal",
    "pivotal trial success", "met primary endpoint", "met all primary",
    "statistically significant", "superior to placebo",
    "phase 2 positive", "positive phase 2",
    "earnings beat", "beat expectations", "record revenue", "record earnings",
    "revenue beat", "raised guidance", "raised full-year", "blowout quarter",
    "record quarterly", "exceeded estimates",
    "major contract", "billion-dollar", "multi-billion", "massive order",
    "strategic partnership", "licensing agreement", "exclusive license",
    "landmark deal", "definitive agreement",
    "acquisition agreement", "merger agreement", "buyout offer",
    "takeover bid", "going private", "strategic alternatives",
    "letter of intent",
]

# ─────────────────────────────────────────────
# RATE LIMITER GLOBAL (token bucket compartido)
# ─────────────────────────────────────────────
class RateLimiter:
    """Permite MAX_CALLS_POR_MINUTO llamadas/min en total entre todos los workers."""
    def __init__(self, calls_per_minute):
        self._interval = 60.0 / calls_per_minute
        self._last = 0.0
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            now = time.time()
            wait = self._interval - (now - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.time()

rate_limiter = RateLimiter(MAX_CALLS_POR_MINUTO)

# ─────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────
bot = telebot.TeleBot(TELEGRAM_TOKEN)
lock = threading.Lock()

noticias_vistas = set()
alertas_hoy = 0
fecha_alertas = datetime.now().strftime("%Y-%m-%d")

# Stats por grupo
stats_grupos = {
    nombre: {"ultimo_ciclo": None, "duracion": None, "alertas": 0, "ciclos": 0}
    for nombre in GRUPOS_DEFAULT
}

# ─────────────────────────────────────────────
# PERSISTENCIA
# ─────────────────────────────────────────────
def cargar_grupos():
    if os.path.exists(ARCHIVO_GRUPOS):
        try:
            with open(ARCHIVO_GRUPOS, "r") as f:
                datos = json.load(f)
            # Si es formato viejo (lista plana), migrar a CUSTOM
            if isinstance(datos, list):
                print(f"Migrando lista antigua ({len(datos)} tickers) → grupo CUSTOM")
                grupos = {k: list(v) for k, v in GRUPOS_DEFAULT.items()}
                grupos["CUSTOM"] = datos
                return grupos
            # Completar grupos faltantes con defaults
            grupos = {k: list(v) for k, v in GRUPOS_DEFAULT.items()}
            for nombre, tickers in datos.items():
                grupos[nombre] = tickers
            print(f"Grupos cargados: { {k: len(v) for k, v in grupos.items()} }")
            return grupos
        except Exception as e:
            print(f"⚠️  Error leyendo {ARCHIVO_GRUPOS}: {e}. Usando defaults.")
    return {k: list(v) for k, v in GRUPOS_DEFAULT.items()}

def guardar_grupos():
    try:
        with open(ARCHIVO_GRUPOS, "w") as f:
            json.dump(grupos, f)
    except Exception as e:
        print(f"⚠️  Error guardando grupos: {e}")

grupos = cargar_grupos()

# ─────────────────────────────────────────────
# HISTORIAL DE ALERTAS
# ─────────────────────────────────────────────
def guardar_en_historial(entrada: dict):
    """Añade una entrada al historial. Mantiene como máximo MAX_HISTORIAL."""
    try:
        if os.path.exists(ARCHIVO_HISTORIAL):
            with open(ARCHIVO_HISTORIAL, "r") as f:
                datos = json.load(f)
        else:
            datos = []
        datos.append(entrada)
        if len(datos) > MAX_HISTORIAL:
            datos = datos[-MAX_HISTORIAL:]
        with open(ARCHIVO_HISTORIAL, "w") as f:
            json.dump(datos, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[HISTORIAL] Error guardando: {e}")

def leer_historial(n=10):
    """Devuelve las últimas n entradas del historial."""
    try:
        if not os.path.exists(ARCHIVO_HISTORIAL):
            return []
        with open(ARCHIVO_HISTORIAL, "r") as f:
            datos = json.load(f)
        return datos[-n:]
    except Exception as e:
        print(f"[HISTORIAL] Error leyendo: {e}")
        return []

# Eliminar duplicados dentro de cada grupo
for _g in grupos:
    _seen = set()
    grupos[_g] = [x for x in grupos[_g] if not (x in _seen or _seen.add(x))]

def total_empresas():
    return sum(len(v) for v in grupos.values())

# ─────────────────────────────────────────────
# DETECCIÓN DE NOTICIAS
# ─────────────────────────────────────────────
def es_noticia_alto_impacto(texto):
    texto = texto.lower()
    return any(frase in texto for frase in PALABRAS_ALTO_IMPACTO)

# Rangos basados en estudios históricos de catalizadores similares
# (conservadores — el rango real puede ser mayor o menor)
# Cada regla: (keywords, rango, plazo, descripcion)
# Rangos y plazos basados en histórico de small-caps en eventos similares
_REGLAS_POTENCIAL = [
    # ── FDA completa / aprobaciones regulatorias ──────────────────────
    (["fda approves", "fda clears", "nda approval", "bla approval",
      "accelerated approval"],
     "+30% a +120%", "1-3 días (reacción inmediata al anuncio)",
     "Aprobación FDA — históricamente el mayor catalizador binario"),

    (["fda approval", "fda grants approval"],
     "+25% a +100%", "1-5 días",
     "Aprobación FDA en small-cap (alta volatilidad inicial)"),

    (["breakthrough therapy designation"],
     "+10% a +40%", "1-7 días",
     "Breakthrough Therapy acelera revisión FDA"),

    (["priority review granted"],
     "+10% a +35%", "2-7 días",
     "Priority Review recorta tiempo de aprobación a ~6 meses"),

    (["fast track designation"],
     "+5% a +25%", "3-10 días",
     "Fast Track — señal regulatoria positiva temprana"),

    (["pdufa"],
     "+5% a +30%", "1-4 semanas (hasta la fecha PDUFA)",
     "Anticipación a decisión FDA programada"),

    # ── Ensayos clínicos ──────────────────────────────────────────────
    (["phase 3 success", "phase 3 positive", "pivotal trial success",
      "positive pivotal", "met all primary"],
     "+40% a +180%", "1-3 días (pico en apertura de mercado)",
     "Fase 3 exitosa — evento binario de mayor impacto en biotech"),

    (["met primary endpoint", "statistically significant",
      "superior to placebo"],
     "+20% a +80%", "1-5 días",
     "Endpoint primario alcanzado — alta probabilidad de aprobación"),

    (["phase 2 positive", "positive phase 2"],
     "+15% a +60%", "3-10 días",
     "Fase 2 positiva — señal temprana, abre camino a Fase 3"),

    # ── Ganancias / ingresos ──────────────────────────────────────────
    (["record revenue", "record earnings", "blowout quarter",
      "record quarterly"],
     "+10% a +30%", "1-5 días (post-earnings drift)",
     "Resultados récord — suele extenderse varios días"),

    (["earnings beat", "beat expectations", "revenue beat",
      "exceeded estimates"],
     "+5% a +20%", "1-3 días",
     "Beat de estimaciones de Wall Street"),

    (["raised guidance", "raised full-year"],
     "+5% a +15%", "3-10 días",
     "Mejora de guidance — efecto más gradual que el earnings beat"),

    # ── M&A / Adquisiciones ───────────────────────────────────────────
    (["acquisition agreement", "merger agreement", "buyout offer",
      "takeover bid", "going private"],
     "+20% a +60%", "1-2 días (sube al precio de oferta)",
     "M&A o OPA — prima típica 30-50% sobre precio previo"),

    (["strategic alternatives", "letter of intent"],
     "+10% a +40%", "1-3 semanas (hasta confirmación)",
     "Proceso de venta en curso — alta incertidumbre"),

    # ── Contratos grandes ─────────────────────────────────────────────
    (["billion-dollar", "multi-billion", "massive order"],
     "+8% a +30%", "1-5 días",
     "Contrato billonario — impacto proporcional a la cap de mercado"),

    (["major contract", "strategic partnership", "licensing agreement",
      "exclusive license", "landmark deal", "definitive agreement"],
     "+3% a +20%", "2-7 días",
     "Acuerdo estratégico relevante"),
]

def estimar_potencial(titulo, resumen):
    """Devuelve (rango, plazo, descripcion) basado en el tipo de evento."""
    texto = (titulo + " " + resumen).lower()
    for keywords, rango, plazo, descripcion in _REGLAS_POTENCIAL:
        if any(kw in texto for kw in keywords):
            return rango, plazo, descripcion
    return "+5% a +20%", "3-10 días", "Noticia de impacto positivo"

def escanear_ticker(empresa, hoy):
    """Escanea un ticker. Devuelve lista de alertas a enviar."""
    rate_limiter.acquire()
    try:
        url = (
            f"https://finnhub.io/api/v1/company-news"
            f"?symbol={empresa}&from={hoy}&to={hoy}&token={FINNHUB_API_KEY}"
        )
        resp = requests.get(url, timeout=10)

        if resp.status_code == 429:
            print(f"    ⚠️  Rate limit global. Esperando 65s...")
            time.sleep(65)
            rate_limiter.acquire()
            resp = requests.get(url, timeout=10)

        noticias = resp.json()
        if not isinstance(noticias, list):
            return []

        alertas = []
        for noticia in noticias[:5]:
            noticia_id = str(noticia.get("id", ""))
            with lock:
                if noticia_id and noticia_id in noticias_vistas:
                    continue

            titulo = noticia.get("headline", "")
            resumen = noticia.get("summary", "")

            if es_noticia_alto_impacto(titulo + " " + resumen):
                with lock:
                    if noticia_id:
                        noticias_vistas.add(noticia_id)
                rango, plazo, descripcion = estimar_potencial(titulo, resumen)
                alertas.append({
                    "empresa": empresa,
                    "titulo": titulo,
                    "resumen": resumen[:500] + ("..." if len(resumen) > 500 else ""),
                    "url": noticia.get("url", ""),
                    "rango": rango,
                    "plazo": plazo,
                    "descripcion": descripcion,
                })
        return alertas

    except Exception as e:
        print(f"    ⚠️  {empresa}: {e}")
        return []

# ─────────────────────────────────────────────
# WORKER POR GRUPO
# ─────────────────────────────────────────────
def worker_grupo(nombre_grupo):
    global alertas_hoy, fecha_alertas, noticias_vistas

    emoji = {"BIOTECH_FDA": "🧬", "TECH_GROWTH": "💻",
              "EV_SPACE_CRYPTO": "🚀", "FINTECH_MEME_INTL": "💰",
              "CUSTOM": "⭐"}.get(nombre_grupo, "📊")

    print(f"[{nombre_grupo}] Worker iniciado {emoji}")

    while True:
        hoy = datetime.now().strftime("%Y-%m-%d")

        with lock:
            if hoy != fecha_alertas:
                alertas_hoy = 0
                fecha_alertas = hoy
                noticias_vistas = set()

        lista = list(grupos.get(nombre_grupo, []))
        if not lista:
            time.sleep(60)
            continue

        inicio = time.time()
        alertas_ciclo = 0

        print(f"  {emoji} [{nombre_grupo}] Escaneando {len(lista)} tickers...")

        for empresa in lista:
            for alerta in escanear_ticker(empresa, hoy):
                mensaje = (
                    f"🚨 ALERTA — {emoji} {nombre_grupo}\n"
                    f"Ticker: ${alerta['empresa']}\n\n"
                    f"📰 {alerta['titulo']}\n\n"
                    f"{alerta['resumen']}\n\n"
                    f"📈 Potencial estimado: {alerta['rango']}\n"
                    f"   ⏱️ Plazo estimado: {alerta['plazo']}\n"
                    f"   Basado en: {alerta['descripcion']}\n"
                    f"   ⚠️ Estimación orientativa — siempre gestiona el riesgo\n\n"
                    f"🔗 {alerta['url']}"
                )
                try:
                    bot.send_message(CHAT_ID, mensaje)
                except Exception as e:
                    print(f"    ⚠️  Error Telegram: {e}")

                guardar_en_historial({
                    "tipo":     "noticia",
                    "fecha":    datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "grupo":    nombre_grupo,
                    "ticker":   alerta["empresa"],
                    "titulo":   alerta["titulo"],
                    "rango":    alerta["rango"],
                    "plazo":    alerta["plazo"],
                    "evento":   alerta["descripcion"],
                    "url":      alerta["url"],
                })

                with lock:
                    alertas_hoy += 1
                    stats_grupos[nombre_grupo]["alertas"] += 1
                alertas_ciclo += 1
                print(f"    🚨 ALERTA [{nombre_grupo}]: {alerta['empresa']} — {alerta['titulo'][:60]}")

        duracion = int(time.time() - inicio)
        with lock:
            stats_grupos[nombre_grupo]["ultimo_ciclo"] = datetime.now()
            stats_grupos[nombre_grupo]["duracion"] = duracion
            stats_grupos[nombre_grupo]["ciclos"] += 1

        print(
            f"  {emoji} [{nombre_grupo}] ✅ Ciclo #{stats_grupos[nombre_grupo]['ciclos']} "
            f"completado en {duracion}s. Alertas: {alertas_ciclo}.\n"
        )

        time.sleep(PAUSA_ENTRE_CICLOS)

# ─────────────────────────────────────────────
# COMANDOS TELEGRAM
# ─────────────────────────────────────────────
@bot.message_handler(commands=["agregar"])
def handle_agregar(message):
    partes = message.text.strip().split()
    if len(partes) < 2:
        bot.reply_to(message, "⚠️ Uso: /agregar TICKER\nEjemplo: /agregar NVDA")
        return

    ticker = partes[1].upper()
    todos = [t for g in grupos.values() for t in g]
    if ticker in todos:
        bot.reply_to(message, f"⚠️ {ticker} ya está en la lista.")
        return

    url_check = f"https://finnhub.io/api/v1/quote?symbol={ticker}&token={FINNHUB_API_KEY}"
    try:
        resp = requests.get(url_check, timeout=10).json()
        if resp.get("c", 0) == 0:
            bot.reply_to(message, f"❌ Ticker {ticker} no encontrado.")
            return
    except Exception:
        bot.reply_to(message, f"❌ Error verificando {ticker}.")
        return

    with lock:
        grupos["CUSTOM"].append(ticker)
    guardar_grupos()
    bot.reply_to(message, f"✅ {ticker} agregado al grupo CUSTOM ⭐. Total: {total_empresas()} empresas.")
    print(f"[CUSTOM] {ticker} agregado por /agregar")

@bot.message_handler(commands=["quitar"])
def handle_quitar(message):
    partes = message.text.strip().split()
    if len(partes) < 2:
        bot.reply_to(message, "⚠️ Uso: /quitar TICKER\nEjemplo: /quitar FFIE")
        return

    ticker = partes[1].upper()
    encontrado = False
    with lock:
        for nombre, lista in grupos.items():
            if ticker in lista:
                lista.remove(ticker)
                encontrado = True
                grupo_removido = nombre
                break

    if not encontrado:
        bot.reply_to(message, f"⚠️ {ticker} no está en ningún grupo.")
        return

    guardar_grupos()
    bot.reply_to(message, f"🗑️ {ticker} eliminado del grupo {grupo_removido}. Total: {total_empresas()} empresas.")
    print(f"[{grupo_removido}] {ticker} quitado por /quitar")

@bot.message_handler(commands=["lista"])
def handle_lista(message):
    lineas = [f"📋 Empresas vigiladas ({total_empresas()} total):\n"]
    emojis = {"BIOTECH_FDA": "🧬", "TECH_GROWTH": "💻",
               "EV_SPACE_CRYPTO": "🚀", "FINTECH_MEME_INTL": "💰", "CUSTOM": "⭐"}
    for nombre, lista in grupos.items():
        if not lista:
            continue
        e = emojis.get(nombre, "📊")
        lineas.append(f"\n{e} {nombre} ({len(lista)}):\n")
        lineas.append(" | ".join(sorted(lista)))

    texto = "\n".join(lineas)
    for i in range(0, len(texto), 4000):
        bot.reply_to(message, texto[i:i+4000])

@bot.message_handler(commands=["status"])
def handle_status(message):
    emojis = {"BIOTECH_FDA": "🧬", "TECH_GROWTH": "💻",
               "EV_SPACE_CRYPTO": "🚀", "FINTECH_MEME_INTL": "💰", "CUSTOM": "⭐"}
    lineas = [
        f"📊 Estado del Robot\n",
        f"✅ {len(grupos)} workers paralelos activos",
        f"🔍 {total_empresas()} empresas vigiladas",
        f"🚨 Alertas enviadas hoy: {alertas_hoy}",
        f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n",
        "── Por grupo ──",
    ]
    for nombre, st in stats_grupos.items():
        e = emojis.get(nombre, "📊")
        n = len(grupos.get(nombre, []))
        if st["ultimo_ciclo"] is None:
            lineas.append(f"{e} {nombre} ({n}) — escaneando...")
        else:
            hace = int((datetime.now() - st["ultimo_ciclo"]).total_seconds())
            m, s = hace // 60, hace % 60
            dur = st["duracion"] or 0
            lineas.append(
                f"{e} {nombre} ({n}) — último ciclo: {m}m{s}s atrás "
                f"| duración: {dur}s | alertas: {st['alertas']}"
            )
    bot.reply_to(message, "\n".join(lineas))

@bot.message_handler(commands=["buscar"])
def handle_buscar(message):
    partes = message.text.strip().split()
    if len(partes) < 2:
        bot.reply_to(message, "⚠️ Uso: /buscar TICKER\nEjemplo: /buscar NVDA")
        return

    ticker = partes[1].upper()
    bot.reply_to(message, f"🔍 Buscando noticias de ${ticker} ahora mismo...")
    print(f"[BUSCAR] Escaneo manual: {ticker}")

    hoy = datetime.now().strftime("%Y-%m-%d")

    rate_limiter.acquire()
    try:
        url = (
            f"https://finnhub.io/api/v1/company-news"
            f"?symbol={ticker}&from={hoy}&to={hoy}&token={FINNHUB_API_KEY}"
        )
        resp = requests.get(url, timeout=10)
        noticias = resp.json()

        if not isinstance(noticias, list) or len(noticias) == 0:
            bot.reply_to(message, f"📭 No hay noticias hoy para ${ticker}.")
            return

        # Mostrar las últimas 3 noticias relevantes o cualquier noticia
        enviadas = 0
        for noticia in noticias[:5]:
            titulo = noticia.get("headline", "")
            resumen = noticia.get("summary", "")
            url_noticia = noticia.get("url", "")

            es_impacto = es_noticia_alto_impacto(titulo + " " + resumen)

            if es_impacto:
                rango, plazo, descripcion = estimar_potencial(titulo, resumen)
                potencial_txt = (
                    f"\n📈 Potencial estimado: {rango}\n"
                    f"   ⏱️ Plazo estimado: {plazo}\n"
                    f"   Basado en: {descripcion}\n"
                    f"   ⚠️ Estimación orientativa — gestiona el riesgo"
                )
                prefijo = "🚨 ALTO IMPACTO"
            else:
                potencial_txt = ""
                prefijo = "📰 Noticia"

            resumen_corto = resumen[:400] + ("..." if len(resumen) > 400 else "")
            msg = (
                f"{prefijo} — ${ticker}\n\n"
                f"{titulo}\n\n"
                f"{resumen_corto}"
                f"{potencial_txt}\n\n"
                f"🔗 {url_noticia}"
            )
            bot.reply_to(message, msg)
            enviadas += 1

            if enviadas >= 3:
                break

        if enviadas == 0:
            bot.reply_to(message, f"📭 No encontré noticias de hoy para ${ticker}.")

    except Exception as e:
        bot.reply_to(message, f"❌ Error al buscar ${ticker}: {e}")
        print(f"[BUSCAR] Error con {ticker}: {e}")

@bot.message_handler(commands=["ayuda"])
def handle_ayuda(message):
    texto = (
        "🤖 Comandos disponibles:\n\n"
        "/status — Estado de todos los workers\n"
        "/precios — Precio actual de BTC y Xiaomi\n"
        "/historial — Últimas 10 alertas disparadas\n"
        "/briefing — Resumen semanal de earnings en tu lista\n"
        "/lista — Ver empresas por grupo\n"
        "/buscar TICKER — Escanear una empresa al instante\n"
        "/agregar TICKER — Agregar al grupo CUSTOM ⭐\n"
        "/quitar TICKER — Quitar de cualquier grupo\n"
        "/ayuda — Este menú\n\n"
        f"⚙️ Sistema: {len(grupos)} workers paralelos\n"
        f"🔍 {total_empresas()} empresas vigiladas\n\n"
        "🧬 BIOTECH_FDA — catalizadores FDA\n"
        "💻 TECH_GROWTH — tech y software\n"
        "🚀 EV_SPACE_CRYPTO — EV, espacio, cripto\n"
        "💰 FINTECH_MEME_INTL — fintech, meme, intl\n"
        "⭐ CUSTOM — tus tickers personales\n\n"
        "🚨 Alertas solo para: FDA, ensayos fase 2/3,\n"
        "   ganancias récord, contratos masivos, M&A"
    )
    bot.reply_to(message, texto)

@bot.message_handler(commands=["historial"])
def handle_historial(message):
    partes = message.text.strip().split()
    n = 10
    if len(partes) > 1 and partes[1].isdigit():
        n = max(1, min(int(partes[1]), 50))

    entradas = leer_historial(n)
    if not entradas:
        bot.reply_to(message, "📭 Historial vacío — aún no se ha disparado ninguna alerta.")
        return

    iconos = {
        "noticia":       "🚨",
        "precio_btc":    "₿",
        "precio_xiaomi": "小",
    }
    lineas = [f"📋 Últimas {len(entradas)} alertas registradas:\n"]
    for e in reversed(entradas):
        icono = iconos.get(e.get("tipo", ""), "📌")
        ticker = e.get("ticker", "?")
        fecha  = e.get("fecha", "?")
        titulo = e.get("titulo", "")[:70]
        rango  = e.get("rango", "")
        plazo  = e.get("plazo", "")
        lineas.append(
            f"{icono} [{fecha}] ${ticker}\n"
            f"   {titulo}\n"
            f"   📈 {rango} | ⏱ {plazo}\n"
        )

    total_registradas = 0
    try:
        if os.path.exists(ARCHIVO_HISTORIAL):
            with open(ARCHIVO_HISTORIAL) as f:
                total_registradas = len(json.load(f))
    except Exception:
        pass

    lineas.append(f"\n📂 Total en historial: {total_registradas} alertas guardadas")
    lineas.append("Usa /historial N para ver más (máx 50)")

    texto = "\n".join(lineas)
    for i in range(0, len(texto), 4000):
        bot.reply_to(message, texto[i:i+4000])
    print(f"[HISTORIAL] /historial respondido ({len(entradas)} entradas)")

# ─────────────────────────────────────────────
# BRIEFING MATUTINO
# ─────────────────────────────────────────────
def generar_briefing():
    """Obtiene earnings de la semana y cruza con la lista vigilada."""
    hoy = datetime.now()
    lunes = hoy - timedelta(days=hoy.weekday())
    viernes = lunes + timedelta(days=4)
    desde = lunes.strftime("%Y-%m-%d")
    hasta = viernes.strftime("%Y-%m-%d")
    todos_tickers = set(t for g in grupos.values() for t in g)
    try:
        rate_limiter.acquire()
        url = (
            f"https://finnhub.io/api/v1/calendar/earnings"
            f"?from={desde}&to={hasta}&token={FINNHUB_API_KEY}"
        )
        resp = requests.get(url, timeout=15)
        datos = resp.json()
        earnings_list = datos.get("earningsCalendar", [])
    except Exception as e:
        print(f"[BRIEFING] Error obteniendo earnings: {e}")
        earnings_list = []

    # Filtrar solo los que están en nuestra lista
    coincidencias = [
        e for e in earnings_list
        if e.get("symbol", "").upper() in todos_tickers
    ]

    semana_str = f"{lunes.strftime('%d/%m')} — {viernes.strftime('%d/%m/%Y')}"
    lineas = [
        f"🌅 BRIEFING SEMANAL — {hoy.strftime('%d/%m/%Y')}\n",
        f"📅 Semana: {semana_str}",
        f"🔍 Vigilando {len(todos_tickers)} empresas\n",
    ]

    if not coincidencias:
        lineas.append("📭 No hay earnings programados esta semana para empresas vigiladas.")
    else:
        lineas.append(f"📊 {len(coincidencias)} earnings detectados esta semana:\n")
        dias_es = {
            "Monday": "Lunes", "Tuesday": "Martes", "Wednesday": "Miércoles",
            "Thursday": "Jueves", "Friday": "Viernes",
        }
        for fecha in sorted(set(e.get("date", "?") for e in coincidencias)):
            try:
                dt = datetime.strptime(fecha, "%Y-%m-%d")
                dia_nombre = dias_es.get(dt.strftime("%A"), dt.strftime("%A"))
                lineas.append(f"\n📆 {dia_nombre} {dt.strftime('%d/%m')}:")
            except Exception:
                lineas.append(f"\n📆 {fecha}:")

            for e in [x for x in coincidencias if x.get("date") == fecha]:
                sym = e.get("symbol", "?")
                hora = e.get("hour", "")
                eps_est = e.get("epsEstimate")
                rev_est = e.get("revenueEstimate")
                hora_txt = {"bmo": "antes apertura", "amc": "tras cierre", "dmh": "en horario"}.get(hora, hora)
                
                # Estimación rápida de impacto
                rango = "+5% a +20%"
                plazo = "1-3 días"
                if eps_est and eps_est > 0:
                    rango = "+8% a +30%"
                    plazo = "1-5 días"
                
                extra = []
                if eps_est is not None:
                    extra.append(f"EPS est: ${eps_est:.2f}")
                if rev_est:
                    extra.append(f"Rev est: ${rev_est/1e6:.0f}M")
                extra_str = " | ".join(extra)
                
                lineas.append(
                    f" • ${sym} — {hora_txt}\n"
                    f"   📈 Potencial estimado: {rango}\n"
                    f"   ⏱️ Plazo estimado: {plazo}\n"
                    f"   {extra_str}"
                )

    lineas.append(
        f"\n⚠️ Las alertas automáticas siguen activas 24/7.\n"
        f"Usa /buscar TICKER para escanear al instante."
    )
    return "\n".join(lineas)

@bot.message_handler(commands=["briefing"])
def handle_briefing(message):
    bot.reply_to(message, "📊 Generando briefing semanal, un momento...")
    print(f"[BRIEFING] Solicitado manualmente")
    texto = generar_briefing()
    bot.reply_to(message, texto)

# ─────────────────────────────────────────────
# MONITOR DE PRECIOS — BTC y XIAOMI
# ─────────────────────────────────────────────
BTC_CAIDA_DIA_PCT   = 8.0      # alerta si BTC cae >8% en el día
BTC_CAIDA_30MIN_PCT = 5.0      # alerta si BTC cae >5% en 30 minutos
BTC_NIVEL_ALERTA    = 40_000   # alerta si BTC toca $40,000
XIAOMI_NIVEL_EUR    = 2.0      # alerta si Xiaomi (XIACY) llega a €2.00

_precios_btc     = deque(maxlen=12)   # historial de los últimos 60 min (5-min ticks)
_btc_apertura    = None
_fecha_precios   = None
_btc_precio_actual   = None
_xiacy_eur_actual    = None
_alertas_precio  = {
    "btc_caida_dia":    False,
    "btc_nivel_40k":    False,
    "xiaomi_2eur":      False,
    "btc_brusco_ts":    0,          # timestamp del último alerta de movimiento brusco
}

def _get_quote(simbolo):
    url = f"https://finnhub.io/api/v1/quote?symbol={simbolo}&token={FINNHUB_API_KEY}"
    return requests.get(url, timeout=10).json().get("c", 0)

def loop_monitor_precios():
    global _btc_apertura, _fecha_precios, _btc_precio_actual, _xiacy_eur_actual

    print("[PRECIO] Monitor BTC + Xiaomi iniciado 🪙")

    while True:
        try:
            hoy = datetime.now().strftime("%Y-%m-%d")

            # Reset diario
            if _fecha_precios != hoy:
                _fecha_precios = hoy
                _btc_apertura  = None
                _precios_btc.clear()
                _alertas_precio["btc_caida_dia"]  = False
                _alertas_precio["btc_nivel_40k"]  = False
                _alertas_precio["xiaomi_2eur"]     = False
                print("[PRECIO] Nuevo día — alertas de precio reiniciadas.")

            ahora = time.time()

            # ── BTC/USD ───────────────────────────────────────────
            rate_limiter.acquire()
            btc = _get_quote("BINANCE:BTCUSDT")

            if btc and btc > 0:
                _btc_precio_actual = btc

                if _btc_apertura is None:
                    _btc_apertura = btc
                    print(f"[PRECIO] BTC apertura: ${btc:,.0f}")

                _precios_btc.append((ahora, btc))

                # Caída diaria >8%
                cambio_dia = (btc - _btc_apertura) / _btc_apertura * 100
                if cambio_dia <= -BTC_CAIDA_DIA_PCT and not _alertas_precio["btc_caida_dia"]:
                    _alertas_precio["btc_caida_dia"] = True
                    bot.send_message(CHAT_ID,
                        f"🔴 ALERTA BTC — CAÍDA DEL DÍA\n\n"
                        f"Bitcoin bajó {cambio_dia:.1f}% desde apertura\n"
                        f"💰 Ahora: ${btc:,.0f}\n"
                        f"📉 Apertura: ${_btc_apertura:,.0f}\n"
                        f"📉 Pérdida: ${_btc_apertura - btc:,.0f}\n\n"
                        f"⚠️ Revisa posiciones — volatilidad alta"
                    )
                    guardar_en_historial({
                        "tipo":   "precio_btc",
                        "fecha":  datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "ticker": "BTC",
                        "titulo": f"BTC caída del día {cambio_dia:.1f}%",
                        "precio": f"${btc:,.0f}",
                        "rango":  f"{cambio_dia:.1f}%",
                        "plazo":  "intraday",
                        "evento": "Caída diaria superó el umbral de alerta",
                    })
                    print(f"[PRECIO] 🔴 BTC caída diaria: {cambio_dia:.1f}%")

                # Movimiento brusco en 30 min (6 ticks × 5 min = 30 min)
                cooldown = 3600
                if (len(_precios_btc) >= 6 and
                        ahora - _alertas_precio["btc_brusco_ts"] > cooldown):
                    precio_30min = _precios_btc[-6][1]
                    cambio_30 = (btc - precio_30min) / precio_30min * 100
                    if cambio_30 <= -BTC_CAIDA_30MIN_PCT:
                        _alertas_precio["btc_brusco_ts"] = ahora
                        bot.send_message(CHAT_ID,
                            f"⚡ ALERTA BTC — MOVIMIENTO BRUSCO\n\n"
                            f"Bitcoin cayó {cambio_30:.1f}% en 30 minutos\n"
                            f"💰 Ahora: ${btc:,.0f}\n"
                            f"📉 Hace 30 min: ${precio_30min:,.0f}\n"
                            f"📉 Caída: ${precio_30min - btc:,.0f}\n\n"
                            f"⚡ Alta volatilidad — movimiento inusual detectado"
                        )
                        guardar_en_historial({
                            "tipo":   "precio_btc",
                            "fecha":  datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "ticker": "BTC",
                            "titulo": f"BTC movimiento brusco {cambio_30:.1f}% en 30 min",
                            "precio": f"${btc:,.0f}",
                            "rango":  f"{cambio_30:.1f}%",
                            "plazo":  "30 minutos",
                            "evento": "Caída brusca intraday",
                        })
                        print(f"[PRECIO] ⚡ BTC movimiento brusco 30min: {cambio_30:.1f}%")

                # Nivel $40,000
                if btc <= BTC_NIVEL_ALERTA and not _alertas_precio["btc_nivel_40k"]:
                    _alertas_precio["btc_nivel_40k"] = True
                    bot.send_message(CHAT_ID,
                        f"🎯 ALERTA BTC — NIVEL ${BTC_NIVEL_ALERTA:,}\n\n"
                        f"Bitcoin ha tocado ${btc:,.0f}\n"
                        f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                        f"Nivel de soporte histórico relevante.\n"
                        f"⚠️ Monitoriza posibles rebotes o continuación bajista."
                    )
                    guardar_en_historial({
                        "tipo":   "precio_btc",
                        "fecha":  datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "ticker": "BTC",
                        "titulo": f"BTC tocó nivel ${BTC_NIVEL_ALERTA:,}",
                        "precio": f"${btc:,.0f}",
                        "rango":  "nivel clave",
                        "plazo":  "—",
                        "evento": f"Precio alcanzó el nivel de alerta ${BTC_NIVEL_ALERTA:,}",
                    })
                    print(f"[PRECIO] 🎯 BTC nivel $40,000 alcanzado")

            # ── Xiaomi XIACY (USD → EUR) ──────────────────────────
            rate_limiter.acquire()
            xiacy_usd = _get_quote("XIACY")

            rate_limiter.acquire()
            eur_usd = _get_quote("OANDA:EUR_USD")   # 1 EUR = X USD

            if xiacy_usd and xiacy_usd > 0 and eur_usd and eur_usd > 0:
                xiacy_eur = xiacy_usd / eur_usd
                _xiacy_eur_actual = xiacy_eur
                print(f"[PRECIO] XIACY: ${xiacy_usd:.3f} = €{xiacy_eur:.3f} | BTC: ${btc:,.0f}")

                if xiacy_eur <= XIAOMI_NIVEL_EUR and not _alertas_precio["xiaomi_2eur"]:
                    _alertas_precio["xiaomi_2eur"] = True
                    bot.send_message(CHAT_ID,
                        f"🎯 ALERTA XIAOMI — NIVEL €{XIAOMI_NIVEL_EUR:.2f}\n\n"
                        f"Xiaomi (XIACY) ha tocado €{xiacy_eur:.3f}\n"
                        f"💰 Precio USD: ${xiacy_usd:.3f}\n"
                        f"💱 EUR/USD: {eur_usd:.4f}\n"
                        f"📅 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                        f"Nivel de precio objetivo alcanzado.\n"
                        f"⚠️ Confirma con volumen antes de actuar."
                    )
                    guardar_en_historial({
                        "tipo":   "precio_xiaomi",
                        "fecha":  datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "ticker": "XIACY",
                        "titulo": f"Xiaomi tocó nivel €{XIAOMI_NIVEL_EUR:.2f}",
                        "precio": f"€{xiacy_eur:.3f} (${xiacy_usd:.3f} USD)",
                        "rango":  "nivel objetivo",
                        "plazo":  "—",
                        "evento": f"Precio Xiaomi alcanzó el nivel de alerta €{XIAOMI_NIVEL_EUR:.2f}",
                    })
                    print(f"[PRECIO] 🎯 Xiaomi nivel €2.00 alcanzado")

        except Exception as e:
            print(f"[PRECIO] Error: {e}")

        time.sleep(300)   # cada 5 minutos

@bot.message_handler(commands=["precios"])
def handle_precios(message):
    partes = []

    if _btc_precio_actual and _btc_apertura:
        cambio = (_btc_precio_actual - _btc_apertura) / _btc_apertura * 100
        signo = "+" if cambio >= 0 else ""
        partes.append(
            f"₿ Bitcoin (BTC)\n"
            f"   Precio: ${_btc_precio_actual:,.0f}\n"
            f"   Hoy: {signo}{cambio:.2f}% (apertura ${_btc_apertura:,.0f})\n"
            f"   Alerta nivel $40k: {'✅ enviada' if _alertas_precio['btc_nivel_40k'] else '🟡 pendiente'}"
        )
    else:
        partes.append("₿ Bitcoin — sin datos aún (primer ciclo en curso)")

    if _xiacy_eur_actual:
        partes.append(
            f"\n小 Xiaomi (XIACY)\n"
            f"   Precio: €{_xiacy_eur_actual:.3f}\n"
            f"   Alerta nivel €2.00: {'✅ enviada' if _alertas_precio['xiaomi_2eur'] else '🟡 pendiente'}"
        )
    else:
        partes.append("\n小 Xiaomi — sin datos aún")

    partes.append(f"\n🕐 {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    bot.reply_to(message, "\n".join(partes))
    print(f"[PRECIO] /precios respondido")

# ─────────────────────────────────────────────
# KEEP-ALIVE — servidor HTTP + auto-ping
# ─────────────────────────────────────────────
class _PingHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        btc_txt = f"${_btc_precio_actual:,.0f}" if _btc_precio_actual else "cargando..."
        body = (
            f"✅ Bot activo\n"
            f"Workers: {len([g for g, v in grupos.items() if v])}\n"
            f"Empresas: {total_empresas()}\n"
            f"BTC: {btc_txt}\n"
            f"Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n"
        )
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, format, *args):
        pass  # silenciar logs HTTP en consola

def loop_keepalive_server():
    server = HTTPServer(("0.0.0.0", 8080), _PingHandler)
    print("[KEEPALIVE] Servidor HTTP activo en puerto 8080")
    server.serve_forever()

def loop_selfping():
    """Hace ping a sí mismo cada 4 minutos para evitar el sleep de Replit."""
    time.sleep(90)  # esperar a que el servidor HTTP arranque
    dominio = os.environ.get("REPLIT_DEV_DOMAIN", "")
    if not dominio:
        print("[SELFPING] REPLIT_DEV_DOMAIN no encontrado — self-ping desactivado")
        return
    url = f"https://{dominio}"
    print(f"[SELFPING] Auto-ping activado → {url}")
    while True:
        try:
            r = requests.get(url, timeout=10)
            print(f"[SELFPING] OK {r.status_code} — bot despierto")
        except Exception as e:
            print(f"[SELFPING] Error: {e}")
        time.sleep(240)  # cada 4 minutos
def enviar_briefing():
    texto = generar_briefing()
    try:
        bot.send_message(CHAT_ID, texto)
        print(f"[BRIEFING] Enviado a las {datetime.now().strftime('%H:%M')}")
    except Exception as e:
        print(f"[BRIEFING] Error enviando: {e}")

def loop_briefing():
    """Envía el briefing todos los días a las 09:00."""
    HORA_BRIEFING = 9
    ultimo_envio = None
    while True:
        ahora = datetime.now()
        hoy = ahora.date()
        
        if ahora.hour == HORA_BRIEFING and (ultimo_envio is None or ultimo_envio != hoy):
            enviar_briefing()
            ultimo_envio = hoy
            print(f"[BRIEFING] Enviado correctamente a las {ahora.strftime('%H:%M')}")
        
        time.sleep(30)
if __name__ == "__main__":
    grupos_activos = [g for g, v in grupos.items() if v]
    print(f"Bot iniciado con {len(grupos_activos)} workers paralelos.")
    print(f"Total empresas: {total_empresas()} (ampliado)")
    print(f"Rate limiter: {MAX_CALLS_POR_MINUTO} calls/min compartidos entre workers.")

    for nombre_grupo in grupos_activos:
        t = threading.Thread(target=worker_grupo, args=(nombre_grupo,), daemon=True)
        t.start()

        t_briefing = threading.Thread(target=loop_briefing, daemon=True)
    t_briefing.start()
    print("Briefing automático programado a las 09:00 cada día.")

    t_precios = threading.Thread(target=loop_monitor_precios, daemon=True)
    t_precios.start()
    print("Monitor de precios BTC + Xiaomi iniciado (cada 5 min).")

    t_http = threading.Thread(target=loop_keepalive_server, daemon=True)
    t_http.start()

    t_ping = threading.Thread(target=loop_selfping, daemon=True)
    t_ping.start()
    print("Escuchando comandos de Telegram...")

    # ==================== CONFIGURACIÓN PARA RENDER ====================
    print("✅ Bot configurado para Render")
    bot.infinity_polling()
