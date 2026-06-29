import time
import os
import json
import threading
import telebot
import requests
from datetime import datetime, timedelta
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]

ARCHIVO_GRUPOS = "grupos.json"
ARCHIVO_HISTORIAL = "historial.json"
MAX_HISTORIAL = 500
PAUSA_ENTRE_CICLOS = 20
MAX_CALLS_POR_MINUTO = 55

# ─────────────────────────────────────────────
# GRUPOS DE EMPRESAS
# ─────────────────────────────────────────────
GRUPOS_DEFAULT = {
    "BIOTECH_FDA": [
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
        "PEGY", "HLTH", "HOOK", "HGEN", "ANIP", "ICCM", "LHCG",
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
    "CUSTOM": [],  # stocks añadidos por el usuario con /agregar
}

# ─────────────────────────────────────────────
# FRASES DE ALTO IMPACTO
# ─────────────────────────────────────────────
PALABRAS_ALTO_IMPACTO = [
    "fda approval", "fda approves", "fda clears", "fda grants approval",
    "breakthrough therapy designation", "fast track designation",
    "priority review granted", "pdufa", "nda approval", "bla approval",
    "accelerated approval", "phase 3 success", "phase 3 positive",
    "positive pivotal", "pivotal trial success", "met primary endpoint",
    "met all primary", "statistically significant", "superior to placebo",
    "phase 2 positive", "positive phase 2", "earnings beat", "beat expectations",
    "record revenue", "record earnings", "revenue beat", "raised guidance",
    "raised full-year", "blowout quarter", "record quarterly",
    "major contract", "billion-dollar", "multi-billion", "massive order",
    "strategic partnership", "licensing agreement", "exclusive license",
    "landmark deal", "definitive agreement", "acquisition agreement",
    "merger agreement", "buyout offer", "takeover bid", "going private",
    "strategic alternatives", "letter of intent",
]

# Rate Limiter
class RateLimiter:
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

bot = telebot.TeleBot(TELEGRAM_TOKEN)
lock = threading.Lock()
noticias_vistas = set()
alertas_hoy = 0
fecha_alertas = datetime.now().strftime("%Y-%m-%d")

grupos = GRUPOS_DEFAULT.copy()

def total_empresas():
    return sum(len(v) for v in grupos.values())
# ─────────────────────────────────────────────
# PERSISTENCIA
# ─────────────────────────────────────────────
def cargar_grupos():
    if os.path.exists(ARCHIVO_GRUPOS):
        try:
            with open(ARCHIVO_GRUPOS, "r") as f:
                datos = json.load(f)
            grupos = {k: list(v) for k, v in GRUPOS_DEFAULT.items()}
            for nombre, tickers in datos.items():
                grupos[nombre] = tickers
            print(f"Grupos cargados: {{k: len(v) for k, v in grupos.items()}}")
            return grupos
        except Exception as e:
            print(f"⚠️ Error leyendo grupos: {e}")
    return GRUPOS_DEFAULT.copy()

def guardar_grupos():
    try:
        with open(ARCHIVO_GRUPOS, "w") as f:
            json.dump(grupos, f)
    except Exception as e:
        print(f"⚠️ Error guardando grupos: {e}")

grupos = cargar_grupos()

# ... (el resto del código)

print(f"Bot iniciado con {len(grupos)} grupos | Total empresas: {total_empresas()}")

# El resto de funciones (escanear_ticker, worker_grupo, etc.) se mantienen iguales.

# ─────────────────────────────────────────────
# ARRANQUE
# ─────────────────────────────────────────────
if __name__ == "__main__":
    grupos_activos = [g for g, v in grupos.items() if v]
    print(f"Bot iniciado con {len(grupos_activos)} workers paralelos.")
    print(f"Total empresas: {total_empresas()}")
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

    print("✅ Bot configurado para Render")
    bot.infinity_polling()
