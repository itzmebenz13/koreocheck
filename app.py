"""
SHEIN Price Checker — Railway Backend
Full cart + checkout simulation using owner's SHEIN account.

Flow:
  1. GET  /product/get_goods_detail_realtime_data  → price + applicable coupons
  2. POST /order/add_to_cart                       → add item, get cart_id + product info
  3. POST /order/order/checkout                    → full price breakdown (shipping, points, total)
  4. POST /order/del_carts                         → cleanup added items
  5. Return complete breakdown to frontend

All owner credentials are set as Railway environment variables.
"""

import os
import re
import time
import json
import threading
import urllib3
from flask import Flask, request, jsonify, render_template

import requests as rq

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY CREDENTIAL OVERRIDE
# Updated via /admin/refresh — survives until server restarts.
# Overrides the env var values so Railway redeploy is NOT needed.
# ─────────────────────────────────────────────────────────────────────────────
_live_creds = {}   # keys: GW_AUTH, ANTI_IN, CS_RANDOM, RULEIDS, ATC_GW_AUTH, ATC_ANTI_IN

def _cred(key: str) -> str:
    """Return live override if set, else fall back to env var."""
    return _live_creds.get(key) or globals().get(key, "")

def _parse_raw_headers(raw: str) -> dict:
    """Extract headers from a raw HTTP request dump."""
    found = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        name, _, val = line.partition(":")
        name = name.strip().lower()
        val  = val.strip()
        mapping = {
            "x-gw-auth":   "GW_AUTH",
            "anti-in":     "ANTI_IN",
            "x-cs-random": "CS_RANDOM",
            "ruleids":     "RULEIDS",
            "armortoken":  "ARMOR_TOKEN",
            "token":       "TOKEN",
            "smdeviceid":  "SMDEVICE_ID",
            "cookie":      "COOKIE",
            "x-ad-flag":   "AD_FLAG",
        }
        if name in mapping and val:
            found[mapping[name]] = val
    return found

# ─────────────────────────────────────────────────────────────────────────────
# CART CLEANUP QUEUE  (30-minute auto-cleanup safety net)
# Tracks cart_ids added by the price checker so stale items are auto-deleted
# even if the immediate post-checkout cleanup fails.
# ─────────────────────────────────────────────────────────────────────────────
_cleanup_queue  = {}   # {cart_id: (timestamp, country)}
_cleanup_lock   = threading.Lock()
CLEANUP_TTL_SEC = 1800  # 30 minutes

def _enqueue_cleanup(cart_ids: list, country: str = "PH"):
    ts = time.time()
    with _cleanup_lock:
        for cid in cart_ids:
            _cleanup_queue[cid] = (ts, country)

def _cleanup_worker():
    """Background thread: deletes stale cart items every 60 seconds."""
    while True:
        time.sleep(60)
        now = time.time()
        to_del = {}   # country → [cart_id]
        with _cleanup_lock:
            for cid, (ts, country) in list(_cleanup_queue.items()):
                if now - ts > CLEANUP_TTL_SEC:
                    to_del.setdefault(country, []).append(cid)
            for cids in to_del.values():
                for cid in cids:
                    _cleanup_queue.pop(cid, None)
        for country, cids in to_del.items():
            try:
                api_delete_cart_items(cids, country)
            except Exception:
                pass

_cleanup_thread = threading.Thread(target=_cleanup_worker, daemon=True)
_cleanup_thread.start()

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────────
# OWNER CREDENTIALS  (Railway env vars)
# ─────────────────────────────────────────────────────────────────
SMDEVICE_ID = os.environ.get("SMDEVICE_ID", "")
ARMOR_TOKEN = os.environ.get("ARMOR_TOKEN", "")
GW_AUTH     = os.environ.get("GW_AUTH", "")
TOKEN       = os.environ.get("TOKEN", "")
CS_RANDOM   = os.environ.get("CS_RANDOM", "")
ANTI_IN     = os.environ.get("ANTI_IN", "")
AD_FLAG     = os.environ.get("AD_FLAG", "")
UGID        = os.environ.get("UGID", "")
SORTUID     = os.environ.get("SORTUID", "")
DEVICE_ID   = os.environ.get("DEVICE_ID", "")
DEVICE_INFO = os.environ.get("DEVICE_INFO", "Pixel4 Android11")
APP_VERSION = os.environ.get("APP_VERSION", "13.9.8")
APPCOUNTRY  = os.environ.get("APPCOUNTRY", "GB")
COOKIE      = os.environ.get("COOKIE", "")
RULEIDS     = os.environ.get("RULEIDS", "")

# Cart-specific credentials (from add-to-cart capture — different signature than product detail)
ATC_GW_AUTH = os.environ.get("ATC_GW_AUTH", "")   # x-gw-auth from add-to-cart capture
ATC_ANTI_IN = os.environ.get("ATC_ANTI_IN", "")   # anti-in from add-to-cart capture

# Owner's pre-configured address  (no user input needed)
ADDRESS_ID  = os.environ.get("ADDRESS_ID", "2124807075")
CITY        = os.environ.get("CITY", "BAUANG")
POSTCODE    = os.environ.get("POSTCODE", "2501")
STATE       = os.environ.get("STATE", "LA-UNION")
COUNTRY_ID  = os.environ.get("COUNTRY_ID", "170")

API_HOST    = "https://api-service.shein.com"
CURRENCY_MAP = {"PH": "PHP", "TH": "THB", "MY": "MYR", "SG": "SGD", "US": "USD"}
SYMBOL_MAP   = {"PHP": "₱", "THB": "฿", "MYR": "RM", "SGD": "S$", "USD": "$"}


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def headers(country: str = "PH", extra: dict = None) -> dict:
    currency = CURRENCY_MAP.get(country.upper(), "PHP")
    h = {
        "accept":              "application/json",
        "app-from":            "shein",
        "appname":             "shein app",
        "apptype":             "shein",
        "appcountry":          country.upper(),   # Use the actual country, not the capture country (was GB, causing 836000 for PH products)
        "appcurrency":         currency,
        "applanguage":         "en",
        "clientid":            "100",
        "currency":            currency,
        "dev-id":              DEVICE_ID,
        "device":              DEVICE_INFO,
        "deviceid":            DEVICE_ID,
        "devicesystemversion": "Android11",
        "devtype":             "Android",
        "language":            "en",
        "localcountry":        country.upper(),
        "network-type":        "UNKNOWN",
        "os-version":          "11",
        "platform":            "app-native",
        "siteuid":             "android",
        "smdeviceid":          _cred("SMDEVICE_ID"),
        "armortoken":          _cred("ARMOR_TOKEN"),
        "token":               _cred("TOKEN"),
        "x-gw-auth":           _cred("GW_AUTH"),
        "x-cs-random":         _cred("CS_RANDOM"),
        "anti-in":             _cred("ANTI_IN"),
        "x-ad-flag":           _cred("AD_FLAG"),
        "ugid":                UGID,
        "newuid":              SORTUID,
        "sortuid":             SORTUID,
        "usercountry":         country.upper(),
        "version":             APP_VERSION,
        "appversion":          APP_VERSION,
        "cookie":              _cred("COOKIE"),
        "ruleids":             "45950," + (_cred("RULEIDS") or RULEIDS),   # 45950 = PH access rule
        "user-agent":          f"Shein {APP_VERSION} Android 11 {DEVICE_INFO} {country.upper()} en {SORTUID}",
        "accept-encoding":     "gzip",
        "uberctx-personal-switch": "u-1.r-1.s-1",
        "uberctx-traffic-mark-member": "26",
    }
    if extra:
        h.update(extra)
    return h


def extract_goods_id(raw: str) -> str | None:
    raw = raw.strip()
    if raw.isdigit():
        return raw
    for pat in [r"-p-(\d+)-", r"goods_id=(\d+)", r"/(\d{7,12})(?:[^\d]|$)"]:
        m = re.search(pat, raw)
        if m:
            return m.group(1)
    return None


def parse_amount(obj) -> float:
    if not obj:
        return 0.0
    try:
        return float(obj.get("amount", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def fmt_amount(obj, symbol="₱") -> str:
    if not obj:
        return f"{symbol}0"
    return obj.get("amountWithSymbol") or f"{symbol}{obj.get('amount', 0)}"


def parse_threshold(text: str) -> float:
    if not text or "no min" in text.lower():
        return 0.0
    nums = re.findall(r"[\d]+", text.replace(",", ""))
    return float(nums[0]) if nums else 0.0


def calc_coupon_discount(discount_str: str, price: float, min_order: float) -> dict:
    eligible = price >= min_order
    pct_m = re.search(r"(\d+(?:\.\d+)?)\s*%", discount_str)
    if pct_m and eligible:
        pct  = float(pct_m.group(1))
        disc = round(price * pct / 100, 2)
        return {"eligible": True, "type": "percent", "pct": pct,
                "discount": disc, "final": round(price - disc, 2)}
    if pct_m:
        return {"eligible": False, "type": "percent", "pct": float(pct_m.group(1)),
                "discount": 0, "final": price}
    if "free" in discount_str.lower():
        return {"eligible": eligible, "type": "free_shipping",
                "discount": 0, "final": price}
    amt_m = re.search(r"[\d,]+(?:\.\d+)?", discount_str.replace(",", ""))
    if amt_m and eligible:
        disc = float(amt_m.group())
        return {"eligible": True, "type": "fixed", "amount": disc,
                "discount": disc, "final": round(max(price - disc, 0), 2)}
    return {"eligible": False, "type": "unknown", "discount": 0, "final": price}


# ─────────────────────────────────────────────────────────────────
# SHEIN API CALLS
# ─────────────────────────────────────────────────────────────────

TIMEZONE_MAP = {
    "PH": "Asia/Manila",
    "TH": "Asia/Bangkok",
    "MY": "Asia/Kuala_Lumpur",
    "SG": "Asia/Singapore",
    "US": "America/New_York",
}

COUNTRY_ID_MAP = {
    "PH": "170", "TH": "219", "MY": "131", "SG": "185", "US": "226",
}

def _product_detail_params(goods_id: str, country: str = "PH") -> dict:
    """Build the full set of query params for the product detail realtime endpoint."""
    tz = TIMEZONE_MAP.get(country.upper(), "Asia/Manila")
    return {
        "priorityMallType":            "1",
        "sceneFromPage":               "",
        "isRelatedColorNeedPromotion": "",
        "promotionId":                 "",
        "isAppointMall":               "0",
        "useSupplyGoods":              "",
        "isUserSelectedMallCode":      "0",
        "sceneFlag":                   "",
        "mallCode":                    "1",
        "localSiteQueryFlag":          "0",
        "orderPrice":                  "",
        "isHideNotSatisfied":          "",
        "isSizeGatherTag":             "",
        "hasReportMember":             "0",
        "sourceFrom":                  "goods_detail",
        "promotionLogoType":           "",
        "promotionType":               "",
        "isHidePromotionTip":          "",
        "goods_id":                    goods_id,
        "timeZone":                    tz,
        "isHideEstimatePriceInfo":     "",
        "popComponentEntry":           "",
        "bundledPurchaseMainGoodsId":  "",
        "visitNumOfDay":               "1",
        "isShowMall":                  "0",
        "isPaidMember":                "0",
        "billno":                      "",
        "promotionProductMark":        "",
    }


def api_search_product(goods_id: str, country: str = "PH") -> dict | None:
    """
    Search for a product by goods_id using the search endpoint.
    This works for ALL products — no 836000 issues.
    Returns the first matching product dict, or None.
    """
    body = (
        f"keywords={goods_id}&page=1&limit=10&sort=0"
        f"&source=search&scene=search&mall_code=1"
        f"&search_source=1&page_name=page_search"
        f"&force_suggest=0&force_not_correct=0"
        f"&isClickRefresh=0&exposedPosition=0"
        f"&poskey=&tag_type=&cat_id=&store_code="
    )
    h = headers(country, {"content-type": "application/x-www-form-urlencoded"})
    try:
        resp = rq.post(f"{API_HOST}/product/get_products_by_keywords",
                       data=body, headers=h, timeout=12, verify=False)
        data = resp.json()
        if str(data.get("code")) == "0":
            products = (data.get("info") or {}).get("products") or []
            # Find exact goods_id match
            for p in products:
                if str(p.get("goods_id")) == str(goods_id):
                    return p
            # Return first result if no exact match
            return products[0] if products else None
    except Exception:
        pass
    return None


def api_product_static(goods_id: str, country: str = "PH") -> dict:
    """
    Fetch STATIC product data — name, images, SKUs, base price.
    Uses get_goods_detail_static_data_v2 which works for ALL products
    (confirmed from user captures — no 836000 issues).
    """
    params = {
        "priorityMallType":       "1",
        "trendingId":             "",
        "sceneFromPage":          "",
        "goods_id":               goods_id,
        "goodsPicAbAbt":          "A",
        "isHidePaidMemberInfo":   "0",
        "isHideEstimatePriceInfo": "",
        "isAppointMall":          "0",
        "mall_code":              "1",
        "isUserSelectedMallCode": "0",
        "underPriceShowAbtParam": "B",
        "sceneFlag":              "",
        "isShowMall":             "0",
        "abt_branch_ids":         "",
        "isPaidMember":           "0",
        "isHideNotSatisfied":     "",
        "isSizeGatherTag":        "",
        "sourceFrom":             "goods_detail",
    }
    resp = rq.get(f"{API_HOST}/product/get_goods_detail_static_data_v2",
                  params=params, headers=headers(country), timeout=15, verify=False)
    return resp.json()


def api_product_detail(goods_id: str, country: str = "PH") -> dict:
    """
    Fetch REALTIME product data — coupons (cmpCouponInfo), live stock, promotions.
    Uses get_goods_detail_realtime_data.
    May return 836000 for some products — callers must handle gracefully.
    """
    params = _product_detail_params(goods_id, country)
    try:
        resp = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                      params=params, headers=headers(country), timeout=12, verify=False)
        return resp.json()
    except Exception:
        return {"code": "error", "msg": "Request failed"}


def _try_web_product_detail(goods_id: str, country: str = "PH") -> dict | None:
    """
    Fallback: fetch product info from the SHEIN web API (m.shein.com).
    Uses lighter security — no armortoken required for basic product info.
    """
    country_lower = country.lower()
    web_base = f"https://m.shein.com/{country_lower}"
    tz = TIMEZONE_MAP.get(country.upper(), "Asia/Manila")
    cid = COUNTRY_ID_MAP.get(country.upper(), "170")

    web_headers = {
        "accept":           "application/json, text/plain, */*",
        "user-agent":       "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "origin":           "https://m.shein.com",
        "referer":          f"{web_base}/",
        "accept-language":  "en-US,en;q=0.9",
        "x-requested-with": "XMLHttpRequest",
    }
    if SMDEVICE_ID:
        web_headers["smdeviceid"] = SMDEVICE_ID
    if CSRF_TOKEN := os.environ.get("CSRF_TOKEN", ""):
        web_headers["x-csrf-token"] = CSRF_TOKEN
    if CS_RANDOM:
        web_headers["x-cs-random"] = CS_RANDOM
    if GW_AUTH:
        web_headers["x-gw-auth"] = GW_AUTH
    if ARMOR_TOKEN:
        web_headers["armortoken"] = ARMOR_TOKEN
    if COOKIE:
        web_headers["cookie"] = COOKIE
    if ANTI_IN:
        web_headers["anti-in"] = ANTI_IN
    if AD_FLAG:
        web_headers["x-ad-flag"] = AD_FLAG

    params = {
        "_ver": "1.1.8", "_lang": "en",
        "goods_id": goods_id, "mallCode": "1",
        "localSiteQueryFlag": "0", "countryId": cid,
        "isPaidMember": "0", "timeZone": tz,
        "visitNumOfDay": "1", "sourceFrom": "goods_detail",
    }

    try:
        resp = rq.get(
            f"{web_base}/bff-api/product/get_goods_detail_realtime_data",
            params=params, headers=web_headers, timeout=15, verify=False
        )
        return resp.json()
    except Exception:
        pass

    # Also try without /bff-api/ prefix
    try:
        resp = rq.get(
            f"https://m.shein.com/api/product/get_goods_detail_realtime_data",
            params=params, headers=web_headers, timeout=15, verify=False
        )
        return resp.json()
    except Exception:
        pass

    return None


def api_add_to_cart(goods_id: str, sku_code: str, qty: int = 1,
                    country: str = "PH") -> dict:
    """
    Add item to cart.
    Uses params format from the working T2 account capture:
    promotion_type, trace_id, promotion_id, local_goods — these are required for
    newer products that reject the older isAppointMall/fromPageName format.
    """
    import time as _t
    trace_id = f"sandroid{int(_t.time() * 1000)}{DEVICE_ID}"
    body = (f"promotion_type=0"
            f"&trace_id={trace_id}"
            f"&quantity={qty}"
            f"&mall_code=1"
            f"&skuMallCode=1"
            f"&goods_id={goods_id}"
            f"&promotion_id=0"
            f"&local_goods=0"
            f"&sku_code={sku_code}")
    overrides = {"content-type": "application/x-www-form-urlencoded"}
    if ATC_GW_AUTH:
        overrides["x-gw-auth"] = ATC_GW_AUTH
    if ATC_ANTI_IN:
        overrides["anti-in"] = ATC_ANTI_IN
    h = headers(country, overrides)
    resp = rq.post(f"{API_HOST}/order/add_to_cart",
                   params={"goods_id": goods_id},
                   data=body, headers=h, timeout=15, verify=False)
    return resp.json()


def api_checkout(country: str = "PH") -> dict:
    """Call checkout page — returns full price breakdown for checked cart items."""
    session_id = f"{SORTUID}{int(time.time() * 1000)}"
    payload = {
        "biz_mode_list": ["0"],
        "and_page": "v2",
        "request_card_token": "1",
        "hasCardBin": "1",
        "goods_type": "0",
        "userLocalSizeCountry": "",
        "is_old_version": "0",
        "giftcard_verify": "0",
        "isFirst": "1",
        "city": CITY,
        "postcode": POSTCODE,
        "state": STATE,
        "address_id": ADDRESS_ID,
        "country_id": COUNTRY_ID,
        "popup": {"oneClickLowestTimes": "0"},
    }
    overrides = {
        "content-type":   "application/json; charset=utf-8",
        "frontend-scene": "page_checkout",
        "sessionid":      session_id,
    }
    if ATC_GW_AUTH:
        overrides["x-gw-auth"] = ATC_GW_AUTH
    if ATC_ANTI_IN:
        overrides["anti-in"] = ATC_ANTI_IN
    h = headers(country, overrides)
    resp = rq.post(f"{API_HOST}/order/order/checkout",
                   json=payload, headers=h, timeout=20, verify=False)
    return resp.json()


def api_delete_cart_items(cart_ids: list, country: str = "PH") -> dict:
    """Remove items from cart by cart_id list."""
    payload = {"cart_id_list": cart_ids}
    h = headers(country, {"content-type": "application/json; charset=utf-8"})
    resp = rq.post(f"{API_HOST}/order/del_carts",
                   json=payload, headers=h, timeout=15, verify=False)
    return resp.json()


def api_get_cart(country: str = "PH") -> dict:
    """Get current cart to identify existing checked items before our check."""
    cid = COUNTRY_ID_MAP.get(country.upper(), COUNTRY_ID)
    h = headers(country, {"content-type": "application/json; charset=utf-8"})
    resp = rq.post(f"{API_HOST}/order/get_carts_info_for_order_confirm",
                   json={"bag_show_style": "1", "country_id": cid,
                         "userLocalSizeCountry": "", "postcode": POSTCODE},
                   headers=h, timeout=15, verify=False)
    return resp.json()


def parse_variants(info: dict) -> dict:
    """
    Extract variant matrix from multiLevelSaleAttribute.sku_list.
    Returns:
      {
        variants: [{sku_code, color, color_img, size, stock, in_stock}],
        has_colors: bool,
        has_sizes:  bool,
        unique_colors: [{"name": ..., "img": ...}],
        unique_sizes:  ["S","M","L",...],
        default_sku:   str,
      }
    """
    mls      = info.get("multiLevelSaleAttribute") or {}
    sku_list = mls.get("sku_list") or []

    variants       = []
    colors_seen    = {}   # name → img
    sizes_seen     = []   # ordered list, preserve insertion order

    for sku in sku_list:
        sku_code = sku.get("sku_code") or ""
        attrs    = sku.get("sku_sale_attr") or []
        stock    = int(sku.get("stock") or 0)

        color     = ""
        color_img = ""
        size      = ""

        for a in attrs:
            name_en = a.get("attr_name_en", "")
            val     = a.get("attr_value_name") or a.get("attr_value_name_en") or ""
            if name_en == "Color":
                color     = val
                color_img = a.get("attr_std_value") or ""
            elif name_en == "Size":
                size = val

        if color and color not in colors_seen:
            colors_seen[color] = color_img
        if size and size not in sizes_seen:
            sizes_seen.append(size)

        # Per-SKU price — try mall_price first, then buried, then None (fallback to product price)
        sku_price_raw = sku.get("price") or {}
        mall_p   = ((sku.get("mall_price") or [{}])[0]) if sku.get("mall_price") else {}
        buried_p = (sku_price_raw.get("buriedPrice") or {}).get("price") or {}
        special_p = sku_price_raw.get("special_price") or sku_price_raw.get("salePrice") or {}
        sku_price = (parse_amount(mall_p.get("salePrice") or {})
                     or parse_amount(special_p)
                     or None)
        sku_display = (fmt_amount(mall_p.get("salePrice") or {})
                       or fmt_amount(special_p)
                       or None)

        variants.append({
            "sku_code":    sku_code,
            "color":       color,
            "color_img":   color_img,
            "size":        size,
            "stock":       stock,
            "in_stock":    stock > 0,
            "price":       sku_price,
            "price_display": sku_display,
        })

    # If only 1 unique color across all SKUs, treat as no color choice needed
    has_colors  = len(colors_seen) > 1
    has_sizes   = len(sizes_seen) > 0
    default_sku = variants[0]["sku_code"] if variants else (
        (info.get("buyNowInfo") or {}).get("skcPriceBySkuCode") or "")

    return {
        "variants":      variants,
        "has_colors":    has_colors,
        "has_sizes":     has_sizes,
        "unique_colors": [{"name": k, "img": v} for k, v in colors_seen.items()],
        "unique_sizes":  sizes_seen,
        "default_sku":   default_sku,
    }


# ─────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


def _get_sku_via_main_detail(goods_id: str, country: str) -> list:
    """
    Try to get sku_codes from various product endpoints.
    Returns list of {sku_code, color, size, stock, in_stock}.
    """
    tz = TIMEZONE_MAP.get(country.upper(), "Asia/Manila")
    # Static endpoint is the best fallback — confirmed to return full sku_list
    static_params = {
        "priorityMallType": "1", "goods_id": goods_id,
        "mallCode": "1", "isUserSelectedMallCode": "0",
        "isAppointMall": "0", "isShowMall": "0",
        "sourceFrom": "goods_detail", "isPaidMember": "0",
    }
    attempts = [
        # ⭐ Static endpoint — has sku_list, confirmed working with valid creds
        (f"{API_HOST}/product/get_goods_detail_static_data_v2", static_params),
        # Other fallbacks
        (f"{API_HOST}/product/main/goods_detail_v4",
         {"goods_id": goods_id, "mallCode": "1", "sourceFrom": "goods_detail"}),
        (f"{API_HOST}/product/get_goods_detail_realtime_data",
         _product_detail_params(goods_id, country)),
    ]

    # Also try web API (m.shein.com) — different security model
    web_country = country.lower()
    web_headers_map = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Mobile Safari/537.36",
        "referer": f"https://m.shein.com/{web_country}/",
        "x-requested-with": "XMLHttpRequest",
    }
    if COOKIE:      web_headers_map["cookie"]      = COOKIE
    if CS_RANDOM:   web_headers_map["x-cs-random"] = CS_RANDOM
    if GW_AUTH:     web_headers_map["x-gw-auth"]   = GW_AUTH
    if ARMOR_TOKEN: web_headers_map["armortoken"]   = ARMOR_TOKEN
    if SMDEVICE_ID: web_headers_map["smdeviceid"]   = SMDEVICE_ID
    if ANTI_IN:     web_headers_map["anti-in"]      = ANTI_IN

    web_attempts = [
        (f"https://m.shein.com/{web_country}/bff-api/product/get_goods_detail_realtime_data",
         {"_ver": "1.1.8", "_lang": "en", "goods_id": goods_id, "mallCode": "1",
          "sourceFrom": "goods_detail", "visitNumOfDay": "1",
          "timeZone": tz, "isPaidMember": "0"}),
    ]

    def _extract_skus(data):
        info = data.get("info") or {}
        for getter in [
            lambda i: (i.get("multiLevelSaleAttribute") or {}).get("sku_list") or [],
            lambda i: i.get("sku_list") or [],
            lambda i: i.get("skuList") or [],
        ]:
            sku_list = getter(info)
            if sku_list:
                result = []
                for sku in sku_list:
                    attrs = sku.get("sku_sale_attr") or []
                    color = next((a.get("attr_value_name") for a in attrs if a.get("attr_name_en") == "Color"), "")
                    size  = next((a.get("attr_value_name") for a in attrs if a.get("attr_name_en") == "Size"), "")
                    stock = int(sku.get("stock") or 0)
                    result.append({
                        "sku_code": sku.get("sku_code") or "",
                        "color": color, "size": size,
                        "stock": stock, "in_stock": stock > 0,
                    })
                return [r for r in result if r["sku_code"]]
        return []

    for ep, params in attempts:
        for h_variant in [headers(country), {k: v for k, v in headers(country).items() if k not in ("armortoken", "x-gw-auth", "anti-in")}]:
            try:
                data = rq.get(ep, params=params, headers=h_variant, timeout=10, verify=False).json()
                if str(data.get("code")) == "0":
                    skus = _extract_skus(data)
                    if skus:
                        return skus
            except Exception:
                pass

    for ep, params in web_attempts:
        try:
            data = rq.get(ep, params=params, headers=web_headers_map, timeout=10, verify=False).json()
            if str(data.get("code")) == "0":
                skus = _extract_skus(data)
                if skus:
                    return skus
        except Exception:
            pass

    return []


def _item_info_via_atc(goods_id: str, country: str) -> tuple:
    """
    Fallback product info when product detail realtime returns 836000.
    Step 1: Try main product detail endpoint to get sku_codes.
    Step 2: Use first available sku_code to do ATC probe → gets name, image, price.
    Returns without coupon data but item is fully usable.
    """
    currency = CURRENCY_MAP.get(country, "PHP")
    symbol   = SYMBOL_MAP.get(currency, "₱")

    prod_name    = f"Product #{goods_id}"
    prod_image   = ""
    prod_price   = 0.0
    prod_display = f"{symbol}0"
    variants     = []

    # ── Step 1: Get sku_codes via main product detail (different security path)
    sku_entries = _get_sku_via_main_detail(goods_id, country)
    first_sku   = next((s["sku_code"] for s in sku_entries if s.get("sku_code") and s.get("in_stock")), "") \
                  or next((s["sku_code"] for s in sku_entries if s.get("sku_code")), "")

    if not first_sku:
        return jsonify({
            "error": "Credentials need refreshing. Open SHEIN app → view any product page → capture the request to api-service.shein.com/product/get_goods_detail_realtime_data → update GW_AUTH, ANTI_IN, CS_RANDOM, RULEIDS in Railway, then redeploy."
        }), 400

    # ── Step 2: ATC probe with known sku_code → get name, image, price
    if ATC_GW_AUTH and ATC_ANTI_IN:
        try:
            atc_body = (f"isAppointMall=&mall_code=1&quantity=1&sceneFlag="
                        f"&skuMallCode=1&fromPageName=goodsDetailAddToCart"
                        f"&goods_id={goods_id}&sku_code={first_sku}")
            h_overrides = {"content-type": "application/x-www-form-urlencoded"}
            if ATC_GW_AUTH: h_overrides["x-gw-auth"] = ATC_GW_AUTH
            if ATC_ANTI_IN: h_overrides["anti-in"]   = ATC_ANTI_IN
            atc_resp = rq.post(f"{API_HOST}/order/add_to_cart",
                               params={"goods_id": goods_id},
                               data=atc_body,
                               headers=headers(country, h_overrides),
                               timeout=8, verify=False)
            atc = atc_resp.json()
            if str(atc.get("code")) == "0":
                cart_obj = ((atc.get("info") or {}).get("cart") or {})
                cart_id  = cart_obj.get("id")
                product  = cart_obj.get("product") or {}
                prod_name    = product.get("goods_name") or prod_name
                prod_image   = product.get("goods_thumb") or product.get("goods_img") or ""
                prod_price   = parse_amount(product.get("salePrice") or {})
                prod_display = fmt_amount(product.get("salePrice"), symbol)
                if cart_id:
                    try:
                        api_delete_cart_items([cart_id], country)
                    except Exception:
                        _enqueue_cleanup([cart_id], country)
        except Exception:
            pass

    # ── Build variants from what we got (either from main detail or ATC)
    if sku_entries:
        # Use per-sku price if we have it, otherwise fall back to prod_price
        for s in sku_entries:
            variants.append({
                "sku_code":    s["sku_code"],
                "color":       s.get("color", ""),
                "color_img":   "",
                "size":        s.get("size", ""),
                "stock":       s.get("stock", 0),
                "in_stock":    s.get("in_stock", False),
                "price":       prod_price,
                "price_display": prod_display,
            })

    has_sizes  = any(v.get("size")  for v in variants)
    has_colors = len(set(v.get("color") for v in variants if v.get("color"))) > 1
    unique_sizes  = list(dict.fromkeys(v["size"] for v in variants if v.get("size")))
    unique_colors = list(dict.fromkeys(v["color"] for v in variants if v.get("color")))
    default_sku   = first_sku

    return jsonify({
        "goods_id":           goods_id,
        "name":               prod_name,
        "image":              prod_image,
        "country":            country,
        "currency":           currency,
        "symbol":             symbol,
        "sale_price":         prod_price,
        "sale_display":       prod_display,
        "stock":              variants[0]["stock"] if variants else "?",
        "is_on_sale":         False,
        "free_shipping":      False,
        "shipping_time":      "",
        "coupons":            [],
        "coupons_unavailable": True,
        "variants":           variants,
        "has_colors":         has_colors,
        "has_sizes":          has_sizes,
        "unique_colors":      [{"name": c, "img": ""} for c in unique_colors],
        "unique_sizes":       unique_sizes,
        "default_sku":        default_sku,
    })


@app.route("/api/item-info", methods=["POST"])
def item_info():
    """
    Product lookup using a two-step approach:
    1. STATIC endpoint (get_goods_detail_static_data_v2) — works for ALL products.
       Returns: name, image, price, SKUs/variants.
    2. REALTIME endpoint (get_goods_detail_realtime_data) — tried for coupons only.
       Returns: cmpCouponInfo. May fail for some products (836000) — that's OK.
    """
    body     = request.get_json(silent=True) or {}
    raw      = body.get("query", "").strip()
    country  = body.get("country", "PH").upper()

    goods_id = extract_goods_id(raw)
    if not goods_id:
        return jsonify({"error": "Could not extract a product ID from your input."}), 400

    currency = CURRENCY_MAP.get(country, "PHP")
    symbol   = SYMBOL_MAP.get(currency, "₱")

    # ── STEP 1: Get basic product info ───────────────────────────────────────
    # Try in order: static endpoint → search endpoint
    # Both are reliable and don't have the 836000 issue of the realtime endpoint.
    sinfo      = {}
    prod_name  = f"Product #{goods_id}"
    prod_image = ""
    sale_raw   = {}
    sale_price = 0.0
    is_on_sale = False
    stock      = "?"
    variant_data = {"variants": [], "has_colors": False, "has_sizes": False,
                    "unique_colors": [], "unique_sizes": [], "default_sku": ""}

    # Try static endpoint
    try:
        static = api_product_static(goods_id, country)
        if str(static.get("code")) == "0":
            sinfo      = static.get("info") or {}
            sale_raw   = sinfo.get("sale_price") or sinfo.get("retail_price") or {}
            sale_price = parse_amount(sale_raw)
            prod_name  = sinfo.get("goods_name") or prod_name
            prod_image = sinfo.get("goods_img")  or sinfo.get("original_img") or ""
            is_on_sale = sinfo.get("is_on_sale") == "1"
            stock      = sinfo.get("stock", "?")
            variant_data = parse_variants(sinfo)
    except Exception:
        pass

    # If static failed, try search endpoint (works for ALL products)
    if not sinfo:
        try:
            sp = api_search_product(goods_id, country)
            if sp:
                sale_raw   = sp.get("salePrice") or sp.get("retailPrice") or {}
                sale_price = parse_amount(sale_raw)
                prod_name  = sp.get("goods_name") or prod_name
                prod_image = sp.get("goods_img")  or ""
                is_on_sale = bool(sp.get("is_on_sale"))
                stock      = sp.get("stock", "?")
                # fromSkuCode is the default SKU from search results
                default_sku = sp.get("fromSkuCode") or ""
                if default_sku:
                    variant_data = {
                        "variants":      [{"sku_code": default_sku, "color": "", "color_img": "",
                                          "size": "", "stock": int(stock) if str(stock).isdigit() else 0,
                                          "in_stock": True, "price": sale_price, "price_display": fmt_amount(sale_raw, symbol)}],
                        "has_colors":    False,
                        "has_sizes":     False,
                        "unique_colors": [],
                        "unique_sizes":  [],
                        "default_sku":   default_sku,
                    }
        except Exception:
            pass

    if not sinfo and not variant_data.get("default_sku"):
        return jsonify({"error": f"Product #{goods_id} not found. Check the product ID and try again."}), 400

    # ── STEP 2: Realtime endpoint — for coupons only (failure is OK) ─────────
    coupons           = []
    free_shipping     = False
    shipping_time     = ""
    coupons_unavailable = False

    try:
        realtime = api_product_detail(goods_id, country)
        if str(realtime.get("code")) == "0":
            rinfo       = realtime.get("info") or {}
            free_shipping = rinfo.get("isProductShippingFree") == "1"
            shipping_time = rinfo.get("shipping_time_information", "")
            # Use realtime price if available (more accurate)
            rt_price = parse_amount(rinfo.get("sale_price") or {})
            if rt_price > 0:
                sale_price = rt_price
                sale_raw   = rinfo.get("sale_price") or sale_raw
            coupon_list = (rinfo.get("cmpCouponInfo") or {}).get("cmpCouponInfoList") or []
            for c in coupon_list:
                if c.get("isValid") != 1:
                    continue
                for rule in (c.get("rules") or []):
                    disc_str  = rule.get("discount", "")
                    thresh    = rule.get("threshold", "No Min. Buy")
                    min_order = parse_threshold(thresh)
                    calc      = calc_coupon_discount(disc_str, sale_price, min_order)
                    is_fs     = (c.get("businessExtension") or {}).get("productDetail", {}).get("isFreeShipping") == "1"
                    coupons.append({
                        "code":          c.get("coupon", ""),
                        "coupon_type":   (c.get("couponType") or {}).get("name", "Coupon"),
                        "discount_str":  disc_str,
                        "threshold_str": thresh,
                        "min_order":     min_order,
                        "is_free_ship":  is_fs,
                        "tip":           (c.get("businessExtension") or {}).get("productDetail", {}).get("newCouponShowTip", ""),
                        **calc,
                        "savings_pct": round(calc["discount"] / sale_price * 100, 1) if sale_price and calc["discount"] else 0,
                    })
            coupons.sort(key=lambda x: (-int(x["eligible"]), -x["discount"]))
        else:
            coupons_unavailable = True   # realtime failed — coupons unknown
    except Exception:
        coupons_unavailable = True

    # ── STEP 3: Get name/image via ATC if static didn't return good ones ──────
    if (not prod_image or prod_name == f"Product #{goods_id}") and ATC_GW_AUTH and ATC_ANTI_IN and variant_data.get("default_sku"):
        try:
            atc_body = (f"isAppointMall=&mall_code=1&quantity=1&sceneFlag="
                        f"&skuMallCode=1&fromPageName=goodsDetailAddToCart"
                        f"&goods_id={goods_id}&sku_code={variant_data['default_sku']}")
            ov = {"content-type": "application/x-www-form-urlencoded",
                  "x-gw-auth": _cred("ATC_GW_AUTH") or ATC_GW_AUTH,
                  "anti-in":   _cred("ATC_ANTI_IN") or ATC_ANTI_IN}
            atc = rq.post(f"{API_HOST}/order/add_to_cart",
                          params={"goods_id": goods_id}, data=atc_body,
                          headers=headers(country, ov), timeout=5, verify=False).json()
            if str(atc.get("code")) == "0":
                cart_obj = ((atc.get("info") or {}).get("cart") or {})
                product  = cart_obj.get("product") or {}
                prod_name  = product.get("goods_name") or prod_name
                prod_image = product.get("goods_thumb") or prod_image
                cart_id    = cart_obj.get("id")
                if cart_id:
                    try:    api_delete_cart_items([cart_id], country)
                    except: _enqueue_cleanup([cart_id], country)
        except Exception:
            pass

    return jsonify({
        "goods_id":           goods_id,
        "name":               prod_name,
        "image":              prod_image,
        "country":            country,
        "currency":           currency,
        "symbol":             symbol,
        "sale_price":         sale_price,
        "sale_display":       fmt_amount(sale_raw, symbol),
        "stock":              stock,
        "is_on_sale":         is_on_sale,
        "free_shipping":      free_shipping,
        "shipping_time":      shipping_time,
        "coupons":            coupons,
        "coupons_unavailable": coupons_unavailable,
        **variant_data,
    })


@app.route("/api/checkout", methods=["POST"])
def do_checkout():
    """
    Cart price simulation using ONLY the product detail API.
    No cart/checkout API calls — avoids 836000 security restriction
    on account-write endpoints from a remote server.
    """
    body    = request.get_json(silent=True) or {}
    items   = body.get("items", [])
    country = body.get("country", "PH").upper()

    if not items:
        return jsonify({"error": "No items in cart."}), 400

    currency = CURRENCY_MAP.get(country, "PHP")
    symbol   = SYMBOL_MAP.get(currency, "₱")

    item_results    = []
    all_coupons_map = {}
    all_free_ship   = True

    for item in items:
        gid = str(item.get("goods_id", ""))
        qty = max(1, int(item.get("qty", 1)))
        if not gid:
            continue

        dinfo = {}
        try:
            det   = api_product_detail(gid, country)
            if str(det.get("code")) == "0":
                dinfo = det.get("info") or {}
            # If 836000, silently continue — use price from cart item below
        except Exception:
            pass   # Non-fatal — fall back to stored item price

        sale_raw   = dinfo.get("sale_price") or {}
        # Use product detail price if available, otherwise use price stored in virtual cart
        sale_price = parse_amount(sale_raw) or float(item.get("price") or 0)
        free_ship  = dinfo.get("isProductShippingFree") == "1"
        if not free_ship:
            all_free_ship = False

        item_results.append({
            "goods_id":      gid,
            "sku_code":      item.get("sku_code", ""),
            "qty":           qty,
            "name":          item.get("name", f"Product #{gid}"),
            "image":         item.get("image", ""),
            "sale_price":    sale_price,
            "price_display": fmt_amount(sale_raw, symbol),
            "color":         item.get("color", ""),
            "size":          item.get("size", ""),
            "free_shipping": free_ship,
            "stock":         dinfo.get("stock", "?"),
        })

        cpns = (dinfo.get("cmpCouponInfo") or {}).get("cmpCouponInfoList") or []
        for c in cpns:
            code = c.get("coupon", "")
            if code and c.get("isValid") == 1:
                all_coupons_map[code] = c

    if not item_results:
        return jsonify({"error": "No valid items found."}), 400

    subtotal    = round(sum(r["sale_price"] * r["qty"] for r in item_results), 2)
    grand_total = subtotal

    # ── Try the real checkout API (for shipping, points, official totals)
    # Requires ATC_GW_AUTH + ATC_ANTI_IN to be set in Railway env vars
    checkout_data  = {}
    added_cart_ids = []
    used_real_checkout = False

    if ATC_GW_AUTH and ATC_ANTI_IN:
        try:
            for item in item_results:
                if not item.get("sku_code"):
                    continue
                atc = api_add_to_cart(item["goods_id"], item["sku_code"], item["qty"], country)
                if str(atc.get("code")) == "0":
                    cart_obj = ((atc.get("info") or {}).get("cart") or {})
                    cart_id  = cart_obj.get("id")
                    if cart_id:
                        added_cart_ids.append(cart_id)
                        _enqueue_cleanup([cart_id], country)   # 30-min safety net
                        product = cart_obj.get("product") or {}
                        item["name"]  = product.get("goods_name") or item["name"]
                        item["image"] = product.get("goods_thumb") or item["image"]
            if added_cart_ids:
                co = api_checkout(country)
                if str(co.get("code")) == "0":
                    checkout_data      = co.get("info") or {}
                    used_real_checkout = True
        except Exception:
            pass
        finally:
            # Immediate cleanup — remove from queue on success
            if added_cart_ids:
                try:
                    api_delete_cart_items(added_cart_ids, country)
                    with _cleanup_lock:
                        for cid in added_cart_ids:
                            _cleanup_queue.pop(cid, None)
                except Exception:
                    pass  # 30-min fallback will handle it

    # ── Shipping (real from checkout or calculated)
    if used_real_checkout:
        shipping_raw  = checkout_data.get("shippingPrice") or {}
        shipping_cost = parse_amount(shipping_raw)
        shipping_free = shipping_cost == 0.0
        ffi           = checkout_data.get("freight_free_info") or {}
        need_more     = parse_amount(ffi.get("shipping_price_diff")) if not shipping_free else 0.0
        point_info    = checkout_data.get("point") or {}
        avail_points  = int(point_info.get("total_point") or 0)
        max_pt_disc   = parse_amount(point_info.get("pointPrice"))
        points_tip    = point_info.get("useTip", "")
        total_info    = checkout_data.get("total_price_info") or {}
        grand_total   = parse_amount(total_info.get("grandTotalPrice")) or subtotal
        sorted_official = [r for r in (checkout_data.get("sorted_price") or []) if r.get("show") == 1]
    else:
        FREE_SHIP_THRESHOLD = 249.0
        shipping_free = all_free_ship or subtotal >= FREE_SHIP_THRESHOLD
        shipping_cost = 0.0 if shipping_free else 49.0
        need_more     = round(max(FREE_SHIP_THRESHOLD - subtotal, 0), 2) if not shipping_free else 0.0
        avail_points  = 0
        max_pt_disc   = 0.0
        points_tip    = ""
        sorted_official = []

    coupon_options = []
    for code, c in all_coupons_map.items():
        for rule in (c.get("rules") or []):
            disc_str   = rule.get("discount", "")
            thresh_str = rule.get("threshold", "No Min. Buy")
            min_order  = parse_threshold(thresh_str)
            calc       = calc_coupon_discount(disc_str, subtotal, min_order)
            is_fs      = (c.get("businessExtension") or {}).get("productDetail", {}).get("isFreeShipping") == "1"
            tip        = (c.get("businessExtension") or {}).get("productDetail", {}).get("newCouponShowTip", "")
            final_total = grand_total if calc["type"] == "free_shipping" else round(max(grand_total - calc["discount"], 0), 2)
            coupon_options.append({
                "code":          code,
                "coupon_type":   (c.get("couponType") or {}).get("name", "Coupon"),
                "discount_str":  disc_str,
                "threshold_str": thresh_str,
                "min_order":     min_order,
                "is_free_ship":  is_fs,
                "tip":           tip,
                **calc,
                "final_total":   final_total,
                "savings_pct":   round(calc["discount"] / grand_total * 100, 1) if grand_total and calc["discount"] else 0,
            })

    coupon_options.sort(key=lambda x: (-int(x["eligible"]), -x["discount"]))
    best_coupon = next((c for c in coupon_options if c["eligible"] and c["discount"] > 0), None)

    sorted_price = sorted_official or [
        {"type": "origin",   "local_name": "Subtotal:",    "price_with_symbol": f"{symbol}{subtotal:.2f}", "show": 1},
        {"type": "shipping", "local_name": "Shipping Fee:", "price_with_symbol": "FREE" if shipping_free else f"{symbol}{shipping_cost:.2f}", "show": 1},
        {"type": "coupon",   "local_name": "Best Coupon:", "price_with_symbol": f"-{symbol}{best_coupon['discount']:.2f}" if best_coupon else f"{symbol}0", "show": 1},
        {"type": "total",    "local_name": "Grand Total:", "price_with_symbol": f"{symbol}{(best_coupon['final_total'] if best_coupon else grand_total):.2f}", "show": 1},
    ]

    return jsonify({
        "country":             country,
        "currency":            currency,
        "symbol":              symbol,
        "items":               item_results,
        "subtotal":            subtotal,
        "subtotal_display":    f"{symbol}{subtotal:.2f}",
        "grand_total":         grand_total,
        "grand_total_display": f"{symbol}{grand_total:.2f}",
        "used_real_checkout":  used_real_checkout,
        "cleanup_notice":      True,   # tell frontend to show 30-min notice
        "shipping": {
            "free":              shipping_free,
            "cost":              shipping_cost,
            "cost_display":      "FREE" if shipping_free else f"{symbol}{shipping_cost:.2f}",
            "need_more_for_free": need_more,
            "need_more_display": f"{symbol}{need_more:.2f}" if need_more > 0 else "",
        },
        "points": {
            "available":            avail_points,
            "max_discount":         max_pt_disc,
            "max_discount_display": f"{symbol}{max_pt_disc:.2f}",
            "ratio_tip":            points_tip,
            "has_points":           avail_points > 0 and max_pt_disc > 0,
        },
        "sorted_price":  sorted_price,
        "coupons":       coupon_options,      # all coupons for user selection
        "best_coupon":   best_coupon,
        "coupon_count":  len(coupon_options),
    })


@app.route("/health")
def health():
    ok = bool(TOKEN and ARMOR_TOKEN and GW_AUTH and ADDRESS_ID)
    return jsonify({"status": "ok", "credentials_set": ok, "address_id": ADDRESS_ID})


@app.route("/debug")
def debug():
    """Quick credential check — open /debug in browser."""
    ok = bool(TOKEN and ARMOR_TOKEN and GW_AUTH and ADDRESS_ID)
    atc_ok = bool(ATC_GW_AUTH and ATC_ANTI_IN)
    return jsonify({
        "status": "ok",
        "credentials_set": ok,
        "atc_credentials_set": atc_ok,
        "address_id": ADDRESS_ID,
        "usage": {
            "product_debug_working": "/debug/product?goods_id=470311441&country=PH",
            "product_debug_failing": "/debug/product?goods_id=492039801&country=PH",
            "product_debug_custom":  "/debug/product?goods_id=YOUR_ID&country=PH",
        },
        "env_check": {
            "TOKEN_set":    bool(TOKEN),
            "ARMOR_set":    bool(ARMOR_TOKEN),
            "GW_AUTH_set":  bool(GW_AUTH),
            "SMDEVICE_set": bool(SMDEVICE_ID),
            "COOKIE_set":   bool(COOKIE),
            "ATC_GW_set":   bool(ATC_GW_AUTH),
            "ATC_ANTI_set": bool(ATC_ANTI_IN),
            "TOKEN_last12": TOKEN[-12:] if TOKEN else "MISSING",
            "DEVICE_ID":    DEVICE_ID[:36] if DEVICE_ID else "MISSING",
        },
        "cleanup_queue_size": len(_cleanup_queue),
    })


@app.route("/debug/product")
def debug_product():
    """
    Comprehensive product debug endpoint.
    Tests every API call in sequence and reports exactly what fails and why.

    Usage:
      /debug/product?goods_id=470311441&country=PH
      /debug/product?goods_id=YOUR_ID&country=TH
    """
    country  = request.args.get("country", "PH").upper()
    goods_id = request.args.get("goods_id", "492039801").strip()
    report   = {"goods_id": goods_id, "country": country, "steps": [], "diagnosis": []}

    def step(name, status, code=None, msg=None, detail=None):
        s = {"step": name, "status": status}
        if code  is not None: s["shein_code"] = str(code)
        if msg:               s["shein_msg"]  = msg
        if detail:            s["detail"]     = detail
        report["steps"].append(s)
        return s

    # ── Step 1: Product detail with full params
    try:
        params_full = _product_detail_params(goods_id, country)
        resp1 = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                       params=params_full, headers=headers(country),
                       timeout=15, verify=False)
        r1 = resp1.json()
        code1 = str(r1.get("code", "?"))
        info1 = r1.get("info") or {}
        if code1 == "0":
            step("product_detail_full_params", "✅ OK", code=code1,
                 detail={
                     "sale_price": (info1.get("sale_price") or {}).get("amountWithSymbol"),
                     "stock": info1.get("stock"),
                     "sku_count": len((info1.get("multiLevelSaleAttribute") or {}).get("sku_list") or []),
                     "coupon_count": len(((info1.get("cmpCouponInfo") or {}).get("cmpCouponInfoList")) or []),
                     "timezone_used": params_full["timeZone"],
                 })
        else:
            step("product_detail_full_params", "❌ FAILED", code=code1,
                 msg=r1.get("msg") or "",
                 detail={"timezone_used": params_full["timeZone"],
                         "hint": "836000 = security/param mismatch, try different country"})
    except Exception as exc:
        step("product_detail_full_params", "💥 EXCEPTION", msg=str(exc))
        r1 = {}; code1 = "exc"; info1 = {}

    # ── Step 2: Retry with different mallCode values
    if code1 != "0":
        tz = TIMEZONE_MAP.get(country, "Asia/Manila")
        worked = False
        for mall_code in ["0", "", "2"]:
            label = f"retry_mallCode={repr(mall_code)}"
            try:
                attempt = dict(params_full)
                attempt["mallCode"] = mall_code
                attempt["isUserSelectedMallCode"] = "1" if mall_code else "0"
                r2 = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                            params=attempt, headers=headers(country),
                            timeout=15, verify=False).json()
                code2 = str(r2.get("code", "?"))
                if code2 == "0":
                    step(label, f"✅ OK — mallCode={repr(mall_code)} worked!", code=code2)
                    info1 = r2.get("info") or {}
                    worked = True
                    break
                else:
                    step(label, "❌ FAILED", code=code2, msg=r2.get("msg") or "")
            except Exception as exc:
                step(label, "💥 EXCEPTION", msg=str(exc))
        if not worked:
            try:
                bare = {
                    "goods_id": goods_id, "sourceFrom": "goods_detail",
                    "visitNumOfDay": "1", "isShowMall": "1",
                    "isPaidMember": "0", "timeZone": tz,
                }
                r3 = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                            params=bare, headers=headers(country),
                            timeout=15, verify=False).json()
                code3 = str(r3.get("code", "?"))
                if code3 == "0":
                    step("retry_bare_params", "✅ OK (bare params worked)", code=code3)
                    info1 = r3.get("info") or {}
                else:
                    step("retry_bare_params", "❌ FAILED", code=code3, msg=r3.get("msg") or "")
            except Exception as exc:
                step("retry_bare_params", "💥 EXCEPTION", msg=str(exc))

        # ── Try removing more headers progressively (armortoken may be the culprit)
        for label, pop_keys in [
            ("no_gw_auth",         ["x-gw-auth", "anti-in"]),
            ("no_sig_headers",     ["x-gw-auth", "anti-in", "x-cs-random"]),
            ("no_armortoken",      ["x-gw-auth", "anti-in", "x-cs-random", "armortoken"]),
            ("no_all_sec_headers", ["x-gw-auth", "anti-in", "x-cs-random", "armortoken", "x-ad-flag"]),
        ]:
            try:
                h_test = dict(headers(country))
                for k in pop_keys:
                    h_test.pop(k, None)
                r_t = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                             params=params_full, headers=h_test,
                             timeout=15, verify=False).json()
                ct = str(r_t.get("code", "?"))
                if ct == "0":
                    step(label, f"✅ OK — removing {pop_keys} fixed it!", code=ct)
                    info1 = r_t.get("info") or {}
                    break
                else:
                    step(label, "❌ FAILED", code=ct, msg=r_t.get("msg") or "",
                         detail={"removed_headers": pop_keys})
            except Exception as exc:
                step(label, "💥 EXCEPTION", msg=str(exc))

    # ── Step 3: Add-to-cart test (requires ATC creds)
    test_sku = ""
    if info1:
        mls = info1.get("multiLevelSaleAttribute") or {}
        sku_list = mls.get("sku_list") or []
        test_sku = sku_list[0].get("sku_code") if sku_list else ""

    if not ATC_GW_AUTH or not ATC_ANTI_IN:
        step("add_to_cart", "⚠ SKIPPED", detail={"reason": "ATC_GW_AUTH or ATC_ANTI_IN not set in Railway env vars"})
    elif not test_sku:
        step("add_to_cart", "⚠ SKIPPED", detail={"reason": "No SKU found — product detail must succeed first"})
    else:
        try:
            atc = api_add_to_cart(goods_id, test_sku, 1, country)
            atc_code = str(atc.get("code", "?"))
            cart_obj = ((atc.get("info") or {}).get("cart") or {})
            cart_id  = cart_obj.get("id")
            prod     = cart_obj.get("product") or {}
            if atc_code == "0":
                step("add_to_cart", "✅ OK", code=atc_code, detail={
                    "cart_id": cart_id,
                    "product_name": prod.get("goods_name", "")[:80],
                    "sku_used": test_sku,
                })
                if cart_id:
                    try:
                        api_delete_cart_items([cart_id], country)
                        step("cart_cleanup", "✅ OK", detail={"cart_id": cart_id})
                    except Exception as exc:
                        step("cart_cleanup", "⚠ FAILED", msg=str(exc),
                             detail={"cart_id": cart_id, "note": "30-min auto-cleanup will handle this"})
                        _enqueue_cleanup([cart_id], country)
            else:
                step("add_to_cart", "❌ FAILED", code=atc_code,
                     msg=atc.get("msg") or "",
                     detail={"sku_used": test_sku,
                             "hint": "836000 = ATC creds may need refreshing (re-capture add-to-cart request)"})
        except Exception as exc:
            step("add_to_cart", "💥 EXCEPTION", msg=str(exc))

    # ── Diagnosis summary
    statuses = [s["status"] for s in report["steps"]]
    if all("✅" in s for s in statuses):
        report["diagnosis"].append("✅ All systems working for this product.")
    else:
        for s in report["steps"]:
            if "❌" in s["status"] or "💥" in s["status"]:
                code = s.get("shein_code", "")
                if code == "836000":
                    report["diagnosis"].append(
                        f"❌ {s['step']}: 836000 — "
                        + ("Try a different country. " if "product_detail" in s["step"] else "Refresh ATC_GW_AUTH and ATC_ANTI_IN from a new add-to-cart capture.")
                    )
                elif "💥" in s["status"]:
                    report["diagnosis"].append(f"💥 {s['step']}: Exception — {s.get('shein_msg','')}")
                else:
                    report["diagnosis"].append(f"❌ {s['step']}: code={code} — {s.get('shein_msg','')}")

    return jsonify(report)


@app.route("/admin/refresh", methods=["GET", "POST"])
def admin_refresh():
    """
    Credential refresh page.
    GET:  Shows the paste form.
    POST: Parses a raw HTTP request dump and updates in-memory credentials instantly.
          No Railway redeploy needed — takes effect immediately for all products.
    """
    secret = os.environ.get("ADMIN_SECRET", "")

    if request.method == "POST":
        provided = (request.form.get("secret") or "").strip()
        if secret and provided != secret:
            return "<h2 style='font-family:monospace;color:red'>Wrong secret.</h2>", 403

        raw = request.form.get("raw", "").strip()
        if not raw:
            return "<h2 style='font-family:monospace;color:red'>No data pasted.</h2>", 400

        parsed = _parse_raw_headers(raw)
        if not parsed:
            return "<h2 style='font-family:monospace;color:red'>No headers found. Make sure you pasted the full raw HTTP request.</h2>", 400

        updated = []
        for k, v in parsed.items():
            _live_creds[k] = v
            updated.append(k)

        # If this looks like a product-detail request, also set as ATC fallback
        if "GW_AUTH" in parsed:
            _live_creds["ATC_GW_AUTH"] = parsed["GW_AUTH"]
        if "ANTI_IN" in parsed:
            _live_creds["ATC_ANTI_IN"] = parsed["ANTI_IN"]

        html = f"""<!DOCTYPE html><html><head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:monospace;background:#0a0c10;color:#00ff88;padding:24px;max-width:600px;margin:0 auto}}
h2{{color:#00ff88}}ul{{margin:12px 0}}li{{margin:4px 0}}
a{{color:#00e5ff;text-decoration:none}}a:hover{{text-decoration:underline}}</style></head><body>
<h2>✅ Credentials Updated</h2>
<p>Updated {len(updated)} field(s) in memory:</p>
<ul>{"".join(f"<li>{k}</li>" for k in updated)}</ul>
<p style="color:#ffcc00">⚡ Takes effect immediately — no redeploy needed.</p>
<p><a href="/">← Back to Price Checker</a> &nbsp;|&nbsp; <a href="/debug/product">Test it now</a></p>
</body></html>"""
        return html

    # GET — show paste form
    html = f"""<!DOCTYPE html><html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Refresh Credentials</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:monospace;background:#0a0c10;color:#c8d4e0;padding:20px;min-height:100vh}}
.wrap{{max-width:640px;margin:0 auto}}
h1{{color:#00e5ff;font-size:18px;margin-bottom:4px}}
p{{font-size:12px;color:#4a5568;margin-bottom:16px}}
label{{font-size:12px;color:#00e5ff;display:block;margin-bottom:6px;letter-spacing:1px;text-transform:uppercase}}
textarea{{width:100%;background:#040608;color:#8be9fd;border:1px solid #1e2530;border-radius:6px;padding:12px;font-size:11px;font-family:monospace;resize:vertical;min-height:220px;outline:none}}
textarea:focus{{border-color:#00e5ff}}
input[type=text]{{width:100%;background:#040608;color:#8be9fd;border:1px solid #1e2530;border-radius:6px;padding:10px 12px;font-size:12px;font-family:monospace;outline:none;margin-bottom:16px}}
input[type=text]:focus{{border-color:#00e5ff}}
button{{background:#00e5ff;color:#000;border:none;border-radius:6px;padding:12px 24px;font-size:13px;font-weight:700;cursor:pointer;width:100%;margin-top:12px;font-family:monospace}}
button:hover{{background:#33ecff}}
.steps{{background:#0f1218;border:1px solid #1e2530;border-radius:8px;padding:14px;margin-bottom:20px;font-size:12px;line-height:1.8}}
.steps span{{color:#00e5ff}}
a{{color:#4a5568;font-size:11px;display:block;margin-top:12px}}
</style></head><body><div class="wrap">
<h1>🔑 Refresh Credentials</h1>
<p>Updates GW_AUTH, ANTI_IN, CS_RANDOM, RULEIDS instantly — no Railway redeploy needed.</p>
<div class="steps">
<span>How to capture:</span><br>
1. Open SHEIN app → tap any product page<br>
2. In your HTTP capture tool, find the request to:<br>
&nbsp;&nbsp;<code style="color:#ffcc00">api-service.shein.com/product/get_goods_detail_realtime_data</code><br>
3. Copy the <b>full raw request</b> (all headers + URL line)<br>
4. Paste it below and click Refresh
</div>
{'<label>Admin Secret</label><input type="text" name="secret" form="rf" placeholder="Enter secret..." autocomplete="off">' if secret else ''}
<form id="rf" method="POST">
<input type="hidden" name="secret" value="">
<label>Paste Raw HTTP Request</label>
<textarea name="raw" placeholder="POST /product/get_goods_detail_realtime_data?... HTTP/2&#10;host: api-service.shein.com&#10;x-gw-auth: a=vhOvoN...&#10;anti-in: 2_4.2.3_...&#10;..."></textarea>
<button type="submit">⚡ Refresh Credentials</button>
</form>
<a href="/">← Back to Price Checker</a>
</div>
<script>
// Wire the secret input to the hidden form field
const si = document.querySelector('input[name=secret]:not([type=hidden])');
if(si) si.addEventListener('input', () => document.querySelector('input[name=secret][type=hidden]').value = si.value);
</script>
</body></html>"""
    return html


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
