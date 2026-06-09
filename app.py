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
import urllib3
from flask import Flask, request, jsonify, render_template

import requests as rq

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
        "appcountry":          APPCOUNTRY,
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
        "smdeviceid":          SMDEVICE_ID,
        "armortoken":          ARMOR_TOKEN,
        "token":               TOKEN,
        "x-gw-auth":           GW_AUTH,
        "x-cs-random":         CS_RANDOM,
        "anti-in":             ANTI_IN,
        "x-ad-flag":           AD_FLAG,
        "ugid":                UGID,
        "newuid":              SORTUID,
        "sortuid":             SORTUID,
        "usercountry":         country.upper(),
        "version":             APP_VERSION,
        "appversion":          APP_VERSION,
        "cookie":              COOKIE,
        "ruleids":             RULEIDS,
        "user-agent":          f"Shein {APP_VERSION} Android 11 {DEVICE_INFO} {APPCOUNTRY} en {SORTUID}",
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

def api_product_detail(goods_id: str, country: str = "PH") -> dict:
    """Fetch realtime product data (price + applicable coupons).
    All params — including empty ones — must match the original app request exactly."""
    params = {
        "priorityMallType":          "1",
        "sceneFromPage":             "",
        "isRelatedColorNeedPromotion": "",
        "promotionId":               "",
        "isAppointMall":             "0",
        "useSupplyGoods":            "",
        "isUserSelectedMallCode":    "0",
        "sceneFlag":                 "",
        "mallCode":                  "1",
        "localSiteQueryFlag":        "0",
        "orderPrice":                "",
        "isHideNotSatisfied":        "",
        "isSizeGatherTag":           "",
        "hasReportMember":           "0",
        "sourceFrom":                "goods_detail",
        "promotionLogoType":         "",
        "promotionType":             "",
        "isHidePromotionTip":        "",
        "goods_id":                  goods_id,
        "timeZone":                  "Asia/Manila",
        "isHideEstimatePriceInfo":   "",
        "popComponentEntry":         "",
        "bundledPurchaseMainGoodsId": "",
        "visitNumOfDay":             "1",
        "isShowMall":                "0",
        "isPaidMember":              "0",
        "billno":                    "",
        "promotionProductMark":      "",
    }
    resp = rq.get(f"{API_HOST}/product/get_goods_detail_realtime_data",
                  params=params, headers=headers(country), timeout=15, verify=False)
    return resp.json()


def api_add_to_cart(goods_id: str, sku_code: str, qty: int = 1,
                    country: str = "PH") -> dict:
    """Add item to cart. Returns full response with cart.id and product info."""
    body = (f"isAppointMall=&mall_code=1&quantity={qty}&sceneFlag="
            f"&skuMallCode=1&fromPageName=goodsDetailAddToCart"
            f"&goods_id={goods_id}&sku_code={sku_code}")
    h = headers(country, {"content-type": "application/x-www-form-urlencoded"})
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
    h = headers(country, {
        "content-type":    "application/json; charset=utf-8",
        "frontend-scene":  "page_checkout",
        "sessionid":       session_id,
    })
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

        variants.append({
            "sku_code":  sku_code,
            "color":     color,
            "color_img": color_img,
            "size":      size,
            "stock":     stock,
            "in_stock":  stock > 0,
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


@app.route("/api/item-info", methods=["POST"])
def item_info():
    """
    Quick lookup: return item name, image, price, variants, and applicable coupons.
    Does NOT add to cart.
    """
    body     = request.get_json(silent=True) or {}
    raw      = body.get("query", "").strip()
    country  = body.get("country", "PH").upper()

    goods_id = extract_goods_id(raw)
    if not goods_id:
        return jsonify({"error": "Could not extract a product ID from your input."}), 400

    try:
        detail = api_product_detail(goods_id, country)
    except Exception as exc:
        return jsonify({"error": f"Request failed: {exc}"}), 502

    if str(detail.get("code")) != "0":
        shein_code = detail.get("code", "?")
        shein_msg  = detail.get("msg") or detail.get("message") or ""
        return jsonify({"error": shein_msg or f"SHEIN API error (code={shein_code}). Check /debug for details."}), 400

    info      = detail.get("info") or {}
    sale_raw  = info.get("sale_price") or {}
    currency  = CURRENCY_MAP.get(country, "PHP")
    symbol    = SYMBOL_MAP.get(currency, "₱")
    sale_price = parse_amount(sale_raw)

    # Coupons applicable to this product from owner's wallet
    coupon_list = (info.get("cmpCouponInfo") or {}).get("cmpCouponInfoList") or []
    coupons = []
    for c in coupon_list:
        if c.get("isValid") != 1:
            continue
        for rule in (c.get("rules") or []):
            disc_str      = rule.get("discount", "")
            thresh_str    = rule.get("threshold", "No Min. Buy")
            min_order     = parse_threshold(thresh_str)
            calc          = calc_coupon_discount(disc_str, sale_price, min_order)
            is_free_ship  = (c.get("businessExtension") or {}).get("productDetail", {}).get("isFreeShipping") == "1"
            coupons.append({
                "code":         c.get("coupon", ""),
                "coupon_type":  (c.get("couponType") or {}).get("name", "Coupon"),
                "discount_str": disc_str,
                "threshold_str":thresh_str,
                "min_order":    min_order,
                "is_free_ship": is_free_ship,
                "tip":          (c.get("businessExtension") or {}).get("productDetail", {}).get("newCouponShowTip", ""),
                **calc,
                "savings_pct": round(calc["discount"] / sale_price * 100, 1) if sale_price and calc["discount"] else 0,
            })
    coupons.sort(key=lambda x: (-int(x["eligible"]), -x["discount"]))

    # Parse variant matrix from multiLevelSaleAttribute.sku_list
    variant_data = parse_variants(info)

    return jsonify({
        "goods_id":     goods_id,
        "country":      country,
        "currency":     currency,
        "symbol":       symbol,
        "sale_price":   sale_price,
        "sale_display": fmt_amount(sale_raw, symbol),
        "stock":        info.get("stock", "?"),
        "is_on_sale":   info.get("is_on_sale") == "1",
        "free_shipping":info.get("isProductShippingFree") == "1",
        "shipping_time":info.get("shipping_time_information", ""),
        "coupons":      coupons,
        **variant_data,    # variants, has_colors, has_sizes, unique_colors, unique_sizes, default_sku
    })


@app.route("/api/checkout", methods=["POST"])
def do_checkout():
    """
    Full checkout simulation for a list of items.
    Steps:
      1. Add each item to owner's cart
      2. Call checkout endpoint → get full price breakdown
      3. Delete all added cart items
      4. Return enriched result
    """
    body    = request.get_json(silent=True) or {}
    items   = body.get("items", [])    # [{goods_id, sku_code, qty, name, price, image}, ...]
    country = body.get("country", "PH").upper()

    if not items:
        return jsonify({"error": "No items in cart."}), 400

    currency = CURRENCY_MAP.get(country, "PHP")
    symbol   = SYMBOL_MAP.get(currency, "₱")

    added_cart_ids   = []
    item_results     = []
    all_coupons_map  = {}   # code → coupon info (merged across items)

    # ── Step 1: Add all items to cart
    for item in items:
        gid      = str(item.get("goods_id", ""))
        sku      = str(item.get("sku_code", ""))
        qty      = max(1, int(item.get("qty", 1)))

        if not gid or not sku:
            continue

        try:
            atc = api_add_to_cart(gid, sku, qty, country)
        except Exception as exc:
            return jsonify({"error": f"Add to cart failed for {gid}: {exc}"}), 502

        if str(atc.get("code")) != "0":
            shein_code = atc.get("code", "?")
            shein_msg  = atc.get("msg") or atc.get("message") or ""
            return jsonify({"error": f"Add to cart failed [code={shein_code}]: {shein_msg or 'no message'} — item {gid}"}), 400

        cart_info = (atc.get("info") or {}).get("cart") or {}
        cart_id   = cart_info.get("id")
        product   = cart_info.get("product") or {}

        if cart_id:
            added_cart_ids.append(cart_id)

        item_results.append({
            "goods_id":    gid,
            "sku_code":    sku,
            "qty":         qty,
            "cart_id":     cart_id,
            "name":        product.get("goods_name") or item.get("name", f"Product {gid}"),
            "image":       product.get("goods_thumb") or item.get("image", ""),
            "sale_price":  parse_amount(product.get("salePrice")),
            "price_display": fmt_amount(product.get("salePrice"), symbol),
            "color":       next((a.get("attr_value_name") for a in (product.get("sku_sale_attr") or [])
                                 if a.get("attr_name_en") == "Color"), ""),
            "size":        next((a.get("attr_value_name") for a in (product.get("sku_sale_attr") or [])
                                 if a.get("attr_name_en") == "Size"), ""),
        })

        # Get applicable coupons for this item
        try:
            det  = api_product_detail(gid, country)
            dinfo = det.get("info") or {}
            cpns  = (dinfo.get("cmpCouponInfo") or {}).get("cmpCouponInfoList") or []
            for c in cpns:
                code = c.get("coupon", "")
                if code and c.get("isValid") == 1:
                    all_coupons_map[code] = c
        except Exception:
            pass   # Non-fatal; continue without coupon data for this item

    if not added_cart_ids:
        return jsonify({"error": "No items could be added to the cart."}), 400

    # ── Step 2: Call checkout
    checkout_data = {}
    try:
        co = api_checkout(country)
        if str(co.get("code")) == "0":
            checkout_data = co.get("info") or {}
    except Exception:
        pass   # Non-fatal; continue with partial data

    # ── Step 3: Clean up — delete all added cart items
    try:
        api_delete_cart_items(added_cart_ids, country)
    except Exception:
        pass   # Best effort cleanup

    # ── Step 4: Build response
    subtotal      = sum(r["sale_price"] * r["qty"] for r in item_results)
    shipping_raw  = checkout_data.get("shippingPrice") or {}
    shipping_cost = parse_amount(shipping_raw)
    shipping_free = shipping_cost == 0.0

    ffi           = checkout_data.get("freight_free_info") or {}
    shipping_diff = parse_amount(ffi.get("shipping_price_diff"))
    need_more_for_free = shipping_diff if not shipping_free else 0.0

    # Points
    point_info     = checkout_data.get("point") or {}
    avail_points   = int(point_info.get("total_point") or 0)
    max_point_disc = parse_amount(point_info.get("pointPrice"))
    points_ratio   = point_info.get("useTip", "")

    # Sorted price (official breakdown rows)
    sorted_price   = [r for r in (checkout_data.get("sorted_price") or []) if r.get("show") == 1]

    # Grand total (no coupon)
    total_info     = checkout_data.get("total_price_info") or {}
    grand_total    = parse_amount(total_info.get("grandTotalPrice")) or subtotal

    # Build coupon comparison
    coupon_options = []
    for code, c in all_coupons_map.items():
        rules = c.get("rules") or []
        for rule in rules:
            disc_str   = rule.get("discount", "")
            thresh_str = rule.get("threshold", "No Min. Buy")
            min_order  = parse_threshold(thresh_str)
            calc       = calc_coupon_discount(disc_str, subtotal, min_order)
            is_fs      = (c.get("businessExtension") or {}).get("productDetail", {}).get("isFreeShipping") == "1"
            tip        = (c.get("businessExtension") or {}).get("productDetail", {}).get("newCouponShowTip", "")

            # Final total with coupon
            if calc["type"] == "free_shipping" and calc["eligible"]:
                final_total = grand_total  # price same, just ship free
            else:
                final_total = round(max(grand_total - calc["discount"], 0), 2)

            coupon_options.append({
                "code":         code,
                "coupon_type":  (c.get("couponType") or {}).get("name", "Coupon"),
                "discount_str": disc_str,
                "threshold_str":thresh_str,
                "min_order":    min_order,
                "is_free_ship": is_fs,
                "tip":          tip,
                **calc,
                "final_total":  final_total,
                "savings_pct":  round(calc["discount"] / grand_total * 100, 1) if grand_total and calc["discount"] else 0,
            })

    coupon_options.sort(key=lambda x: (-int(x["eligible"]), -x["discount"]))
    best_coupon = next((c for c in coupon_options if c["eligible"] and c["discount"] > 0), None)

    return jsonify({
        "country":      country,
        "currency":     currency,
        "symbol":       symbol,
        "items":        item_results,
        "subtotal":     round(subtotal, 2),
        "subtotal_display": f"{symbol}{subtotal:.2f}",
        "grand_total":  grand_total,
        "grand_total_display": f"{symbol}{grand_total:.2f}",
        "shipping": {
            "free":         shipping_free,
            "cost":         shipping_cost,
            "cost_display": fmt_amount(shipping_raw, symbol),
            "need_more_for_free": need_more_for_free,
            "need_more_display":  f"{symbol}{need_more_for_free:.2f}" if need_more_for_free > 0 else "",
        },
        "points": {
            "available":     avail_points,
            "max_discount":  max_point_disc,
            "max_discount_display": f"{symbol}{max_point_disc:.2f}",
            "ratio_tip":     points_ratio,
        },
        "sorted_price":   sorted_price,
        "coupons":        coupon_options,
        "best_coupon":    best_coupon,
        "coupon_count":   len(coupon_options),
    })


@app.route("/health")
def health():
    ok = bool(TOKEN and ARMOR_TOKEN and GW_AUTH and ADDRESS_ID)
    return jsonify({"status": "ok", "credentials_set": ok, "address_id": ADDRESS_ID})


@app.route("/debug")
def debug():
    """
    Test endpoint — calls SHEIN product API with goods_id=470311441
    and returns the RAW response so you can diagnose any auth/config issues.
    Open this URL in your browser: /debug
    """
    country = request.args.get("country", "PH")
    goods_id = request.args.get("goods_id", "470311441")
    try:
        result = api_product_detail(goods_id, country)
        code   = str(result.get("code", "?"))
        msg    = result.get("msg", "")
        info   = result.get("info") or {}

        # Get first available sku_code for add-to-cart test
        mls      = info.get("multiLevelSaleAttribute") or {}
        sku_list = mls.get("sku_list") or []
        test_sku = sku_list[0].get("sku_code") if sku_list else ""

        # Test add-to-cart if sku available
        atc_result = {}
        if test_sku:
            try:
                atc = api_add_to_cart(goods_id, test_sku, 1, country)
                atc_code = str(atc.get("code", "?"))
                atc_msg  = atc.get("msg") or ""
                cart_id  = (atc.get("info") or {}).get("cart", {}).get("id")
                atc_result = {"code": atc_code, "msg": atc_msg, "cart_id": cart_id}
                # Clean up immediately
                if cart_id:
                    api_delete_cart_items([cart_id], country)
            except Exception as e:
                atc_result = {"error": str(e)}

        return jsonify({
            "shein_code":        code,
            "shein_msg":         msg,
            "goods_id_returned": info.get("goods_id"),
            "sale_price":        (info.get("sale_price") or {}).get("amountWithSymbol"),
            "stock":             info.get("stock"),
            "sku_count":         len(sku_list),
            "test_sku":          test_sku,
            "add_to_cart_test":  atc_result,
            "coupon_count":      len(((info.get("cmpCouponInfo") or {}).get("cmpCouponInfoList")) or []),
            "env_check": {
                "TOKEN_set":       bool(TOKEN),
                "ARMOR_set":       bool(ARMOR_TOKEN),
                "GW_AUTH_set":     bool(GW_AUTH),
                "SMDEVICE_set":    bool(SMDEVICE_ID),
                "COOKIE_set":      bool(COOKIE),
                "TOKEN_preview":   TOKEN[-12:] if TOKEN else "MISSING",
                "DEVICE_ID":       DEVICE_ID[:30] + "..." if len(DEVICE_ID) > 30 else DEVICE_ID,
            },
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
