from __future__ import annotations

import base64
import os
import re
from io import BytesIO
from typing import Any, Dict, Optional

import requests
from PIL import Image, ImageEnhance, ImageOps

from agents.tools.api_tools import verify_invoice as _verify_invoice

# -----------------------------
# OCR (Vision REST by default)
# -----------------------------


def ocr_document_text_rest(image_path: str) -> Dict[str, Any]:
    """
    OCR an image using Cloud Vision REST API with an API key.

    Requires:
      VISION_API_KEY="..."
    """
    api_key = (os.getenv("VISION_API_KEY") or "").strip()
    if not api_key:
        return {"text": "", "error": "VISION_API_KEY not set"}

    try:
        with open(image_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode("utf-8")

        payload = {
            "requests": [
                {
                    "image": {"content": content_b64},
                    "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
                }
            ]
        }

        resp = requests.post(
            "https://vision.googleapis.com/v1/images:annotate",
            params={"key": api_key},
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        r0 = (data.get("responses") or [{}])[0]
        if "error" in r0 and r0["error"].get("message"):
            return {"text": "", "error": r0["error"]["message"]}

        ann = r0.get("fullTextAnnotation") or {}
        txt = (ann.get("text") or "").strip()
        return {"text": txt, "error": None}
    except Exception as e:
        return {"text": "", "error": str(e)}


def ocr_document_text_grpc(image_path: str) -> Dict[str, Any]:
    """
    OCR using google-cloud-vision gRPC client (fallback).
    """
    try:
        from google.cloud import vision  # lazy import (grpc)

        client = vision.ImageAnnotatorClient()
        with open(image_path, "rb") as f:
            content = f.read()

        image = vision.Image(content=content)
        resp = client.document_text_detection(image=image)

        if resp.error and resp.error.message:
            return {"text": "", "error": resp.error.message}

        txt = (resp.full_text_annotation.text or "").strip()
        return {"text": txt, "error": None}
    except Exception as e:
        return {"text": "", "error": str(e)}


def ocr_document_text(image_path: str) -> Dict[str, Any]:
    """
    Default OCR entrypoint.
    Uses REST first; if no key is configured, falls back to gRPC client.
    """
    out = ocr_document_text_rest(image_path)
    if out.get("error") is None and (out.get("text") or "").strip():
        return out

    if (os.getenv("VISION_API_KEY") or "").strip() == "":
        return ocr_document_text_grpc(image_path)

    return out


def _vision_annotate_rest_from_bytes(image_bytes: bytes) -> Dict[str, Any]:
    api_key = (os.getenv("VISION_API_KEY") or "").strip()
    if not api_key:
        return {"error": "VISION_API_KEY not set", "response": None}

    content_b64 = base64.b64encode(image_bytes).decode("utf-8")
    payload = {
        "requests": [{
            "image": {"content": content_b64},
            "features": [{"type": "DOCUMENT_TEXT_DETECTION"}],
        }]
    }

    resp = requests.post(
        "https://vision.googleapis.com/v1/images:annotate",
        params={"key": api_key},
        json=payload,
        timeout=60,
    )

    if resp.status_code != 200:
        return {"error": f"{resp.status_code}: {resp.text}", "response": None}

    data = resp.json()
    r0 = (data.get("responses") or [{}])[0]
    if "error" in r0 and r0["error"].get("message"):
        return {"error": r0["error"]["message"], "response": r0}

    return {"error": None, "response": r0}


def _premier_store_roi_variants_pil(image_path: str) -> list[Image.Image]:
    img = Image.open(image_path)
    img = ImageOps.exif_transpose(img)

    w, h = img.size
    roi = img.crop((0, 0, int(w * 0.45), int(h * 0.20)))
    roi = roi.resize((roi.size[0] * 4, roi.size[1] * 4), Image.Resampling.LANCZOS)

    g = ImageOps.grayscale(roi)
    variants: list[Image.Image] = []

    a = ImageEnhance.Contrast(g).enhance(3.2)
    a = ImageEnhance.Sharpness(a).enhance(2.6)
    variants.append(a)

    b = ImageOps.autocontrast(g)
    b = ImageEnhance.Sharpness(b).enhance(2.8)
    variants.append(b)

    c = ImageEnhance.Contrast(g).enhance(3.4)
    c = c.point(lambda p: 255 if p > 150 else 0)
    variants.append(c)

    d = ImageEnhance.Contrast(g).enhance(3.4)
    d = d.point(lambda p: 255 if p > 170 else 0)
    variants.append(d)

    e = ImageOps.invert(g)
    e = ImageEnhance.Contrast(e).enhance(3.4)
    e = e.point(lambda p: 255 if p > 150 else 0)
    variants.append(e)

    return variants


def _ocr_text_from_bytes(image_bytes: bytes) -> Dict[str, Any]:
    """
    Lightweight OCR helper using the REST path (respects VISION_API_KEY).
    """
    return _vision_annotate_rest_from_bytes(image_bytes)


def _find_n_code_in_text(txt: str) -> Optional[str]:
    """Return a normalized N######## code from text, tolerant to spaces/hyphens/O→0."""
    if not txt:
        return None

    def _norm(raw: str) -> Optional[str]:
        cleaned = re.sub(r"[^A-Z0-9]", "", raw.upper())
        if not cleaned.startswith("N"):
            return None
        cand = "N" + cleaned[1:].replace("O", "0")
        if re.fullmatch(r"N\d{8,14}", cand):
            return cand
        return None

    candidates = []
    pattern = re.compile(r"N[\s\-]*\d(?:[\s\-]*\d){7,13}", re.IGNORECASE)
    for match in pattern.findall(txt):
        c = _norm(match)
        if c:
            candidates.append(c)

    compact = re.sub(r"[^A-Z0-9]", "", txt.upper())
    for match in re.findall(r"N[0-9O]{8,14}", compact):
        c = _norm(match)
        if c:
            candidates.append(c)

    if not candidates:
        return None

    # Prefer the longest (to avoid partial reads); fall back to last seen
    candidates.sort(key=lambda x: (len(x), x))
    return candidates[-1]


def _ocr_bottom_store_code(image_path: str) -> Optional[str]:
    """
    Focused OCR on variable footer bands to pick up N########### store codes.
    Uses multi-band crops and aggressive upscaling/binarization to survive small text.
    """
    api_key_present = (os.getenv("VISION_API_KEY") or "").strip() != ""
    if not api_key_present:
        return None

    try:
        img = Image.open(image_path)
        img = ImageOps.exif_transpose(img)
        w, h = img.size

        y_bands = [
            (int(h * 0.55), h),
            (int(h * 0.65), h),
            (int(h * 0.72), h),
        ]

        for (y0, y1) in y_bands:
            crop = img.crop((int(w * 0.25), y0, int(w * 0.98), y1))
            crop = crop.resize((crop.size[0] * 4, crop.size[1] * 4), Image.Resampling.LANCZOS)
            g = ImageOps.grayscale(crop)

            variants: list[Image.Image] = []
            base = ImageOps.autocontrast(g)

            c1 = ImageEnhance.Contrast(base).enhance(3.0)
            c1 = ImageEnhance.Sharpness(c1).enhance(2.4)
            variants.append(c1)

            for thr in (130, 160, 190):
                c = ImageEnhance.Contrast(base).enhance(3.4)
                c = c.point(lambda p, t=thr: 255 if p > t else 0)
                variants.append(c)

                inv = ImageOps.invert(base)
                inv = ImageEnhance.Contrast(inv).enhance(3.2)
                inv = inv.point(lambda p, t=thr: 255 if p > t else 0)
                variants.append(inv)

            for v in variants:
                buf = BytesIO()
                v.save(buf, format="PNG")
                resp = _ocr_text_from_bytes(buf.getvalue())
                if resp.get("error"):
                    continue
                ann = (resp.get("response") or {}).get("fullTextAnnotation") or {}
                txt = (ann.get("text") or "").strip()
                code = _find_n_code_in_text(txt)
                if code:
                    return code
    except Exception:
        return None
    return None


def _pil_to_png_bytes(im: Image.Image) -> bytes:
    buf = BytesIO()
    im.save(buf, format="PNG")
    return buf.getvalue()

def _premier_qr_store_roi_variants_pil(image_path: str) -> list[Image.Image]:
    """
    Crop multiple bottom-right bands (QR + code underneath) and generate enhanced variants.
    Placement of the N-code shifts vertically across invoices, so we sweep several y-ranges.
    """
    img = Image.open(image_path)
    img = ImageOps.exif_transpose(img)

    w, h = img.size
    # Sweep a bit wider to catch QRs that are closer to center or edge
    x0_options = [int(w * 0.48), int(w * 0.55), int(w * 0.62), int(w * 0.68)]
    y_ranges = [
        (int(h * 0.52), int(h * 0.86)),
        (int(h * 0.60), int(h * 0.92)),
        (int(h * 0.68), int(h * 0.98)),
    ]

    variants: list[Image.Image] = []

    for x0 in x0_options:
        for (y0, y1) in y_ranges:
            roi = img.crop((x0, y0, w, y1))
            roi = roi.resize((roi.size[0] * 4, roi.size[1] * 4), Image.Resampling.LANCZOS)

            g = ImageOps.grayscale(roi)

            # Base variants: plain auto-contrast + high-contrast binarized + inverted binarized
            base = ImageOps.autocontrast(g)

            c1 = ImageEnhance.Contrast(base).enhance(3.2)
            c1 = ImageEnhance.Sharpness(c1).enhance(2.6)
            variants.append(c1)

            for thr in (130, 160, 190):
                c = ImageEnhance.Contrast(base).enhance(3.6)
                c = c.point(lambda p, t=thr: 255 if p > t else 0)
                variants.append(c)

                inv = ImageOps.invert(base)
                inv = ImageEnhance.Contrast(inv).enhance(3.4)
                inv = inv.point(lambda p, t=thr: 255 if p > t else 0)
                variants.append(inv)

            if len(variants) >= 30:  # guardrail to avoid too many Vision calls
                return variants

    return variants

def _extract_premier_n_store_code_from_text(txt: str) -> Optional[str]:
    """
    Extract N + digits store code from OCR text near QR, fixing O->0 after N.
    """
    if not txt:
        return None
    t = txt.upper()
    m = re.search(r"\bN[0-9O]{8,14}\b", t)
    if not m:
        compact = re.sub(r"[^A-Z0-9]", "", t)
        m = re.search(r"N[0-9O]{8,14}", compact)
        if not m:
            return None
    cand = m.group(0)
    cand = "N" + cand[1:].replace("O", "0")
    if re.fullmatch(r"N\d{8,14}", cand):
        return cand
    return None

def extract_premier_store_code_below_qr(image_path: str) -> Optional[str]:
    """
    Strictly extract Premier store code from QR/footer region.
    """
    for roi_img in _premier_qr_store_roi_variants_pil(image_path):
        out = _vision_annotate_rest_from_bytes(_pil_to_png_bytes(roi_img))
        if out.get("error") or not out.get("response"):
            continue
        ann = (out["response"].get("fullTextAnnotation") or {})
        txt = (ann.get("text") or "").strip()
        code = _find_n_code_in_text(txt) or _extract_premier_n_store_code_from_text(txt)
        if code:
            return code
    return None


def _ocr_digits_under_anchor(roi_img: Image.Image, anchor_box: dict) -> Optional[str]:
    """
    Crop a tight region under the INV/DATE anchor, upscale, binarize, OCR again, and return 6–8 digit store code.
    """
    x0 = max(anchor_box["x0"] - 220, 0)
    x1 = min(anchor_box["x1"] + 220, roi_img.size[0])
    y0 = min(anchor_box["y1"] + 10, roi_img.size[1])
    y1 = min(y0 + 420, roi_img.size[1])

    crop = roi_img.crop((x0, y0, x1, y1))
    crop = crop.resize((crop.size[0] * 5, crop.size[1] * 5), Image.Resampling.LANCZOS)

    for thr in (130, 150, 170, 190):
        c = crop
        if c.mode != "L":
            c = ImageOps.grayscale(c)

        c = ImageEnhance.Contrast(c).enhance(3.8)
        c = ImageEnhance.Sharpness(c).enhance(2.8)
        c = c.point(lambda p: 255 if p > thr else 0)

        out = _vision_annotate_rest_from_bytes(_pil_to_png_bytes(c))
        if out["error"] or not out["response"]:
            continue

        ann = (out["response"].get("fullTextAnnotation") or {})
        txt = (ann.get("text") or "").strip()

        trans = str.maketrans({
            "O": "0", "o": "0",
            "I": "1", "l": "1", "|": "1",
            "S": "5", "s": "5",
            "B": "8",
            "Z": "2",
        })
        cleaned = txt.translate(trans)
        cleaned = re.sub(r"[^0-9]", " ", cleaned)
        m = re.search(r"\b(\d{6,8})\b", cleaned)
        if m:
            return m.group(1)

    return None


def _extract_words_with_boxes(r0: Dict[str, Any]) -> list[dict]:
    """
    Word-level extraction with bounding boxes from fullTextAnnotation.
    """
    out = []
    ann = r0.get("fullTextAnnotation") or {}
    pages = ann.get("pages") or []
    if not pages:
        return out

    for page in pages:
        for block in page.get("blocks", []):
            for para in block.get("paragraphs", []):
                for word in para.get("words", []):
                    txt = "".join(sym.get("text", "") for sym in word.get("symbols", []))
                    bb = word.get("boundingBox", {}).get("vertices", [])
                    if len(bb) < 4:
                        continue
                    xs = [v.get("x", 0) for v in bb]
                    ys = [v.get("y", 0) for v in bb]
                    x0, x1 = min(xs), max(xs)
                    y0, y1 = min(ys), max(ys)
                    out.append({
                        "text": txt,
                        "u": (txt or "").upper(),
                        "x0": x0, "y0": y0, "x1": x1, "y1": y1,
                        "cx": (x0 + x1) / 2.0,
                        "cy": (y0 + y1) / 2.0,
                    })
    return out


def extract_premier_store_code_under_inv_date(image_path: str) -> Optional[str]:
    """
    Strict: return store code only if found under INV/DATE using 2-pass OCR.
    """
    roi_variants = _premier_store_roi_variants_pil(image_path)

    for roi_img in roi_variants:
        out = _vision_annotate_rest_from_bytes(_pil_to_png_bytes(roi_img))
        if out["error"] or not out["response"]:
            continue

        words = _extract_words_with_boxes(out["response"])
        if not words:
            continue

        invs = [w for w in words if ("INV" in w["u"]) or ("INY" in w["u"])]
        dates = [w for w in words if "DATE" in w["u"]]

        anchor_pair = None
        best = 10**18
        for iw in invs:
            for dw in dates:
                if abs(iw["cy"] - dw["cy"]) < 30:
                    d = abs(iw["cx"] - dw["cx"])
                    if d < best:
                        best = d
                        anchor_pair = (iw, dw)

        if not anchor_pair:
            continue

        iw, dw = anchor_pair
        anchor_box = {
            "x0": min(iw["x0"], dw["x0"]),
            "y0": min(iw["y0"], dw["y0"]),
            "x1": max(iw["x1"], dw["x1"]),
            "y1": max(iw["y1"], dw["y1"]),
        }

        store = _ocr_digits_under_anchor(roi_img, anchor_box)
        if store:
            return store

    return None


def _digits_only(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def _norm_inv(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", (s or "")).upper()


def detect_invoice_type(ocr_text: str) -> str:
    """Return 'salesflo' | 'premier' | 'unknown'."""
    t = (ocr_text or "").upper()
    if "SALESFLO" in t or re.search(r"\b[A-Z0-9]{0,8}INV\d{3,}\b", t):
        return "salesflo"
    if "PREMIER" in t or "SUPPLY ID" in t or "CNIC" in t:
        return "premier"
    return "unknown"


def extract_salesflo_fields(ocr_text: str) -> Dict[str, Optional[str]]:
    """Best-effort extraction for Salesflo invoices (invoice_number + store_code)."""
    t = (ocr_text or "")
    t_up = t.upper()
    header = "\n".join(t.splitlines()[:70])
    header_up = header.upper()

    store = _find_n_code_in_text(t)

    inv = None
    m = re.search(r"INVOICE\s*NO\s*[:\-]?\s*([A-Z]?\d{3,6}INV\d{3,})", header_up)
    if m:
        inv = _norm_inv(m.group(1))
    else:
        m2 = re.search(r"\b([A-Z]?\d{3,6}INV\d{3,})\b", header_up)
        if m2:
            inv = _norm_inv(m2.group(1))

    if inv and re.fullmatch(r"\d{3,6}INV\d{3,}", inv):
        inv = "D" + inv[1:] if inv.startswith("0") else "D" + inv

    return {
        "invoice_type": "salesflo",
        "invoice_number": inv,
        "store_code": store,
    }


def extract_premier_fields(ocr_text: str) -> Dict[str, Optional[str]]:
    """Best-effort extraction for Premier invoices (store code only)."""
    _ = ocr_text or ""
    return {
        "invoice_type": "premier",
        "store_code": None,  # force QR-based extraction
    }


def extract_invoice_fields(
    ocr_text: str, invoice_type_hint: Optional[str] = None
) -> Dict[str, Optional[str]]:
    itype = invoice_type_hint or detect_invoice_type(ocr_text)
    if itype == "salesflo":
        return extract_salesflo_fields(ocr_text)
    if itype == "premier":
        return extract_premier_fields(ocr_text)

    a = extract_salesflo_fields(ocr_text)
    b = extract_premier_fields(ocr_text)
    score_a = sum(1 for k, v in a.items() if k != "invoice_type" and v)
    score_b = sum(1 for k, v in b.items() if k != "invoice_type" and v)
    return a if score_a >= score_b else b


def build_salesflo_payload(
    ocr_text: str,
    *,
    mobile_number: Optional[str] = None,
) -> Dict[str, Any]:
    tenant_id = (os.getenv("TENANT_ID") or "").strip()
    extracted = extract_salesflo_fields(ocr_text)

    return {
        "tenant_id": tenant_id,
        "mobile_number": mobile_number,
        "invoice_type": "salesflo",
        "store_codes": [extracted["store_code"]] if extracted.get("store_code") else [],
        "invoice_number": extracted.get("invoice_number"),
    }


def build_premier_payload(
    *,
    image_path: str,
    mobile_number: Optional[str] = None,
    ocr_text: Optional[str] = None,
    store_code_override: Optional[str] = None,
) -> Dict[str, Any]:
    tenant_id = (os.getenv("TENANT_ID") or "").strip()

    if ocr_text is None:
        ocr = ocr_document_text(image_path)
        ocr_text = ocr.get("text") or ""

    extracted = extract_premier_fields(ocr_text)

    # Strict: only take store code from QR/footer (override wins if provided)
    store_code = store_code_override or extract_premier_store_code_below_qr(image_path)

    # Fallback: allow full-page OCR search to catch codes when footer crop fails
    if not store_code:
        store_code = _find_n_code_in_text(ocr_text) or _extract_premier_n_store_code_from_text(ocr_text)

    return {
        "tenant_id": tenant_id,
        "mobile_number": mobile_number,
        "invoice_type": "premier",
        "store_codes": [store_code] if store_code else [],
    }


def build_invoice_payload_from_image(
    image_path: str,
    *,
    mobile_number: Optional[str],
) -> Dict[str, Any]:
    """
    Extract invoice fields from a single image and return a verification-ready payload.
    """
    result: Dict[str, Any] = {
        "payload": None,
        "invoice_type": None,
        "missing": [],
        "error": None,
        "ocr_text": "",
    }

    ocr = ocr_document_text(image_path)
    text = (ocr.get("text") or "").strip()
    if not text:
        result["error"] = ocr.get("error") or "No text detected"
        return result

    result["ocr_text"] = text
    invoice_type_hint = detect_invoice_type(text)
    extracted = extract_invoice_fields(text, invoice_type_hint=invoice_type_hint)
    invoice_type = (extracted.get("invoice_type") or invoice_type_hint or "unknown").lower()

    if invoice_type == "salesflo":
        payload = build_salesflo_payload(text, mobile_number=mobile_number)
        # Fallback: try focused footer OCR to pick up N######## store code if missing
        if not (payload.get("store_codes") or []):
            fallback_store = _ocr_bottom_store_code(image_path)
            if fallback_store:
                payload["store_codes"] = [fallback_store]

        missing = []
        if not payload.get("invoice_number"):
            missing.append("invoice_number")
        if not (payload.get("store_codes") or []):
            missing.append("store_code")
    else:
        payload = build_premier_payload(
            image_path=image_path,
            mobile_number=mobile_number,
            ocr_text=text,
            store_code_override=extracted.get("store_code"),
        )
        invoice_type = "premier"
        missing = []
        if not (payload.get("store_codes") or []):
            missing.append("store_code")

    payload["invoice_type"] = invoice_type
    result["payload"] = payload
    result["invoice_type"] = invoice_type
    result["missing"] = missing
    result["error"] = ocr.get("error")
    return result


def verify_extracted_invoice(
    *,
    tenant_id: str,
    mobile_number: Optional[str],
    invoice_type: str,
    invoice_number: Optional[str],
    store_codes: Optional[list[str]],
) -> Dict[str, Any]:
    """
    Verify extracted fields via the repo's verify_invoice tool (no CNIC required).
    """
    return _verify_invoice(
        tenant_id=tenant_id,
        mobile_number=mobile_number,
        invoice_type=invoice_type,
        invoice_number=invoice_number,
        store_codes=store_codes,
    )
