"""Prism-native one-shot vehicle valuation pipeline.

Production flow:

1. One Cloud Tasks worker request owns the valuation from start to finish.
2. All first-party evidence starts concurrently.
3. Every evidence result is merged and persisted to the valuation job immediately.
4. Current Manheim MMR and AccuTrade Target Auction are collected before the remaining
    evidence. No baseline is published until the valuation model compares both anchors
    and selects the stronger starting point.
5. Browser evidence is obtained through Prism's existing /evidence/browser route.
   That route owns the persistent Playwright/AccuTrade session and semaphore and
   returns the screenshot/CARFAX as inline artifacts. This worker materializes those
   returned artifacts locally for the model pass, so it is safe even when Cloud Run
   sends /evidence/browser to a different instance.
6. AutoCheck is collected alongside AccuTrade/CARFAX and retained as structured VHR evidence.
7. One lightweight GMTV year/build classification followed by three valuation passes:
    pre-pass - select model years sharing the target's basic generation/build
      pass 1 - Gemini + Google Search curated public-market evidence
      pass 2 - valuation reasoning and explicit evidence-supported value ladder
    pass 3 - collector-backed adjustment audit plus strict final schema
8. Python normalizes the ladder arithmetic and persists a terminal complete/error row.

There is no detached valuation task, no baseline_waiting/value_market stage, and no
second browser implementation in this module.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import csv
import io
import inspect
import json
import logging
import os
import re
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

import aiohttp
import requests
import google.auth
from google import genai
from google.genai import types

try:
    from pypdf import PdfReader
except ImportError:  # Production installs pypdf from prism/requirements.txt.
    PdfReader = None

logger = logging.getLogger(__name__)

# Valuation model calls use a dedicated Vertex AI GLOBAL endpoint client.
# This keeps the rest of Prism free to use its existing regional client while allowing
# globally served models without changing Prism's existing regional clients.
WEBSEARCH_MODEL = os.environ.get("VALUATION_WEBSEARCH_MODEL", "gemini-2.5-flash")
VALUATION_MODEL = os.environ.get("VALUATION_MODEL", "gemini-2.5-flash")
VALUATION_VERTEX_LOCATION = "global"
VALUATION_VERTEX_PROJECT = (
    os.environ.get("VALUATION_VERTEX_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or os.environ.get("GCLOUD_PROJECT")
    or os.environ.get("GCP_PROJECT")
    or ""
)
_GLOBAL_VERTEX_CLIENT = None
GMTV_YEAR_WINDOW = int(os.environ.get("GMTV_YEAR_WINDOW", "5"))
GMTV_MAX_COMPS = int(os.environ.get("GMTV_MAX_COMPS", "50"))
GMTV_SALES_FILE = os.environ.get("GMTV_SALES_FILE", "3monthsales.csv")
GMTV_TIMEOUT = float(os.environ.get("VALUATION_GMTV_TIMEOUT", "15"))
MARKET_TREND_MAX_POINTS = int(os.environ.get("VALUATION_MARKET_TREND_MAX_POINTS", "20"))
MARKET_TREND_CONCURRENCY = int(os.environ.get("VALUATION_MARKET_TREND_CONCURRENCY", "6"))
MARKET_TREND_TIMEOUT = float(os.environ.get("VALUATION_MARKET_TREND_TIMEOUT", "75"))
GMTV_YEAR_FILTER_TIMEOUT = float(os.environ.get("VALUATION_GMTV_YEAR_FILTER_TIMEOUT", "30"))
GROUNDING_RESOLVE_CONCURRENCY = int(os.environ.get("VALUATION_GROUNDING_RESOLVE_CONCURRENCY", "8"))
GROUNDING_RESOLVE_TIMEOUT = float(os.environ.get("VALUATION_GROUNDING_RESOLVE_TIMEOUT", "6"))
GROUNDING_MAX_SOURCES = int(os.environ.get("VALUATION_GROUNDING_MAX_SOURCES", "30"))

PUBLIC_AUCTION_BASELINE_SOURCES = (
    ("Bring a Trailer", "bringatrailer.com", "enthusiast, collector, performance, and unusual vehicles"),
    ("Mecum Auctions", "mecum.com", "collector, muscle, truck, motorcycle, and specialty vehicles"),
    ("Cars & Bids", "carsandbids.com", "modern enthusiast vehicles"),
    ("Barrett-Jackson", "barrett-jackson.com", "collector and specialty vehicles"),
    ("RM Sotheby's", "rmsothebys.com", "high-end collector vehicles"),
    ("Bonhams Cars", "cars.bonhams.com", "collector and high-end vehicles"),
    ("Collecting Cars", "collectingcars.com", "enthusiast and collector vehicles"),
    ("PCARMARKET", "pcarmarket.com", "Porsche, exotic, and enthusiast vehicles"),
    ("Hemmings Auctions", "hemmings.com", "classic and collector vehicles"),
    ("Hagerty Marketplace", "hagerty.com", "classic and collector vehicles"),
    ("Broad Arrow Auctions", "broadarrowauctions.com", "collector vehicles"),
    ("Worldwide Auctioneers", "worldwideauctioneers.com", "classic and collector vehicles"),
    ("GAA Classic Cars", "gaaclassiccars.com", "classic, muscle, and collector vehicles"),
    ("eBay Motors sold listings", "ebay.com", "broad-market fallback with strict sold-status verification"),
)
PUBLIC_AUCTION_BASELINE_DOMAINS = {
    domain for _, domain, _ in PUBLIC_AUCTION_BASELINE_SOURCES
}
SPECIALTY_AUCTION_MAKES = {
    "ACURA NSX", "ALFA ROMEO", "ASTON MARTIN", "BENTLEY", "BUGATTI",
    "FERRARI", "FISKER", "KARMA", "LAMBORGHINI", "LOTUS", "LUCID",
    "MASERATI", "MAYBACH", "MCLAREN", "POLESTAR", "PORSCHE",
    "RIVIAN", "ROLLS ROYCE", "ROLLS-ROYCE",
}
SPECIALTY_AUCTION_TERMS = (
    "AMG", "ALPINA", "BLACK SERIES", "BLACKWING", "BRABUS", "COBRA", "COMPETITION",
    "CORVETTE", "DEMON", "EXOTIC", "GT-R", "GT3", "GT4", "GT500",
    "HELLCAT", "LFA", "LIMITED EDITION", "M SERIES", "M2", "M3", "M4", "M5",
    "M6", "M8", "MAYBACH", "NISMO", "NSX", "PLAID", "RAPTOR", "RAPTOR R",
    "RS3", "RS4", "RS5", "RS6", "RS7", "RS Q8", "SASQUATCH", "SHELBY",
    "SRT", "SUPRA", "TRACKHAWK", "TREMOR", "TRD PRO", "VIPER", "WIDEBODY",
    "Z06", "ZR1", "OFF ROAD", "OFF-ROAD",
)

MANHEIM_AUTH_TIMEOUT = float(os.environ.get("VALUATION_MANHEIM_AUTH_TIMEOUT", "15"))
MANHEIM_MMR_TIMEOUT = float(os.environ.get("VALUATION_MANHEIM_MMR_TIMEOUT", "15"))
MANHEIM_SALES_TIMEOUT = float(os.environ.get("VALUATION_MANHEIM_SALES_TIMEOUT", "25"))
MANHEIM_CONDITION_TIMEOUT = float(os.environ.get("VALUATION_MANHEIM_CONDITION_TIMEOUT", "15"))
JDPOWER_TIMEOUT = float(os.environ.get("VALUATION_JDPOWER_TIMEOUT", "20"))
BROWSER_TIMEOUT = float(os.environ.get("VALUATION_BROWSER_TIMEOUT", "300"))
# The CRM photo collector may classify newly attached photos before returning them.
CRM_PHOTOS_TIMEOUT = float(os.environ.get("VALUATION_CRM_PHOTOS_TIMEOUT", "150"))
CRM_PHOTO_MAX_IMAGES = int(os.environ.get("VALUATION_CRM_PHOTO_MAX_IMAGES", "50"))
CRM_PHOTO_ATTACH_LIMIT = int(os.environ.get("VALUATION_CRM_PHOTO_ATTACH_LIMIT", "8"))
WEBSEARCH_TIMEOUT = float(os.environ.get("VALUATION_WEBSEARCH_TIMEOUT", "240"))
VALUATION_TIMEOUT = float(os.environ.get("VALUATION_REASONING_TIMEOUT", "180"))
AUDIT_TIMEOUT = float(os.environ.get("VALUATION_AUDIT_TIMEOUT", "120"))

JDPOWER_SERVICE_URL = os.environ.get("JDPOWER_SERVICE_URL", "").rstrip("/")
JDPOWER_API_KEY = os.environ.get("JDPOWER_API_KEY", "")
PRISM_BASE_URL = os.environ.get("PRISM_URL", "https://prism.andrew-chat.com").rstrip("/")
BROWSER_EVIDENCE_URL = os.environ.get(
    "VALUATION_BROWSER_EVIDENCE_URL",
    f"{PRISM_BASE_URL}/evidence/browser",
).strip()
BROWSER_SERVICE_TOKEN = (
    os.environ.get("EVIDENCE_SERVICE_TOKEN")
    or os.environ.get("SA_AUTHENTICATION")
    or ""
)
EVIDENCE_DIR = Path(os.environ.get("EVIDENCE_DIR", "/tmp/evidence"))
MAX_ARTIFACT_BYTES = int(os.environ.get("EVIDENCE_MAX_ARTIFACT_BYTES", str(8 * 1024 * 1024)))

STAGE_ORDER = [
    "baseline",
    "mileage",
    "history",
    "condition",
    "factory_equipment",
    "aftermarket",
    "color",
    "market_heat",
    "retail_ceiling",
]

STAGE_LABELS = {
    "baseline": "Baseline",
    "mileage": "Mileage",
    "history": "History",
    "condition": "Condition",
    "factory_equipment": "Factory equipment",
    "aftermarket": "Aftermarket / mods",
    "color": "Colour",
    "market_heat": "Market heat",
    "retail_ceiling": "Retail ceiling check",
}

RESEARCH_ADJUSTMENT_STAGES = tuple(STAGE_ORDER[1:])

VERIFIED_PRICED_PAIR = "VERIFIED_PRICED_PAIR"
VERIFIED_DEALER_UNDERWRITING = "VERIFIED_DEALER_UNDERWRITING"
VERIFIED_LISTING_CONTEXT = "VERIFIED_LISTING_CONTEXT"
VERIFIED_MARKET_CONTEXT = "VERIFIED_MARKET_CONTEXT"
UNVERIFIED_CONTEXT = "UNVERIFIED_CONTEXT"

_PUBLIC_COMPARISON_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "found": {"type": "BOOLEAN"},
        "url": {"type": "STRING"},
        "price": {"type": "NUMBER"},
        "mileage": {"type": "NUMBER"},
        "build": {"type": "STRING"},
        "listing_status": {"type": "STRING"},
        "material_differences": {"type": "STRING"},
    },
    "required": [
        "found", "url", "price", "mileage", "build", "listing_status",
        "material_differences",
    ],
}

_PUBLIC_UNDERWRITING_FINDING_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "url": {"type": "STRING"},
        "source_type": {"type": "STRING"},
        "finding_role": {
            "type": "STRING",
            "enum": [
                "CLEAN_RETAIL", "IMPAIRED_RETAIL", "LEGAL", "FINANCING",
                "INSURANCE", "LIQUIDITY", "RECON", "OTHER",
            ],
        },
        "finding": {"type": "STRING"},
        "vehicle_scope": {"type": "STRING"},
        "amount_low": {"type": "NUMBER"},
        "amount_high": {"type": "NUMBER"},
        "observation_date": {"type": "STRING"},
    },
    "required": [
        "url", "source_type", "finding_role", "finding", "vehicle_scope",
        "amount_low", "amount_high", "observation_date",
    ],
}

_PUBLIC_DEALER_UNDERWRITING_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "verdict": {"type": "STRING", "enum": ["SUPPORTED", "NOT_SUPPORTED"]},
        "applicable": {"type": "BOOLEAN"},
        "impairment_type": {"type": "STRING"},
        "confirmed_target_fact": {"type": "STRING"},
        "target_fact_source": {"type": "STRING"},
        "dealer_question": {"type": "STRING"},
        "clean_retail_reference_low": {"type": "NUMBER"},
        "clean_retail_reference_high": {"type": "NUMBER"},
        "impaired_retail_exit_low": {"type": "NUMBER"},
        "impaired_retail_exit_high": {"type": "NUMBER"},
        "recommended_wholesale_adjustment_low": {"type": "NUMBER"},
        "recommended_wholesale_adjustment_high": {"type": "NUMBER"},
        "methodology": {"type": "STRING"},
        "limitations": {"type": "STRING"},
        "source_urls": {"type": "ARRAY", "items": {"type": "STRING"}},
        "findings": {
            "type": "ARRAY",
            "items": _PUBLIC_UNDERWRITING_FINDING_SCHEMA,
        },
    },
    "required": [
        "verdict", "applicable", "impairment_type", "confirmed_target_fact",
        "target_fact_source", "dealer_question", "clean_retail_reference_low",
        "clean_retail_reference_high", "impaired_retail_exit_low",
        "impaired_retail_exit_high", "recommended_wholesale_adjustment_low",
        "recommended_wholesale_adjustment_high", "methodology", "limitations",
        "source_urls", "findings",
    ],
}

_PUBLIC_ADJUSTMENT_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "verdict": {"type": "STRING", "enum": ["SUPPORTED", "NOT_SUPPORTED"]},
        "claim": {"type": "STRING"},
        "normal_comparison": _PUBLIC_COMPARISON_SCHEMA,
        "feature_comparison": _PUBLIC_COMPARISON_SCHEMA,
        "observed_difference": {"type": "NUMBER"},
        "research_summary": {"type": "STRING"},
        "wholesale_implication": {"type": "STRING"},
        "source_urls": {"type": "ARRAY", "items": {"type": "STRING"}},
        "dealer_underwriting": _PUBLIC_DEALER_UNDERWRITING_SCHEMA,
    },
    "required": [
        "verdict", "claim", "normal_comparison", "feature_comparison",
        "observed_difference", "research_summary", "wholesale_implication",
        "source_urls", "dealer_underwriting",
    ],
}

PUBLIC_RESEARCH_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "research_memo": {"type": "STRING"},
        "adjustments": {
            "type": "OBJECT",
            "properties": {
                stage_id: _PUBLIC_ADJUSTMENT_SCHEMA
                for stage_id in RESEARCH_ADJUSTMENT_STAGES
            },
            "required": list(RESEARCH_ADJUSTMENT_STAGES),
        },
    },
    "required": ["research_memo", "adjustments"],
}

_RUNTIME: dict[str, Any] = {
    "manheim_headers_provider": None,
    "manheim_current_mmr_collector": None,
    "manheim_historical_mmr_collector": None,
    "manheim_future_mmr_collector": None,
    "manheim_condition_collector": None,
    "manheim_vin_sales_collector": None,
    "manheim_ymm_sales_collector": None,
    "manheim_mm_sales_collector": None,
    "sales_collector": None,
    "crm_photos_collector": None,
    "job_updater": None,
    "websearch_runner": None,
    "web_client": None,
}


def configure_prism_valuation(
    *,
    manheim_headers_provider=None,
    manheim_current_mmr_collector=None,
    manheim_historical_mmr_collector=None,
    manheim_future_mmr_collector=None,
    manheim_condition_collector=None,
    manheim_vin_sales_collector=None,
    manheim_ymm_sales_collector=None,
    manheim_mm_sales_collector=None,
    sales_collector=None,
    crm_photos_collector=None,
    job_updater=None,
    websearch_runner=None,
    web_client=None,
) -> None:
    """Inject Prism's already-working local implementations once at startup."""

    values = {
        "manheim_headers_provider": manheim_headers_provider,
        "manheim_current_mmr_collector": manheim_current_mmr_collector,
        "manheim_historical_mmr_collector": manheim_historical_mmr_collector,
        "manheim_future_mmr_collector": manheim_future_mmr_collector,
        "manheim_condition_collector": manheim_condition_collector,
        "manheim_vin_sales_collector": manheim_vin_sales_collector,
        "manheim_ymm_sales_collector": manheim_ymm_sales_collector,
        "manheim_mm_sales_collector": manheim_mm_sales_collector,
        "sales_collector": sales_collector,
        "crm_photos_collector": crm_photos_collector,
        "job_updater": job_updater,
        "websearch_runner": websearch_runner,
        "web_client": web_client,
    }
    for key, value in values.items():
        if value is not None:
            _RUNTIME[key] = value


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _clean_vin(value: Any) -> str:
    return "" if value is None else str(value).strip().upper()


def _first(data: dict, *keys: str, default=None):
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return default


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    if hasattr(value, "dict"):
        try:
            return _jsonable(value.dict())
        except Exception:
            pass
    return str(value)


def _compact(value: Any, *, max_chars: int) -> str:
    text = json.dumps(_jsonable(value), default=str, ensure_ascii=False)
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "...[truncated]"


def _money(value: float) -> str:
    return f"${float(value):,.0f}"


def _signed(value: float) -> str:
    value = float(value or 0.0)
    if value == 0:
        return "$0"
    return f"{'+' if value > 0 else '-'}${abs(value):,.0f}"


async def _call(func: Callable, *args, **kwargs):
    if inspect.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    value = await asyncio.to_thread(func, *args, **kwargs)
    if inspect.isawaitable(value):
        return await value
    return value


async def _job_update(job_id: str, **fields) -> bool:
    updater = _RUNTIME.get("job_updater")
    if updater is None:
        raise RuntimeError("Prism valuation job_updater is not configured")
    result = await _call(updater, str(job_id), fields)
    if isinstance(result, bool):
        return result
    return str(result).strip().lower() in {"success", "true", "1", "ok"}


def _global_vertex_client():
    """Return one reusable google-genai Vertex client pinned to locations/global."""
    global _GLOBAL_VERTEX_CLIENT
    if _GLOBAL_VERTEX_CLIENT is not None:
        return _GLOBAL_VERTEX_CLIENT

    credentials, adc_project = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    project = VALUATION_VERTEX_PROJECT or adc_project
    if not project:
        raise RuntimeError(
            "Could not determine the Google Cloud project for the valuation global endpoint. "
            "Set VALUATION_VERTEX_PROJECT or GOOGLE_CLOUD_PROJECT."
        )

    _GLOBAL_VERTEX_CLIENT = genai.Client(
        vertexai=True,
        project=project,
        location=VALUATION_VERTEX_LOCATION,
        credentials=credentials,
        http_options=types.HttpOptions(api_version="v1"),
    )
    logger.info(
        "Initialized valuation Vertex GenAI client project=%s location=%s",
        project,
        VALUATION_VERTEX_LOCATION,
    )
    return _GLOBAL_VERTEX_CLIENT


# ---------------------------------------------------------------------------
# Evidence collectors
# ---------------------------------------------------------------------------


def _norm_vehicle_text(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", str(value or "").upper()).strip()


def _squash_vehicle_text(value: Any) -> str:
    """Drop punctuation and spacing so MERCEDES-BENZ / Mercedes Benz / MERCEDESBENZ collapse."""
    return re.sub(r"[^A-Z0-9]+", "", str(value or "").upper())


def _soft_vehicle_match(target: str, value: str) -> bool:
    """Containment in both directions, so SIERRA pulls SIERRA 1500 and the reverse."""
    if not target or not value:
        return False
    return target in value or value in target


def _specialty_auction_research(car: dict) -> dict[str, Any]:
    """Determine whether enthusiast/collector auction research is mandatory."""
    reasons = []
    year = _num(_first(car, "year", "YEAR", "Year", "ModelYear"))
    if year and year <= datetime.now().year - 25:
        reasons.append(f"classic vehicle ({int(year)} model year)")

    make = _norm_vehicle_text(_first(car, "make", "MAKE", "Make"))
    vehicle_text = _norm_vehicle_text(" ".join(
        str(_first(car, key, default="") or "")
        for key in (
            "make", "model", "trim", "series", "style", "body_style",
            "description", "edition", "package", "Package", "PACKAGE",
            "packages", "options", "equipment",
        )
    ))
    if make in {_norm_vehicle_text(value) for value in SPECIALTY_AUCTION_MAKES}:
        reasons.append(f"specialty/high-end make ({make.title()})")

    matched_terms = [
        term for term in SPECIALTY_AUCTION_TERMS
        if re.search(rf"(?<![A-Z0-9]){re.escape(term)}(?![A-Z0-9])", vehicle_text)
    ]
    if matched_terms:
        reasons.append("specialty/performance designation: " + ", ".join(matched_terms[:3]))

    known_value = next(
        (
            value for value in (
                _num(_first(car, "msrp", "MSRP", "original_msrp")),
                _num(_first(car, "retail_price", "asking_price", "list_price")),
            )
            if value is not None and value > 0
        ),
        None,
    )
    if known_value is not None and known_value >= 75_000:
        reasons.append(f"known price/MSRP is at least $75,000 ({_money(known_value)})")

    return {
        "required": bool(reasons),
        "reasons": reasons,
        "required_venues": ["Bring a Trailer", "Mecum Auctions"] if reasons else [],
    }


def _num(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    negative = text.startswith("(") and text.endswith(")")
    text = re.sub(r"[$,()\s]", "", text)
    try:
        result = float(text)
        return -result if negative else result
    except Exception:
        return None


def _sale_date_rank(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text[:19], fmt).timestamp()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _sale_date_iso(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text[:19], fmt).date().isoformat()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def _resolve_gmtv_sales_file() -> Path:
    configured = Path(GMTV_SALES_FILE)
    candidate = configured if configured.is_absolute() else Path(__file__).resolve().parent / configured
    if candidate.exists() and candidate.is_file():
        return candidate
    raise FileNotFoundError(f"GMTV sales export not found: {candidate}")


@lru_cache(maxsize=4)
def _load_gmtv_sales_rows(
    path_string: str, mtime_ns: int
) -> tuple[tuple[str, ...], tuple[dict[str, str], ...]]:
    del mtime_ns
    path = Path(path_string)
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            with path.open("r", encoding=encoding, newline="") as handle:
                reader = csv.DictReader(handle)
                return tuple(reader.fieldnames or []), tuple(dict(row) for row in reader)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Could not decode GMTV sales CSV: {path}")


def _csv_vhr_status(row: dict[str, str]) -> Optional[str]:
    flag = str(row.get("BadCarFax") or "").strip().upper()
    notes = str(row.get("Notes") or "").upper()
    if flag in {"TRUE", "1", "YES"}:
        return "bad"
    if re.search(r"\b(BAD|MINOR|MODERATE|SEVERE)\s+(CARFAX|AUTOCHECK)\b", notes):
        return "bad"
    if re.search(r"\b(ACCIDENT|STRUCTURAL DAMAGE|FRAME DAMAGE)\b", notes):
        return "bad"
    if flag in {"FALSE", "0", "NO"} or re.search(
        r"\bCLEAN\s+(CARFAX|AUTOCHECK)\b", notes
    ):
        return "clean"
    return None


def _same_year_make_model_vhr_profit(
    rows: tuple[dict[str, str], ...], target_year: int, target_make: str, target_model: str
) -> dict[str, Any]:
    groups: dict[str, list[float]] = {"bad": [], "clean": []}
    seen_vins: set[str] = set()
    for row in rows:
        row_year = _num(row.get("ModelYear"))
        if row_year is None or int(row_year) != target_year:
            continue
        if _squash_vehicle_text(row.get("Make")) != target_make:
            continue
        if _squash_vehicle_text(row.get("Model")) != target_model:
            continue
        status = _csv_vhr_status(row)
        net_profit = _num(row.get("NetAmount"))
        vin = str(row.get("Vin") or "").strip().upper()
        if status not in groups or net_profit is None or not vin or vin in seen_vins:
            continue
        groups[status].append(net_profit)
        seen_vins.add(vin)

    bad_average = sum(groups["bad"]) / len(groups["bad"]) if groups["bad"] else None
    clean_average = sum(groups["clean"]) / len(groups["clean"]) if groups["clean"] else None
    sufficient_sample = bool(groups["bad"] and groups["clean"])
    observed_gap = (
        clean_average - bad_average
        if bad_average is not None and clean_average is not None
        else None
    )
    recommended_deduction = (
        round(max(0.0, observed_gap) / 100.0) * 100.0
        if sufficient_sample and observed_gap is not None
        else 0.0
    )
    return {
        "scope": {
            "year": target_year,
            "make": target_make,
            "model": target_model,
            "model_match": "exact after punctuation and spacing normalization",
        },
        "metric": "NetAmount",
        "bad_vhr": {"count": len(groups["bad"]), "average_net_profit": bad_average},
        "clean_vhr": {"count": len(groups["clean"]), "average_net_profit": clean_average},
        "clean_minus_bad_average": observed_gap,
        "sufficient_sample": sufficient_sample,
        "recommended_bad_vhr_deduction": recommended_deduction,
    }


def _local_gmtv_sales_comps(
    path: Path,
    car: dict,
    comparable_years: Optional[list[int]] = None,
    year_filter: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    year_raw = _first(car, "year", "YEAR", "Year", "ModelYear")
    make_raw = _first(car, "make", "MAKE", "Make")
    model_raw = _first(car, "model", "MODEL", "Model")
    if not year_raw or not make_raw or not model_raw:
        raise ValueError("GMTV comps require year, make, and model")

    target_year = int(float(str(year_raw)))
    target_make = _squash_vehicle_text(make_raw)
    target_model = _squash_vehicle_text(model_raw)
    columns, rows = _load_gmtv_sales_rows(
        str(path.resolve()), path.stat().st_mtime_ns
    )
    required = {"Vin", "ModelYear", "Make", "Model", "SoldPrice", "SoldDate", "NetAmount", "BadCarFax"}
    missing = sorted(required - set(columns))
    if missing:
        raise ValueError(f"3monthsales.csv is missing required columns: {missing}")

    result = _csv_sales_comps(path.read_bytes(), car, comparable_years=comparable_years)
    result["source"] = "GMTV 3monthsales.csv"
    result["filters"]["source"] = "3monthsales.csv"
    result["year_build_filter"] = _jsonable(year_filter or {
        "comparable_years": [target_year],
        "reasoning": "No year classifier result; exact model year only.",
        "fallback": True,
    })
    result["same_year_make_model_vhr_net_profit"] = _same_year_make_model_vhr_profit(
        rows, target_year, target_make, target_model
    )
    return result


def _csv_field(row: dict[str, Any], *aliases: str) -> Any:
    normalized = {
        re.sub(r"[^a-z0-9]", "", str(key).lower()): value
        for key, value in row.items()
        if key
    }
    for alias in aliases:
        key = re.sub(r"[^a-z0-9]", "", alias.lower())
        if normalized.get(key) not in (None, ""):
            return normalized[key]
    return None


def _csv_flag(row: dict[str, Any], *aliases: str) -> Optional[bool]:
    value = _csv_field(row, *aliases)
    if value in (None, ""):
        return None
    normalized = str(value).strip().upper()
    if normalized in {"TRUE", "1", "YES", "Y", "BAD"}:
        return True
    if normalized in {"FALSE", "0", "NO", "N", "CLEAN"}:
        return False
    return None


def _lane_and_position(value: Any) -> tuple[Optional[int], Optional[int]]:
    text = str(value or "").strip()
    composite = re.fullmatch(r"\s*(\d{1,3})\s*-\s*(\d{1,4})\s*", text)
    if composite:
        lane_number = int(composite.group(1))
        position = int(composite.group(2))
        if 0 <= lane_number <= 300 and 0 <= position <= 300:
            return lane_number, position
        return None, None

    direct = _num(text)
    if direct is not None and direct.is_integer() and 0 <= direct <= 300:
        return None, int(direct)
    return None, None


def _lane_heat_band(position: float) -> str:
    if position <= 40.0:
        return "medium"
    if position < 100.0:
        return "lower"
    return "high"


def _lane_position_heat(
    candidates: list[dict[str, Any]], raw_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    del raw_rows

    points = []
    for candidate in candidates:
        raw_value = candidate.get("run_number")
        lane_number, position = _lane_and_position(raw_value)
        if position is None:
            candidate["lane_position"] = None
            continue
        band = _lane_heat_band(position)
        lane_position = {
            "raw_lane_value": str(raw_value or "").strip(),
            "lane_number": lane_number,
            "position": position,
            "heat_band": band,
        }
        candidate["lane_position"] = lane_position
        points.append(lane_position)

    counts = {
        band: sum(1 for point in points if point["heat_band"] == band)
        for band in ("medium", "lower", "high")
    }
    if not points:
        return {
            "method": "average absolute run position parsed from RunNumber; ExoticLane ignored",
            "usable_comps": 0,
            "signal": "unknown",
            "counts": counts,
            "interpretation": "No usable GMTV run positions were available.",
        }

    positions = sorted(int(point["position"]) for point in points)
    average = sum(positions) / len(positions)
    midpoint = len(positions) // 2
    median = (
        positions[midpoint]
        if len(positions) % 2
        else (positions[midpoint - 1] + positions[midpoint]) / 2.0
    )
    signal = _lane_heat_band(average)
    interpretations = {
        "medium": "The average GMTV comp sells at positions 0-40, indicating medium/normal lane heat.",
        "lower": "The average GMTV comp sells at middle positions 41-99, the typical low point for lane heat.",
        "high": "The average GMTV comp sells at triple-digit positions 100-300 near the end of the lane, when more bidders are online and competition is typically higher.",
    }
    return {
        "method": "average absolute run position parsed from RunNumber; ExoticLane ignored",
        "bands": {"medium": "0-40", "lower": "41-99", "high": "100-300"},
        "usable_comps": len(points),
        "signal": signal,
        "counts": counts,
        "average_position": round(average, 1),
        "median_position": round(median, 1),
        "interpretation": interpretations[signal],
    }


GMTV_RELEVANCE_WEIGHTS = {
    "exact_build": 1.0,
    "same_model": 0.75,
    "same_segment": 0.4,
    "same_make": 0.2,
}


def _jdpower_build(payload: Any) -> dict[str, str]:
    """Extract comparable build identity from nested JD Power VIN data."""
    aliases = {
        "year": {"year", "modelyear"},
        "make": {"make", "makename"},
        "model": {"model", "modelname"},
        "trim": {"trim", "trimname", "subseries", "subseriesname"},
        "style": {"style", "stylename", "bodystyle", "bodystylename"},
        "drivetrain": {"drivetrain", "drivetype", "drivename"},
        "segment": {"segment", "segmentname", "vehicleclass", "vehicletype", "bodytype"},
    }
    found: dict[str, list[str]] = {field: [] for field in aliases}

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                normalized_key = re.sub(r"[^a-z0-9]", "", str(key).lower())
                for field, field_aliases in aliases.items():
                    if normalized_key in field_aliases and isinstance(child, (str, int, float)):
                        text = str(child).strip()
                        if text and text not in found[field]:
                            found[field].append(text)
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    unwrapped = payload.get("data") if isinstance(payload, dict) and "data" in payload else payload
    visit(unwrapped)
    return {
        field: " | ".join(values)
        for field, values in found.items()
        if values
    }


def _manheim_mmr_build(payload: Any) -> dict[str, str]:
    """Extract the selected Manheim valuation row's build description."""
    payload = payload if isinstance(payload, dict) else {}
    containers = [payload]
    if isinstance(payload.get("data"), dict):
        containers.append(payload["data"])

    for container in containers:
        items = container.get("items")
        if not isinstance(items, list) or not items:
            continue
        item = next(
            (row for row in items if isinstance(row, dict) and row.get("bestMatch") is True),
            None,
        ) or next((row for row in items if isinstance(row, dict)), None)
        if not isinstance(item, dict) or not isinstance(item.get("description"), dict):
            continue
        description = item["description"]
        subseries = _csv_field(description, "subSeries", "subSeriesName", "series")
        values = {
            "year": _csv_field(description, "year", "modelYear"),
            "make": _csv_field(description, "make", "makeName"),
            "model": _csv_field(description, "model", "modelName"),
            "trim": _csv_field(description, "trim", "trimName") or subseries,
            "subseries": subseries,
            "style": _csv_field(description, "style", "styleName", "bodyStyle", "bodyType"),
            "drivetrain": _csv_field(description, "drivetrain", "driveTrain", "driveType"),
        }
        return {
            field: str(value).strip()
            for field, value in values.items()
            if value not in (None, "")
        }
    return {}


def _merge_build_identity(
    preferred: dict[str, Any], fallback: dict[str, Any]
) -> dict[str, str]:
    """Fill build identity from fallback while preferring the valuation's exact row."""
    merged = {
        str(field): str(value).strip()
        for field, value in (fallback or {}).items()
        if value not in (None, "")
    }
    merged.update({
        str(field): str(value).strip()
        for field, value in (preferred or {}).items()
        if value not in (None, "")
    })
    return merged


def _gmtv_relevance_from_builds(
    target: dict[str, Any], comp: dict[str, Any]
) -> tuple[str, str]:
    """Rank a decoded comp without inferring absent build attributes."""
    target_model = _squash_vehicle_text(target.get("model"))
    comp_model = _squash_vehicle_text(comp.get("model"))
    same_model = _soft_vehicle_match(target_model, comp_model)

    target_designation = _squash_vehicle_text(" ".join(
        str(target.get(field) or "") for field in ("trim", "style")
    ))
    comp_designation = _squash_vehicle_text(" ".join(
        str(comp.get(field) or "") for field in ("trim", "style")
    ))
    target_drive = _squash_vehicle_text(target.get("drivetrain"))
    comp_drive = _squash_vehicle_text(comp.get("drivetrain"))
    designation_matches = bool(
        target_designation
        and comp_designation
        and _soft_vehicle_match(target_designation, comp_designation)
    )
    drivetrain_matches = not target_drive or not comp_drive or _soft_vehicle_match(
        target_drive, comp_drive
    )
    if same_model and designation_matches and drivetrain_matches:
        return "exact_build", "Model, trim/style, and available drivetrain agree"
    if same_model:
        return "same_model", "Model agrees; trim/build is different or unverified"

    target_segment = _squash_vehicle_text(target.get("segment"))
    comp_segment = _squash_vehicle_text(comp.get("segment"))
    if target_segment and comp_segment and _soft_vehicle_match(target_segment, comp_segment):
        return "same_segment", "Vehicle segment/body type agrees"
    return "same_make", "same-year/make background; decoded model/build differs"


def _csv_sales_comps(
    csv_bytes: bytes,
    car: dict,
    *,
    comparable_years: Optional[list[int]] = None,
) -> dict[str, Any]:
    year_raw = _first(car, "year", "YEAR", "Year", "ModelYear")
    make_raw = _first(car, "make", "MAKE", "Make")
    model_raw = _first(car, "model", "MODEL", "Model")
    miles_raw = _first(car, "odometer", "Odometer", "ODOMETER", "Mileage", "mileage", "OdometerText")
    if not year_raw or not make_raw or not model_raw:
        raise ValueError("GMTV comps require year, make, and model")

    target_year = int(float(str(year_raw)))
    target_make = _squash_vehicle_text(make_raw)
    target_miles = _num(miles_raw)
    selected_years = {
        int(year) for year in (comparable_years or [target_year])
        if abs(int(year) - target_year) <= GMTV_YEAR_WINDOW
    }
    selected_years.add(target_year)

    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text, newline=""))
    raw_rows = list(reader)
    if not raw_rows:
        raise ValueError("GMTV 3monthsales.csv contained no rows")

    candidates: list[dict[str, Any]] = []
    for row in raw_rows:
        row_year = _num(_csv_field(row, "ModelYear", "Year", "Model Year"))
        sold_price = _num(_csv_field(
            row, "SoldPrice", "Sold Price", "Price Sold", "Sale Price",
            "price_sold_gross_before_auction_fee",
        ))
        if row_year is None or sold_price is None or sold_price <= 0:
            continue
        row_make_raw = _csv_field(row, "Make")
        row_model_raw = _csv_field(row, "Model")
        row_make = _squash_vehicle_text(row_make_raw)
        row_model = _squash_vehicle_text(row_model_raw)
        if row_make != target_make:
            continue
        year_gap = abs(int(row_year) - target_year)
        if int(row_year) not in selected_years:
            continue

        sold_status = str(_csv_field(
            row, "SoldStatusText", "Sold Status", "Sold", "Sale Status",
        ) or "").strip().upper()
        sold_date = _csv_field(row, "SoldDate", "Sold Date", "Sale Date", "Date Sold")
        sold_values = {"YES", "SOLD", "TRUE", "1", "SOLD NOT PAID"}
        unsold_values = {
            "NO", "NOT SOLD", "FALSE", "0", "AVAILABLE", "INVENTORY", "ON HAND"
        }
        if sold_status in unsold_values:
            continue
        if sold_status not in sold_values and (sold_status or not sold_date):
            continue

        miles = _num(_csv_field(
            row, "OdometerText", "Odometer", "Mileage", "Mileage Out", "Mileage In",
        ))
        mileage_gap = abs(miles - target_miles) if miles is not None and target_miles is not None else None
        bad_carfax = _csv_flag(row, "BadCarFax", "Bad CarFax", "bad_carfax")
        bad_autocheck = _csv_flag(
            row,
            "BadAutoCheck",
            "Bad AutoCheck",
            "Bad Autocheck",
            "bad_autocheck",
        )
        exact_model = _soft_vehicle_match(_squash_vehicle_text(model_raw), row_model)
        location = _csv_field(row, "Location", "Auction Location")
        source_name = _csv_field(row, "SourceName", "Source Name", "Purchased From")
        description_parts = [
            f"{int(row_year)} {row_make_raw or ''} {row_model_raw or ''}".strip(),
            f"sold {sold_date}" if sold_date else "",
            str(location or "").strip(),
            f"source {source_name}" if source_name else "",
            "Bad CARFAX" if bad_carfax is True else "",
            "Bad AutoCheck" if bad_autocheck is True else "",
            "same model" if exact_model else "same-year/make candidate; model differs",
        ]
        candidates.append({
            "vin": str(_csv_field(row, "Vin", "VIN No", "VIN Number") or ""),
            "inventory_id": str(_csv_field(
                row, "InventoryId", "Inventory ID", "Inventory Code", "Code",
            ) or "").strip(),
            "price": sold_price,
            "miles": miles,
            "description": " · ".join(x for x in description_parts if x),
            "year": int(row_year),
            "make": row_make_raw,
            "model": row_model_raw,
            "color": _csv_field(row, "ExtColor", "Exterior Color", "Color"),
            "sold_date": sold_date,
            "bad_carfax": bad_carfax,
            "bad_autocheck": bad_autocheck,
            "vhr_flags": {
                "bad_carfax": bad_carfax,
                "bad_autocheck": bad_autocheck,
                "known": bad_carfax is not None or bad_autocheck is not None,
                "source": "bulk_report" if bad_carfax is not None or bad_autocheck is not None else "unknown",
            },
            "run_number": _csv_field(row, "RunNumber", "Run Number"),
            "location": location,
            "source_name": source_name,
            "buyer_name": _csv_field(row, "BuyerName", "Buyer Name", "Buyer"),
            "total_days": _num(_csv_field(row, "TotalDays", "Total Days")),
            "notes": str(_csv_field(row, "Notes", "Comments", "Sold Comments") or "").strip(),
            "exact_year": year_gap == 0,
            "year_gap": year_gap,
            "mileage_gap": mileage_gap,
            "match_quality": "same_model" if exact_model else "same_year_make",
            "relevance_tier": "same_model" if exact_model else "same_make",
            "relevance_weight": (
                GMTV_RELEVANCE_WEIGHTS["same_model"]
                if exact_model
                else GMTV_RELEVANCE_WEIGHTS["same_make"]
            ),
            "relevance_reason": (
                "CSV model agrees; JD Power trim/build decode pending"
                if exact_model
                else "same-year/make background; JD Power decode pending"
            ),
        })

    candidates.sort(key=lambda r: (
        -float(r["relevance_weight"]),
        r["year_gap"],
        float("inf") if r["mileage_gap"] is None else r["mileage_gap"],
        -_sale_date_rank(r.get("sold_date")),
    ))
    candidates = candidates[:GMTV_MAX_COMPS]
    lane_position_heat = _lane_position_heat(candidates, raw_rows)
    return {
        "source": "GMTV 3monthsales.csv",
        "target": {
            "year": target_year,
            "make": str(make_raw),
            "model": str(model_raw),
            "match_make": target_make,
            "odometer": target_miles,
        },
        "filters": {
            "year": target_year,
            "comparable_years": sorted(selected_years),
            "candidate_window": {
                "minimum": target_year - GMTV_YEAR_WINDOW,
                "maximum": target_year + GMTV_YEAR_WINDOW,
            },
            "sold_only": True,
            "make_match": "exact after punctuation and spacing normalization",
            "model_selection": "all models retained for valuation-model review",
            "source": "3monthsales.csv",
        },
        "count": len(candidates),
        "same_model_count": sum(1 for row in candidates if row["match_quality"] == "same_model"),
        "relevance_hierarchy": {
            "weights": GMTV_RELEVANCE_WEIGHTS,
            "source": "CSV model initially; JD Power VIN decode during trend enrichment",
            "warning": "Missing decoded trim/build attributes are never inferred.",
        },
        "lane_position_heat": lane_position_heat,
        "sales": candidates,
    }


async def _collect_gmtv(
    car: dict,
    comparable_years: Optional[list[int]] = None,
    year_filter: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    try:
        path = _resolve_gmtv_sales_file()
        result = await asyncio.to_thread(
            _local_gmtv_sales_comps,
            path,
            car,
            comparable_years,
            year_filter,
        )
        result["errors"] = []
        return result
    except Exception as error:
        return {
            "source": "GMTV 3monthsales.csv",
            "sales": [],
            "errors": [f"GMTV 3monthsales.csv error: {type(error).__name__}: {error}"],
        }


async def _select_gmtv_comparable_years(
    car: dict,
    target_jdpower: Any,
) -> dict[str, Any]:
    """Select same-generation years before pulling GMTV sales."""
    target_year = int(float(str(_first(car, "year", "YEAR", "Year", "ModelYear"))))
    candidate_years = list(range(
        target_year - GMTV_YEAR_WINDOW,
        target_year + GMTV_YEAR_WINDOW + 1,
    ))
    target_build = _jdpower_build(target_jdpower)
    prompt = f"""
You classify model years for wholesale vehicle comparisons. Return JSON only.

TARGET VEHICLE:
{_compact(car, max_chars=10_000)}

JD POWER TARGET BUILD:
{_compact(target_build, max_chars=5_000)}

CANDIDATE MODEL YEARS:
{candidate_years}

Select only years that are basically the same build as the target: same vehicle generation,
basic chassis/platform, body configuration, and materially comparable powertrain family.
Exclude redesigns, generation changes, substantially different chassis/body configurations,
and years you are uncertain about. Always include the target year. This classification only
controls which GMTV sales are pulled; later JD Power VIN decoding ranks each individual sale.
""".strip()

    schema = {
        "type": "OBJECT",
        "properties": {
            "comparable_years": {"type": "ARRAY", "items": {"type": "INTEGER"}},
            "reasoning": {"type": "STRING"},
        },
        "required": ["comparable_years", "reasoning"],
    }

    def classify():
        return _global_vertex_client().models.generate_content(
            model=WEBSEARCH_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
                response_schema=schema,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )

    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(classify),
            timeout=GMTV_YEAR_FILTER_TIMEOUT,
        )
        parsed = json.loads(str(getattr(response, "text", "") or "").strip())
        allowed = set(candidate_years)
        selected = sorted({
            int(year) for year in parsed.get("comparable_years") or []
            if int(year) in allowed
        } | {target_year})
        return {
            "candidate_years": candidate_years,
            "comparable_years": selected,
            "reasoning": str(parsed.get("reasoning") or "").strip(),
            "target_jdpower_build": target_build,
            "model": WEBSEARCH_MODEL,
            "fallback": False,
        }
    except Exception as error:
        return {
            "candidate_years": candidate_years,
            "comparable_years": [target_year],
            "reasoning": (
                "Year/build classifier failed, so GMTV filtering fell back to the exact "
                f"model year: {type(error).__name__}: {error}"
            ),
            "target_jdpower_build": target_build,
            "model": WEBSEARCH_MODEL,
            "fallback": True,
        }


def _trend_direction(change_pct: Optional[float], *, threshold: float = 2.0) -> str:
    if change_pct is None:
        return "unknown"
    if change_pct >= threshold:
        return "up"
    if change_pct <= -threshold:
        return "down"
    return "flat"


def _average(values: list[float]) -> Optional[float]:
    return sum(values) / len(values) if values else None


def _weighted_average(points: list[dict[str, Any]], field: str) -> Optional[float]:
    values = [
        (float(point[field]), float(point.get("relevance_weight") or 0.2))
        for point in points
        if point.get(field) is not None and float(point.get("relevance_weight") or 0.2) > 0
    ]
    total_weight = sum(weight for _, weight in values)
    return (
        sum(value * weight for value, weight in values) / total_weight
        if total_weight
        else None
    )


def _market_vs_mmr_level(recent_variance: Optional[float]) -> str:
    if recent_variance is None:
        return "unknown"
    if recent_variance >= 2.0:
        return "above"
    if recent_variance <= -2.0:
        return "below"
    return "near"


def _sale_to_mmr_gap_direction(
    early_variance: Optional[float], recent_variance: Optional[float]
) -> str:
    if early_variance is None or recent_variance is None:
        return "unknown"
    if early_variance >= 2.0 and recent_variance <= -2.0:
        return "crossed_below"
    if early_variance <= -2.0 and recent_variance >= 2.0:
        return "crossed_above"
    absolute_change = abs(recent_variance) - abs(early_variance)
    if absolute_change >= 2.0:
        return "widening"
    if absolute_change <= -2.0:
        return "shrinking"
    return "stable"


def _market_trend_interpretation(
    *,
    early_variance: Optional[float],
    recent_variance: Optional[float],
    variance_change: Optional[float],
    mmr_change: Optional[float],
) -> str:
    level = _market_vs_mmr_level(recent_variance)
    gap_direction = _sale_to_mmr_gap_direction(early_variance, recent_variance)
    if recent_variance is None:
        return "There is not enough date-matched evidence to compare GMTV sold prices with MMR."

    if level == "above":
        level_text = (
            f"Recent GMTV sold prices average {abs(recent_variance):.2f}% above "
            "date-correct historical MMR; MMR may lag below observed sales."
        )
        gap_name = "above-MMR premium"
    elif level == "below":
        level_text = (
            f"Recent GMTV sold prices average {abs(recent_variance):.2f}% below "
            "date-correct historical MMR; MMR may overstate observed sales."
        )
        gap_name = "below-MMR discount"
    else:
        level_text = (
            f"Recent GMTV sold prices average {recent_variance:+.2f}% versus "
            "date-correct historical MMR, which is broadly near MMR."
        )
        gap_name = "sale-to-MMR gap"

    if gap_direction in {"shrinking", "widening"} and variance_change is not None:
        gap_text = (
            f"The {gap_name} is {gap_direction}; the residual moved "
            f"{variance_change:+.2f} percentage points"
        )
        if early_variance is not None:
            gap_text += f" from {early_variance:+.2f}% to {recent_variance:+.2f}%."
        else:
            gap_text += "."
    elif gap_direction == "crossed_above":
        gap_text = "GMTV sold prices crossed from below MMR to above MMR."
    elif gap_direction == "crossed_below":
        gap_text = "GMTV sold prices crossed from above MMR to below MMR."
    else:
        gap_text = f"The {gap_name} is stable."

    if mmr_change is None:
        mmr_text = "The historical MMR series direction is unavailable."
    else:
        mmr_text = f"Historical MMR itself moved {mmr_change:+.2f}% across the sample."
    return " ".join((level_text, gap_text, mmr_text))


def _market_trend_summary(points: list[dict[str, Any]], attempted: int) -> dict[str, Any]:
    usable = [point for point in points if point.get("historical_mmr")]
    split = max(1, len(usable) // 2)
    early = usable[:split]
    recent = usable[split:] or usable[-1:]

    early_variance = _weighted_average(early, "sale_to_mmr_pct")
    recent_variance = _weighted_average(recent, "sale_to_mmr_pct")
    variance_change = (
        recent_variance - early_variance
        if early_variance is not None and recent_variance is not None
        else None
    )
    early_mmr = _weighted_average(early, "historical_mmr")
    recent_mmr = _weighted_average(recent, "historical_mmr")
    mmr_change = (
        ((recent_mmr - early_mmr) / early_mmr) * 100.0
        if early_mmr and recent_mmr is not None
        else None
    )

    market_around_mmr = _trend_direction(variance_change)
    mmr_direction = _trend_direction(mmr_change)
    market_vs_mmr_level = _market_vs_mmr_level(recent_variance)
    gap_direction = _sale_to_mmr_gap_direction(early_variance, recent_variance)
    if len(usable) < 4 or recent_variance is None:
        accuracy = "thin_evidence"
    elif recent_variance >= 5.0:
        accuracy = "lagging_low"
    elif recent_variance <= -5.0:
        accuracy = "lagging_high"
    else:
        accuracy = "tracking"

    tier_summary = {}
    for tier, weight in GMTV_RELEVANCE_WEIGHTS.items():
        tier_points = [point for point in usable if point.get("relevance_tier") == tier]
        tier_summary[tier] = {
            "weight": weight,
            "usable_points": len(tier_points),
            "average_sale_to_mmr_pct": (
                round(_average([float(point["sale_to_mmr_pct"]) for point in tier_points]), 2)
                if tier_points
                else None
            ),
        }

    return {
        "label": "market trend",
        "method": (
            "JD Power hierarchy-weighted GMTV sold prices compared with date-correct "
            "historical MMR; exact build > same model > same segment > same make"
        ),
        "attempted_points": attempted,
        "usable_points": len(usable),
        "coverage_pct": round((len(usable) / attempted) * 100.0, 1) if attempted else 0.0,
        "chronological": True,
        "mmr_direction": mmr_direction,
        "mmr_change_pct": round(mmr_change, 2) if mmr_change is not None else None,
        "market_around_mmr_direction": market_around_mmr,
        "market_vs_mmr_level": market_vs_mmr_level,
        "recent_market_vs_mmr_pct": round(recent_variance, 2) if recent_variance is not None else None,
        "sale_to_mmr_gap_direction": gap_direction,
        "early_sale_to_mmr_pct": round(early_variance, 2) if early_variance is not None else None,
        "recent_sale_to_mmr_pct": round(recent_variance, 2) if recent_variance is not None else None,
        "sale_to_mmr_shift_pct_points": round(variance_change, 2) if variance_change is not None else None,
        "average_sale_to_mmr_pct": (
            round(_weighted_average(usable, "sale_to_mmr_pct"), 2) if usable else None
        ),
        "relevance_weight_total": round(sum(
            float(point.get("relevance_weight") or 0.2) for point in usable
        ), 2),
        "relevance_tiers": tier_summary,
        "mmr_accuracy_status": accuracy,
        "interpretation": _market_trend_interpretation(
            early_variance=early_variance,
            recent_variance=recent_variance,
            variance_change=variance_change,
            mmr_change=mmr_change,
        ),
    }


async def _enrich_gmtv_market_trend(
    gmtv: dict[str, Any],
    historical_mmr_collector: Callable,
    headers: dict,
    *,
    car: Optional[dict[str, Any]] = None,
    jdpower_decoder: Optional[Callable] = None,
    target_jdpower: Any = None,
    target_manheim_mmr: Any = None,
) -> dict[str, Any]:
    sales = sorted(
        (
            row for row in (gmtv.get("sales") or [])
            if _clean_vin(row.get("vin")) and _sale_date_rank(row.get("sold_date")) > 0
        ),
        key=lambda row: _sale_date_rank(row.get("sold_date")),
    )[-max(1, MARKET_TREND_MAX_POINTS):]
    semaphore = asyncio.Semaphore(max(1, MARKET_TREND_CONCURRENCY))
    decode_semaphore = asyncio.Semaphore(max(1, MARKET_TREND_CONCURRENCY))
    target_jdpower_build = _jdpower_build(target_jdpower)
    if not target_jdpower_build and car:
        target_jdpower_build = {
            field: str(value)
            for field, value in {
                "year": _first(car, "year", "YEAR", "Year", "ModelYear"),
                "make": _first(car, "make", "MAKE", "Make"),
                "model": _first(car, "model", "MODEL", "Model"),
                "trim": _first(car, "trim", "TRIM", "Trim", "series", "Series"),
                "style": _first(car, "body_style", "BodyStyle", "Style"),
                "drivetrain": _first(car, "drivetrain", "DriveType", "Drive Train"),
                "segment": _first(car, "segment", "VehicleSegment", "Vehicle Class"),
            }.items()
            if value not in (None, "")
        }
    target_manheim_build = _manheim_mmr_build(target_manheim_mmr)
    target_build = _merge_build_identity(target_manheim_build, target_jdpower_build)

    async def build_point(row: dict[str, Any]) -> dict[str, Any]:
        sold_date = _sale_date_iso(row.get("sold_date"))
        if sold_date is None:
            raise ValueError(f"Unsupported GMTV sold date: {row.get('sold_date')!r}")
        comp = {
            "vin": _clean_vin(row.get("vin")),
            "mileage": row.get("miles"),
            "odometer": row.get("miles"),
            "historical_date": sold_date,
        }
        point = {
            "sold_date": sold_date,
            "vin": comp["vin"],
            "year": row.get("year"),
            "make": row.get("make"),
            "model": row.get("model"),
            "relevance_tier": row.get("relevance_tier") or "same_make",
            "relevance_weight": float(row.get("relevance_weight") or 0.2),
            "relevance_reason": row.get("relevance_reason") or "JD Power decode unavailable",
            "jdpower_build": {},
            "manheim_build": {},
            "comparison_build": {},
            "jdpower_error": None,
            "miles": row.get("miles"),
            "sold_price": row.get("price"),
            "historical_mmr": None,
            "sale_to_mmr_amount": None,
            "sale_to_mmr_pct": None,
            "error": None,
        }
        if jdpower_decoder is not None:
            try:
                async with decode_semaphore:
                    decoded = await _call(jdpower_decoder, {"vin": comp["vin"]})
                comp_build = _jdpower_build(decoded)
                point["jdpower_build"] = comp_build
                row["jdpower_build"] = comp_build
                row.setdefault("csv_model", row.get("model"))
                decoded_model = str(comp_build.get("model") or "").strip()
                if decoded_model:
                    row["model"] = decoded_model
                    row["model_source"] = "jdpower"
                else:
                    row["model_source"] = "csv"
                if not comp_build:
                    point["jdpower_error"] = "JD Power returned no usable build identity"
            except Exception as error:
                point["jdpower_error"] = f"{type(error).__name__}: {error}"
                row.setdefault("csv_model", row.get("model"))
                row["model_source"] = "csv"
        try:
            async with semaphore:
                payload = await _call(historical_mmr_collector, comp, headers)
            mmr, source, path = _extract_mmr_payload_value(payload)
            if mmr is None:
                raise ValueError("historical MMR response contained no wholesale average")
            manheim_build = _manheim_mmr_build(payload)
            point["manheim_build"] = manheim_build
            if manheim_build:
                row["manheim_build"] = manheim_build
            sold_price = _number(row.get("price"))
            point["historical_mmr"] = mmr
            point["mmr_source"] = source
            point["mmr_path"] = path
            if sold_price is not None:
                variance = sold_price - mmr
                point["sale_to_mmr_amount"] = round(variance, 2)
                point["sale_to_mmr_pct"] = round((variance / mmr) * 100.0, 2)
        except Exception as error:
            point["error"] = f"{type(error).__name__}: {error}"
        comparison_build = _merge_build_identity(
            point["manheim_build"], point["jdpower_build"]
        )
        point["comparison_build"] = comparison_build
        if target_build and comparison_build:
            tier, reason = _gmtv_relevance_from_builds(target_build, comparison_build)
            sources = []
            if target_manheim_build and point["manheim_build"]:
                sources.append("Manheim current/historical MMR builds")
            if target_jdpower_build and point["jdpower_build"]:
                sources.append("JD Power")
            if sources:
                reason = f"{reason}; matched with {' + '.join(sources)}"
            point["relevance_tier"] = tier
            point["relevance_weight"] = GMTV_RELEVANCE_WEIGHTS[tier]
            point["relevance_reason"] = reason
            row["relevance_tier"] = tier
            row["relevance_weight"] = GMTV_RELEVANCE_WEIGHTS[tier]
            row["relevance_reason"] = reason
        return point

    points = await asyncio.gather(*(build_point(row) for row in sales)) if sales else []
    gmtv["market_trend"] = {
        **_market_trend_summary(points, len(sales)),
        "graph_points": points,
        "errors": [point["error"] for point in points if point.get("error")],
        "jdpower_decode_errors": [
            {"vin": point["vin"], "error": point["jdpower_error"]}
            for point in points
            if point.get("jdpower_error")
        ],
        "target_jdpower_build": target_jdpower_build,
        "target_manheim_build": target_manheim_build,
        "target_comparison_build": target_build,
    }
    return gmtv


async def _collect_jdpower(car: dict) -> dict[str, Any]:
    if not JDPOWER_SERVICE_URL:
        return {"data": None, "errors": ["JDPOWER_SERVICE_URL is not set"]}

    vin = _clean_vin(_first(car, "vin", "VIN", "Vin"))
    headers = {"Accept": "application/json"}
    if JDPOWER_API_KEY:
        headers["X-API-Key"] = JDPOWER_API_KEY

    def request_sync():
        response = requests.get(
            f"{JDPOWER_SERVICE_URL}/vin/{vin}",
            headers=headers,
            timeout=JDPOWER_TIMEOUT,
        )
        response.raise_for_status()
        return response.json()

    try:
        return {"data": await asyncio.to_thread(request_sync), "errors": []}
    except Exception as error:
        return {
            "data": None,
            "errors": [f"JD Power request failed: {type(error).__name__}: {error}"],
        }


def _normalize_crm_photos(payload: Any) -> dict[str, Any]:
    """Keep only photos that carry a usable URL, in the shape the cards expect."""

    payload = payload if isinstance(payload, dict) else {}
    images: list[dict[str, Any]] = []

    for row in payload.get("images") or []:
        if not isinstance(row, dict):
            continue

        urls = [
            str(url).strip()
            for url in (row.get("image_urls") or [])
            if str(url or "").strip()
        ]
        primary = str(row.get("image_url") or "").strip()
        if primary and primary not in urls:
            urls.insert(0, primary)
        if not urls:
            continue

        images.append(
            {
                "photo_id": str(row.get("photo_id") or ""),
                "name": str(row.get("name") or ""),
                "type": str(row.get("type") or ""),
                "damaged": bool(row.get("damaged")),
                "description": str(row.get("description") or "").strip(),
                "image_url": urls[0],
                "image_urls": urls,
                "attached_at": str(row.get("attached_at") or ""),
                "updated_at": str(row.get("updated_at") or ""),
                "source": str(row.get("source") or ""),
            }
        )

    lead = payload.get("lead")
    return {
        "lead": lead if isinstance(lead, dict) else None,
        "images": images,
        "count": len(images),
        "damaged_count": sum(1 for image in images if image["damaged"]),
        "errors": [str(error) for error in (payload.get("errors") or []) if error],
    }


async def _collect_crm_photos(car: dict) -> dict[str, Any]:
    """Photo URLs + Ava descriptions from the Powerbuyer lead matching this VIN."""

    # The start route may have already paid for this lookup; do not repeat the
    # classifier backfill just to get the same rows.
    cached = car.get("crm_photos")
    if isinstance(cached, dict) and cached.get("images"):
        return _normalize_crm_photos(cached)

    collector = _RUNTIME.get("crm_photos_collector")
    if collector is None:
        return {
            "lead": None,
            "images": [],
            "count": 0,
            "damaged_count": 0,
            "errors": ["crm_photos_collector is not configured"],
        }

    try:
        result = await _call(collector, car, CRM_PHOTO_MAX_IMAGES)
    except Exception as error:
        return {
            "lead": None,
            "images": [],
            "count": 0,
            "damaged_count": 0,
            "errors": [f"CRM photo lookup failed: {type(error).__name__}: {error}"],
        }

    return _normalize_crm_photos(result)


_PHOTO_DOCUMENT_TYPES = {
    "business_card", "carmax_offer", "carvana_offer", "clear_title",
    "death_certificate", "dealer_license", "dealer_offer", "divorce_decree",
    "drivers_license", "marriage_license", "payoff", "payoff_verification",
    "power_of_attorney", "registration", "release_of_lien", "tda_offer",
    "title_back", "title_check", "title_font", "vin", "vroom_offer",
}
_PHOTO_VEHICLE_TYPES = {
    "back", "blue_driver", "engine", "engine_bay", "front", "interior",
    "left", "odometer", "right", "type_c",
}
_PHOTO_QUALITY_ONLY_PATTERN = re.compile(
    r"\b(?:blur(?:ry|red)?|glare|unreadable|illegible|cropp(?:ed|ing)|"
    r"low[- ]resolution|out of focus|overexposed|underexposed|obstructed)\b",
    re.IGNORECASE,
)
_PHOTO_DAMAGE_PATTERN = re.compile(
    r"\b(?:collision|impact|crush(?:ed)?|crumpl(?:ed|ing)|buckl(?:ed|ing)|"
    r"dent(?:ed|s)?|ding(?:s)?|scratch(?:ed|es)?|scuff(?:ed|s)?|crack(?:ed|s)?|"
    r"broken|missing|rust|corrosion|paint mismatch|misaligned|bent|deployed|"
    r"tear(?:ed|s)?|torn|stain(?:ed|s)?|burn(?:ed|s)?|curb rash|body damage|"
    r"structural damage|frame damage|clear[- ]coat failure)\b",
    re.IGNORECASE,
)
_PHOTO_SEVERE_DAMAGE_PATTERN = re.compile(
    r"\b(?:severe|major|collision|impact|crush(?:ed)?|crumpl(?:ed|ing)|"
    r"buckl(?:ed|ing)|structural|frame damage|airbags? deployed)\b",
    re.IGNORECASE,
)
_PHOTO_MODERATE_DAMAGE_PATTERN = re.compile(
    r"\b(?:moderate|large|deep|broken|cracked|bent|missing|rust|corrosion|"
    r"paint mismatch|misaligned|torn|tear(?:ed|s)?)\b",
    re.IGNORECASE,
)
_PHOTO_SUPER_CLEAN_PATTERN = re.compile(
    r"\b(?:exceptionally clean|super clean|pristine|excellent condition|"
    r"retail[- ]ready|minimal wear|no visible defects?|no dents? or scratches?|"
    r"straight panels?)\b",
    re.IGNORECASE,
)


def _normalized_mmr_grade(value: Any) -> Optional[float]:
    """Normalize Manheim's 0-50 API grade or a displayed 0.0-5.0 grade."""
    grade = _number(value)
    if grade is None or grade < 0:
        return None
    if grade > 5:
        grade /= 10.0
    if grade > 5:
        return None
    return round(float(grade), 1)


def _current_mmr_average_grade(current_mmr: Any) -> tuple[Optional[float], Any]:
    payload = current_mmr if isinstance(current_mmr, dict) else {}
    containers = [payload]
    if isinstance(payload.get("data"), dict):
        containers.append(payload["data"])
    for container in containers:
        items = container.get("items")
        if not isinstance(items, list):
            continue
        item = next(
            (row for row in items if isinstance(row, dict) and row.get("bestMatch") is True),
            None,
        ) or next((row for row in items if isinstance(row, dict)), None)
        if not isinstance(item, dict):
            continue
        raw_grade = item.get("averageGrade")
        if raw_grade in (None, ""):
            raw_grade = item.get("forecastedAverageGrade")
        grade = _normalized_mmr_grade(raw_grade)
        if grade is not None:
            return grade, raw_grade
    return None, None


def _photo_condition_assessment(
    crm_photos: Any,
    *,
    average_grade: Any,
) -> dict[str, Any]:
    """Select a bounded half-grade target from explicit multi-photo condition facts."""
    normalized_average = _normalized_mmr_grade(average_grade)
    result: dict[str, Any] = {
        "confirmed": False,
        "direction": "average",
        "severity": "none",
        "average_grade": normalized_average,
        "adjusted_grade": None,
        "api_grade": None,
        "observations": [],
        "excluded_observations": [],
    }
    if normalized_average is None:
        return result

    images = (crm_photos or {}).get("images") if isinstance(crm_photos, dict) else []
    damage_observations = []
    clean_observations = []
    any_vehicle_quality_problem = False
    seen_photo_ids = set()
    for index, image in enumerate(images or []):
        if not isinstance(image, dict):
            continue
        photo_type = str(image.get("type") or "").strip().lower()
        description = str(image.get("description") or "").strip()
        photo_id = str(image.get("photo_id") or f"photo-{index + 1}").strip()
        if photo_id in seen_photo_ids:
            continue
        seen_photo_ids.add(photo_id)
        base = {
            "photo_id": photo_id,
            "type": photo_type,
            "description": description,
        }

        if photo_type in _PHOTO_DOCUMENT_TYPES:
            result["excluded_observations"].append({**base, "reason": "non_vehicle_image"})
            continue

        quality_only = bool(_PHOTO_QUALITY_ONLY_PATTERN.search(description)) and not bool(
            _PHOTO_DAMAGE_PATTERN.search(description)
        )
        if image.get("damaged") is True:
            any_vehicle_quality_problem = True
            if quality_only:
                result["excluded_observations"].append({**base, "reason": "image_quality_only"})
                continue
            if not _PHOTO_DAMAGE_PATTERN.search(description):
                result["excluded_observations"].append({**base, "reason": "vehicle_damage_not_explicit"})
                continue
            severity = (
                "severe"
                if _PHOTO_SEVERE_DAMAGE_PATTERN.search(description)
                else "moderate"
                if _PHOTO_MODERATE_DAMAGE_PATTERN.search(description)
                else "minor"
            )
            damage_observations.append({**base, "severity": severity})
            continue

        if photo_type in _PHOTO_VEHICLE_TYPES and description:
            clean_observations.append({
                **base,
                "super_clean": bool(_PHOTO_SUPER_CLEAN_PATTERN.search(description)),
            })

    if damage_observations:
        severity_rank = {"minor": 0, "moderate": 1, "severe": 2}
        severity = max(
            (row["severity"] for row in damage_observations),
            key=severity_rank.__getitem__,
        )
        next_lower_half = max(0.0, (int(normalized_average * 2 + 0.999999) / 2.0) - 0.5)
        additional_drop = {"minor": 0.0, "moderate": 0.5, "severe": 1.0}[severity]
        adjusted_grade = max(0.0, next_lower_half - additional_drop)
        if adjusted_grade < normalized_average:
            result.update({
                "confirmed": True,
                "direction": "below_average",
                "severity": severity,
                "adjusted_grade": adjusted_grade,
                "api_grade": int(round(adjusted_grade * 10)),
                "observations": damage_observations,
            })
        return result

    distinct_clean_views = {row["type"] for row in clean_observations}
    super_clean_views = [row for row in clean_observations if row["super_clean"]]
    if (
        not any_vehicle_quality_problem
        and len(distinct_clean_views) >= 3
        and len(super_clean_views) >= 3
    ):
        adjusted_grade = min(5.0, (int(normalized_average * 2) / 2.0) + 0.5)
        if adjusted_grade > normalized_average:
            result.update({
                "confirmed": True,
                "direction": "above_average",
                "severity": "super_clean",
                "adjusted_grade": adjusted_grade,
                "api_grade": int(round(adjusted_grade * 10)),
                "observations": super_clean_views,
            })
    return result


async def _collect_reconditioned_mmr(
    car: dict,
    crm_photos: dict,
    current_mmr: dict,
    collector: Callable,
    headers: dict,
) -> dict[str, Any]:
    """Re-run current MMR at a photo-supported 0-50 condition grade."""
    average_grade, raw_average_grade = _current_mmr_average_grade(current_mmr)
    assessment = _photo_condition_assessment(
        crm_photos,
        average_grade=average_grade,
    )
    average_grade_mmr, average_source, average_path = _extract_mmr_payload_value(current_mmr)
    result = {
        **assessment,
        "attempted": False,
        "status": "not_applicable",
        "raw_average_grade": raw_average_grade,
        "average_grade_mmr": average_grade_mmr,
        "average_grade_mmr_source": average_source,
        "average_grade_mmr_path": average_path,
        "adjusted_grade_mmr": None,
        "mmr_delta": None,
        "payload": None,
        "errors": [],
    }
    if average_grade is None:
        result.update({
            "status": "unavailable",
            "errors": ["Current MMR returned no usable averageGrade."],
        })
        return result
    if average_grade_mmr is None:
        result.update({
            "status": "unavailable",
            "errors": ["Current MMR returned no usable wholesale average."],
        })
        return result
    if not assessment["confirmed"] or assessment["api_grade"] is None:
        return result

    request_car = dict(car)
    request_car["grade"] = int(assessment["api_grade"])
    result["attempted"] = True
    try:
        payload = await _call(collector, request_car, headers)
        adjusted_mmr, adjusted_source, adjusted_path = _extract_mmr_payload_value(payload)
        result["payload"] = _jsonable(payload)
        if adjusted_mmr is None:
            result.update({
                "status": "error",
                "errors": ["Condition-grade MMR response contained no wholesale average."],
            })
            return result
        result.update({
            "status": "complete",
            "adjusted_grade_mmr": adjusted_mmr,
            "adjusted_grade_mmr_source": adjusted_source,
            "adjusted_grade_mmr_path": adjusted_path,
            "mmr_delta": round(adjusted_mmr - average_grade_mmr, 2),
        })
        return result
    except Exception as error:
        result.update({
            "status": "error",
            "errors": [f"Condition-grade MMR failed: {type(error).__name__}: {error}"],
        })
        return result


def _decode_inline_artifact(vin: str, name: str, artifact: Any) -> tuple[Optional[str], Optional[dict], Optional[str]]:
    if not isinstance(artifact, dict):
        return None, None, f"{name}: artifact missing"

    data_b64 = artifact.get("data_b64")
    if not data_b64:
        return None, None, f"{name}: artifact had no data"

    try:
        raw = base64.b64decode(data_b64, validate=True)
    except Exception as error:
        return None, None, f"{name}: invalid base64: {type(error).__name__}: {error}"

    if len(raw) > MAX_ARTIFACT_BYTES:
        return None, None, f"{name}: artifact exceeded {MAX_ARTIFACT_BYTES:,} bytes"

    filename = Path(str(artifact.get("filename") or name)).name
    target = EVIDENCE_DIR / vin / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(raw)

    summary = {
        "filename": filename,
        "mime_type": str(artifact.get("mime_type") or ""),
        "size_bytes": len(raw),
        "collected": True,
    }
    return str(target), summary, None


async def _collect_browser(car: dict) -> dict[str, Any]:
    """Use the EXISTING Prism /evidence/browser route exactly once.

    This deliberately crosses the Cloud Run request boundary. The browser route remains
    an active request while Playwright runs, so Cloud Run allocates CPU to that work.
    The route returns inline base64 artifacts, which are materialized on this worker;
    therefore it is safe when the browser request lands on a different instance.
    """

    vin = _clean_vin(_first(car, "vin", "VIN", "Vin"))
    odometer = _first(
        car,
        "odometer",
        "Odometer",
        "ODOMETER",
        "mileage",
        "Mileage",
        default="",
    )
    if not vin:
        return {"errors": ["Browser evidence requires VIN"]}
    if odometer in (None, ""):
        return {"errors": ["Browser evidence requires odometer"]}

    headers = {"Content-Type": "application/json"}
    if BROWSER_SERVICE_TOKEN:
        headers["Authorization"] = BROWSER_SERVICE_TOKEN

    timeout = aiohttp.ClientTimeout(
        total=BROWSER_TIMEOUT,
        connect=min(15.0, BROWSER_TIMEOUT),
        sock_connect=min(15.0, BROWSER_TIMEOUT),
        sock_read=BROWSER_TIMEOUT,
    )

    started = time.monotonic()
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                BROWSER_EVIDENCE_URL,
                headers=headers,
                json={
                    "vin": vin,
                    "odometer": str(odometer),
                    # The route returns inline bytes; the remote instance can safely
                    # clean its local files after the response is built.
                    "keep_files": False,
                },
            ) as response:
                body = await response.text()
                if response.status < 200 or response.status >= 300:
                    return {
                        "success": False,
                        "errors": [
                            f"/evidence/browser HTTP {response.status}: {body[:1000]}"
                        ],
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
                try:
                    payload = json.loads(body)
                except Exception as error:
                    return {
                        "success": False,
                        "errors": [
                            f"/evidence/browser returned invalid JSON: {type(error).__name__}: {error}"
                        ],
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    }
    except Exception as error:
        return {
            "success": False,
            "errors": [f"/evidence/browser failed: {type(error).__name__}: {error}"],
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }

    errors = [str(x) for x in (payload.get("errors") or []) if x]
    artifacts = payload.get("artifacts") or {}

    accutrade_path, accutrade_meta, accutrade_error = _decode_inline_artifact(
        vin,
        "accutrade_screenshot",
        artifacts.get("accutrade_screenshot"),
    )
    carfax_path, carfax_meta, carfax_error = _decode_inline_artifact(
        vin,
        "carfax_pdf",
        artifacts.get("carfax_pdf"),
    )
    autocheck_path, autocheck_meta, autocheck_error = _decode_inline_artifact(
        vin,
        "autocheck_pdf",
        artifacts.get("autocheck_pdf"),
    )
    for error in (accutrade_error, carfax_error, autocheck_error):
        if error:
            errors.append(error)

    route_report_text = payload.get("report_text") or {}
    if not isinstance(route_report_text, dict):
        route_report_text = {}
    report_text: dict[str, str] = {
        name: str(route_report_text.get(name) or "").strip()
        for name in ("carfax", "autocheck")
        if str(route_report_text.get(name) or "").strip()
    }
    route_report_text_status = payload.get("report_text_status") or {}
    if not isinstance(route_report_text_status, dict):
        route_report_text_status = {}
    report_text_status: dict[str, dict[str, Any]] = {}
    stage_status = copy.deepcopy(payload.get("stage_status") or {})
    for name, path_value in (
        ("carfax", carfax_path),
        ("autocheck", autocheck_path),
    ):
        text = report_text.get(name, "")
        route_status = route_report_text_status.get(name) or {}
        status = copy.deepcopy(route_status) if isinstance(route_status, dict) else {}
        if text:
            status.setdefault("available", bool(path_value))
            status["extracted"] = True
        else:
            text, status = _extract_pdf_text(path_value, vin)
        report_text_status[name] = status
        source_status = stage_status.setdefault(name, {})
        source_status["text_extraction"] = (
            "complete" if status.get("extracted") else
            "error" if status.get("error") else
            "not_produced"
        )
        source_status["vin_match"] = (
            "complete" if status.get("vin_matched") is True else
            "mismatch" if status.get("vin_matched") is False else
            "not_checked"
        )
        if text:
            report_text[name] = text

    accutrade = payload.get("accutrade") or {"target_auction": None}
    target_auction = _number(accutrade.get("target_auction"))
    if accutrade_meta is not None:
        accutrade_meta["target_auction"] = target_auction

    return {
        "success": bool(payload.get("success")) and bool(accutrade_path),
        "accutrade_screenshot": accutrade_path,
        "accutrade": {"target_auction": target_auction},
        "carfax_pdf": carfax_path,
        "autocheck_pdf": autocheck_path,
        "autocheck": payload.get("autocheck") or (
            {"report_collected": True, "artifact": "autocheck_pdf"}
            if autocheck_path
            else None
        ),
        "report_text": report_text,
        "report_text_status": report_text_status,
        "stage_status": stage_status,
        "artifact_summary": {
            "accutrade_screenshot": accutrade_meta,
            "carfax_pdf": carfax_meta,
            "autocheck_pdf": autocheck_meta,
        },
        "route_elapsed_seconds": payload.get("elapsed_seconds"),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "errors": list(dict.fromkeys(errors)),
    }


# ---------------------------------------------------------------------------
# Three model passes
# ---------------------------------------------------------------------------


def _grounding_sources(response: Any) -> list[dict[str, str]]:
    """Extract grounded sources and the exact cited text supporting each source."""
    sources: list[dict[str, str]] = []
    try:
        for candidate in getattr(response, "candidates", None) or []:
            metadata = getattr(candidate, "grounding_metadata", None)
            chunks = list(getattr(metadata, "grounding_chunks", None) or [])
            support_text: dict[int, list[str]] = {}
            for support in getattr(metadata, "grounding_supports", None) or []:
                segment = getattr(support, "segment", None)
                text = str(getattr(segment, "text", "") or "").strip()
                for index in getattr(support, "grounding_chunk_indices", None) or []:
                    if text:
                        support_text.setdefault(int(index), []).append(text)

            for index, chunk in enumerate(chunks):
                web = getattr(chunk, "web", None)
                if web is None:
                    continue
                uri = str(getattr(web, "uri", "") or "")
                title = str(getattr(web, "title", "") or "Web result")
                if not uri or any(row.get("grounding_url") == uri for row in sources):
                    continue
                reasoning = " ".join(dict.fromkeys(support_text.get(index, []))).strip()
                sources.append({
                    "title": title,
                    "url": uri,
                    "grounding_url": uri,
                    "reasoning": reasoning,
                })
    except Exception:
        logger.exception("Failed extracting grounding metadata")
    return sources[:30]


def _resolve_grounding_url_sync(url: str) -> str:
    """Resolve Google's grounding redirect to the publisher URL without downloading the body."""
    url = str(url or "").strip()
    if not url or "vertexaisearch.cloud.google.com/grounding-api-redirect" not in url:
        return url
    try:
        response = requests.get(
            url,
            allow_redirects=True,
            stream=True,
            timeout=(3.0, GROUNDING_RESOLVE_TIMEOUT),
            headers={"User-Agent": "Mozilla/5.0 (compatible; GMTV-Valuation/1.0)"},
        )
        try:
            final_url = str(response.url or "").strip()
            return final_url or url
        finally:
            response.close()
    except Exception as error:
        logger.info("Could not resolve grounding URL %s: %s: %s", url, type(error).__name__, error)
        return url


async def _resolve_grounding_sources(sources: list[dict[str, str]]) -> list[dict[str, str]]:
    sources = sources[:max(0, GROUNDING_MAX_SOURCES)]
    semaphore = asyncio.Semaphore(max(1, GROUNDING_RESOLVE_CONCURRENCY))

    async def resolve_one(source: dict[str, str]) -> dict[str, str]:
        async with semaphore:
            actual = await asyncio.to_thread(_resolve_grounding_url_sync, source.get("grounding_url") or source.get("url") or "")
        row = dict(source)
        row["url"] = actual
        return row

    return await asyncio.gather(*(resolve_one(source) for source in sources)) if sources else []


async def _pass1_websearch(car: dict, evidence: dict) -> dict[str, Any]:
    specialty_research = _specialty_auction_research(car)
    schema_contract = json.dumps(PUBLIC_RESEARCH_SCHEMA, separators=(",", ":"))

    prompt = f"""
You are the public-market evidence researcher for a GMTV wholesale vehicle valuation.
This is evidence collection only. Do NOT choose the final hammer yet. Silently check whether the
vehicle has Canadian history, but confirm that only from CARFAX, AutoCheck, or an exact-VIN
grounded public match when neither report is available. Manheim, GMTV, AccuTrade, JD Power,
generic model research, and non-matching VINs cannot establish Canadian history. Mention
Canadian history in the research memo only when qualified evidence supports a non-zero
Canadian-history adjustment. Otherwise omit the check and its result entirely.

TARGET VEHICLE:
{_compact(car, max_chars=20_000)}

PRIVATE EVIDENCE ALREADY COLLECTED:
{_compact(evidence, max_chars=100_000)}

SPECIALTY AUCTION RESEARCH DECISION:
{_compact(specialty_research, max_chars=5_000)}

Use Google Search as a tool and curate the search yourself. Search multiple queries as
needed. Do not rely on a single search query or a single marketplace.

Prioritize:
1. exact VIN listings, sale/archive references, dealer listings, auction references,
   plus exact email and phone searches when those identifiers are present in TARGET
   VEHICLE,
2. exact year/make/model/trim/drivetrain/body configuration comparables,
3. genuinely close substitutes only when exact-build evidence is thin,
4. current retail asks, supply, days-on-market clues, and market heat that constrain a
   wholesale hammer,
5. visual desirability / "sexiness" evidence for the exact vehicle's color, cosmetic
    presentation, modifications, wheel/package combination, and overall presentation
    when the public market provides enough evidence to compare it meaningfully.
6. exact named-package and edition research. Search the full package name, common aliases,
   manufacturer/coachbuilder name, package contents, production volume when verifiable,
   original MSRP or package price, and package-equipped versus otherwise equivalent
   vehicles. For combinations such as Shelby off-road packages, search both the complete
   package name and component terms rather than collapsing the vehicle into a base trim.

PUBLIC-AUCTION BASELINE CONTINGENCY

If `SPECIALTY AUCTION RESEARCH DECISION.required` is true, the sold-auction search is
MANDATORY even when Manheim MMR and AccuTrade Target Auction are available. For a classic,
exotic, rare, limited-production, unique luxury, performance, or high-end vehicle, search
Bring a Trailer and Mecum plus the most relevant similar specialist venues below. Report
the sold results as corroborating specialty-market evidence and explain whether the normal
wholesale anchors appear to capture the exact configuration and buyer pool. Do not replace
a usable MMR/AccuTrade baseline merely because a collector auction produced a higher price;
apply the existing retail-to-wholesale economics and anti-overcorrection rules.
Include a `SPECIALTY AUCTION SEARCH LOG` in the research memo naming every venue queried,
the query used, result count, accepted sold results, and rejection/no-result reason. Both
Bring a Trailer and Mecum must appear in this log even when either returns no usable comp.
For a rare, named, coachbuilt, limited-production, performance, or off-road package, build
a `PACKAGE NUMERICAL EVIDENCE LEDGER`. Seek every verifiable relevant result available,
with a target of at least 10 numerical observations when the market supplies them. Include
sold auction results first, then close active listings as secondary market context. For
each observation record URL, status, date, price or hammer, mileage, model year, exact
package/build, material options/modifications, and comparability/rejection reason. Search
package-equipped vehicles, base/non-package controls, and relevant specialist venues.
Report the actual count found when fewer than 10 exist; never pad the ledger with weak,
duplicate, unverifiable, or materially different vehicles.

Inspect the collected Manheim current MMR and AccuTrade evidence. If current adjusted
MMR is absent and AccuTrade Target Auction / Bring Money is absent or unreadable, perform
an explicit sold-auction baseline search. Search the platforms whose vehicle audience
fits the target:
{chr(10).join(f'- {name} ({domain}): {scope}' for name, domain, scope in PUBLIC_AUCTION_BASELINE_SOURCES)}

Also search other reputable auction houses when the vehicle segment has a better
specialist venue, but identify the house and explain why its buyer pool is relevant.
Copart and IAAI are salvage/insurance channels: use them only for a similarly branded or
damaged target, never as the ordinary clean-title baseline. CLASSIC.COM may help locate
an original result, but cite and verify the original auction page rather than using an
aggregated number as the baseline.

For every fallback result record the direct URL, SOLD date, hammer/winning-bid amount,
whether buyer fees are included, mileage, trim/configuration, location, condition/history,
and why it is comparable. Reject active listings, estimates, asking prices, bids in
progress, reserve-not-met/no-sale outcomes, and results without a verifiable sold amount.
Seek at least three close sold results across at least two independent auction domains.
Do not average blindly: reconcile configuration, mileage, condition, date, geography,
and collector-retail buyer-pool differences. These public auction hammers are usually
retail/enthusiast transactions, not dealer wholesale; translate them to a GMTV wholesale
baseline only after preserving realistic fees, recon, transport, holding cost, liquidity
risk, and dealer margin.

Requirements:
- Never invent a listing, price, mileage, trim, condition, sale, color, option, rarity,
  appearance characteristic, or URL.
- Separate SOLD/auction evidence from active asking prices.
- For every proposed evidence effect (mileage, history, condition/recon, factory or
    aftermarket equipment, color/appearance, market heat, and retail ceiling), perform a
    controlled paired comparison whenever the public market has enough data:
    first find a normal/average retail listing for the same build, then find a second
    listing with the claimed feature while holding year, trim, drivetrain/body, mileage,
    condition, and other equipment as close as possible. Record both prices, mileage,
    build details, status, exact URLs, and the observed price difference. A generic
    average listing is not evidence for a feature premium unless the paired difference
    isolates that feature.
- If a first-party GMTV 3monthsales sold pair can isolate the effect, use that pair
    first and treat it as controlling evidence; public retail research is secondary.
- For each proposed adjustment, state the observed paired difference and whether the
    proposed dollar adjustment is within $500 of it. If it is not, recommend the adjusted
    amount that is within $500 rather than preserving the unsupported amount.
- Investigate EVERY adjustment claim made by the collected evidence or reasonably suggested
    by the target vehicle: mileage, non-Canadian history, condition/recon, factory equipment,
    aftermarket equipment, color/appearance, market heat, and retail ceiling. For each claim,
    search specifically for both (1) a normal/stock/average retail example of the same vehicle
    and (2) a close example carrying the claimed feature or issue. General model pricing is
    not adjustment evidence.
- A claim is supported only when the memo supplies both examples with exact grounded URLs,
    prices, mileage, trim/build, ACTIVE or SOLD status, material differences, and the signed
    observed price difference. If either side is missing or confounded, mark it NOT SUPPORTED
    and recommend $0.
- DEALER UNDERWRITING WHEN A VEHICLE PAIR DOES NOT EXIST: a confirmed material impairment
    can still require a lower bid even when no close clean-versus-impaired vehicle pair is
    publicly available. Public research may quantify that risk only after CARFAX, AutoCheck,
    Manheim condition evidence, or CRM photos establish that the TARGET actually has the
    issue. Public research cannot establish the target fact itself.
- For every source-confirmed impairment, research the questions a buying dealer would ask:
    legal/title/inspection and registration steps; lender and insurer restrictions; eligible
    retail buyer pool; likely retailability and time-to-sale; auction announcement obligations;
    transport, parts, repair, diagnostic, and recon constraints; clean retail reference range;
    impaired retail exit range; and the maximum wholesale bid that preserves realistic fees,
    recon, holding cost, risk, and margin.
- Populate `dealer_underwriting` even when it is NOT_SUPPORTED. Mark it SUPPORTED only with
    at least two independent grounded domains, a target-relevant clean retail range, a
    quantified impaired retail-exit range, a conservative wholesale-adjustment range, source
    dates/scopes, and explicit limitations. Prefer government or other primary regulatory
    sources for legal facts and current market/auction/valuation sources for monetary facts.
    Generic percentage articles, SEO summaries, snippets without a source page, and a single
    source are context only. Do not apply a generic percentage blindly to the target.
- `recommended_wholesale_adjustment_low` and `_high` are signed deltas. For an impairment both
    must be negative, ordered from the larger deduction to the smaller deduction. This is a
    conservative no-pair fallback, not permission to invent a premium. Controlled GMTV,
    Manheim, or public vehicle pairs still take precedence when available.
- Reject materially wrong trims/configurations and say why.
- Treat same-VIN public evidence as especially important.
- IDENTITY SEARCH FIREWALL: exact VIN, email, and phone matches are identity/listing
    discovery only. Summarize the matched URL, status, vehicle details, and why it matched,
    but NEVER use an identity match as a comp, anchor, adjustment, range input, confidence
    boost, or any other valuation math. A separately discovered listing may affect value
    only if it independently qualifies as a genuine comparable under all comp rules.
- Do not describe a color or appearance combination as "rare" unless the search evidence
  actually supports that conclusion.
- Retail asking prices are not wholesale values. Use them only as market constraints and
  relative evidence.
- Do not make a valuation adjustment from appearance alone unless the observed public
  price difference is large, reasonably consistent, and supported by multiple relevant
  listings/comparisons.
- For a color or appearance conclusion, record actual close retail listings with their
    exact URLs, price, mileage, trim/build differences, and ACTIVE or SOLD status. A
    color name, rarity claim, or subjective desirability claim is not sufficient evidence.
- The same-model sales search may include up to five nearby model years, but do not treat
    them as comparable until you determine from public research which years share the
    same basic generation, chassis, powertrain, body, and meaningful equipment. Clearly
    different generations or major redesigns must be excluded and explained.
- When first-party evidence identifies a material condition, history, equipment, or recon
    issue, search for close public listings or sales that show how that issue affects retail
    exit or buyer demand. Do not invent a repair cost; report when the public market cannot
    isolate the issue.
- Read `gmtv.market_trend` as the controlling first-party historical trend series. Report
    `market_vs_mmr_level`, `sale_to_mmr_gap_direction`, `mmr_direction`, and
    `market_around_mmr_direction` separately. State whether recent GMTV sales remain above
    or below date-correct historical MMR even when that premium or discount is shrinking.
    Explicitly research whether MMR is lagging current conditions or losing accuracy for
    this segment.
- Produce TWO separate forward market studies: `NEAR TERM 21-28 DAYS` and
    `MEDIUM TERM 30-90 DAYS`. For each horizon research current inventory volume,
    listing growth/contraction, days on market, price cuts, sell-through/mobility,
    geographic saturation, seasonality, and likely wholesale direction. State direction,
    confidence, query scope, source count, numerical observation count, observation dates,
    and contrary evidence separately for each horizon. Do not reuse a broad 30-90 day
    claim as proof of the 21-28 day forecast. Reconcile where the horizons agree or diverge.
- Treat retail asks only as a ceiling and directional signal. Never convert an ask into
    wholesale value. A broad retail decline may lead wholesale by roughly one month, so
    investigate that possibility but claim it only when dated evidence supports it.


VISUAL DESIRABILITY / "SEXINESS" CHECK

Perform a targeted public-market comparison of the target vehicle's visual configuration,
cosmetic presentation, and visible aftermarket modifications.

The purpose is to determine whether the target vehicle appears to have a meaningfully
more desirable or less desirable visual specification than the typical example of the
same vehicle, and whether the PUBLIC market shows a sufficiently large price difference
for that desirability to matter to wholesale value.

The controlling question is: DOES THIS VEHICLE'S MARKET CARE ABOUT THIS APPEARANCE
FACTOR? Do not assume every segment rewards or punishes the same feature. A professionally
lifted truck may attract a premium, while the same treatment elsewhere may shrink the
buyer pool. A coherent factory-style package may help; a gaudy, polarizing, poorly
installed, or highly personalized package may hurt. A visibly dented, scraped,
mismatched, or generally beat-up vehicle may retail below an ordinary clean example even
when a precise repair invoice is unavailable.

First identify, as accurately as the supplied vehicle information and public evidence
allow:

- exact year
- make
- model
- trim
- drivetrain
- body configuration
- approximate mileage band
- exterior color
- interior color when known
- wheels / appearance package when visible or explicitly documented
- other obvious appearance-defining factory options
- major modifications that materially change appearance
- visible dents, scratches, paint mismatch, body damage, worn trim, and overall cosmetic
    presentation documented by collected photos or condition evidence

Do not infer undocumented factory packages from appearance alone.

Then curate THREE comparison groups:

1. TARGET / SAME-APPEARANCE COMBO

   Search for vehicles that resemble the target as closely as possible:

   - same year or closest useful model year,
   - same model and trim,
   - same drivetrain/body configuration,
   - reasonably similar mileage,
   - same exterior color when possible,
   - same interior color / wheels / appearance package when known.

   This is the closest public representation of how the target's exact visual
   specification is being priced.

2. MOST DESIRABLE OR LEAST-COMMON OBSERVED COMBO

   Search the same vehicle market for an appearance combination that PUBLIC evidence
   suggests is especially desirable, unusually scarce, or consistently commands a
   premium.

   Do NOT simply guess which color is rare or desirable.

   The comparison must be based on observable evidence such as:
   - noticeably higher asking prices among otherwise close vehicles,
   - very limited availability,
   - repeated dealer descriptions highlighting the specification,
   - documented limited-production color/package information,
   - unusually fast turnover or scarce inventory when that can actually be observed.

   Call this the "rarest/desirable observed combo" only if the evidence supports it.
   Otherwise call it the "highest-priced observed appearance combo."

3. AVERAGE / NORMAL COMBO

   Identify the visually ordinary or most commonly represented specification for the
   same vehicle based on the search results.

   Prefer:
   - commonly available exterior colors,
   - ordinary wheel/package combinations,
   - similar mileage,
   - same trim/drivetrain/body configuration.

   This represents the normal market baseline for the model rather than the target's
   particular visual appeal.


SEARCH STRATEGY FOR THE SEXINESS CHECK

Run multiple targeted searches yourself.

Examples of the type of query construction to use:

- "[year] [make] [model] [trim] [target color] for sale"
- "[year] [make] [model] [trim] [target color] [approx mileage] miles"
- "[year] [make] [model] [trim] [alternate color] for sale"
- "[year] [make] [model] [trim] rare color"
- "[year] [make] [model] [trim] special color"
- "[year] [make] [model] [trim] appearance package"
- "[year] [make] [model] lifted for sale" or the target's actual modification
- "[year] [make] [model] modified vs stock price"
- "[year] [make] [model] dented damaged cosmetic condition for sale"
- exact VIN searches when a VIN is supplied

These are examples only. Construct better searches when the actual vehicle suggests
more specific terminology.

Use multiple dealer/marketplace/auction sources when possible.

Do not compare a premium appearance package against a base vehicle and attribute the
entire price difference to color. Control for trim, drivetrain, mileage, body style,
options, condition, and seller type as much as the available evidence permits.

Build separate controlled comparisons for:
- COLOR / FACTORY APPEARANCE: keep the existing strict color evidence standard.
- AFTERMARKET MARKET APPEAL: compare the target's actual modification against close
    stock/normal examples and determine whether it expands or shrinks the buyer pool.
- COSMETIC PRESENTATION: compare similarly equipped clean examples against close
    visibly damaged, dented, worn, or beat-up examples when such evidence exists.

Do not combine these effects into one number. Report which ladder rung each supported
effect belongs to: `color`, `aftermarket`, or `condition`.


PRICE COMPARISON

For each of the three appearance groups, capture a small set of the strongest available
public examples and record:

- year / make / model / trim,
- drivetrain/body configuration,
- mileage,
- exterior/interior color when shown,
- relevant appearance package/options,
- asking price or SOLD price,
- SOLD vs ACTIVE,
- source,
- direct URL,
- why it is comparable,
- important differences from the target.

Then compare:

TARGET COMBO
vs.
AVERAGE COMBO
vs.
RAREST / HIGHEST-PRICED OBSERVED COMBO

Look specifically for a MATERIAL price spread.

Do not treat a few hundred dollars or ordinary listing noise as meaningful.

A visual-desirability adjustment should only be proposed when there is a BIG enough
difference to plausibly survive normal variation in:
- mileage,
- condition,
- options,
- geography,
- dealer pricing,
- listing age,
- seller strategy.

Prefer repeated evidence across several close vehicles over one unusually expensive
listing.


VALUATION IMPLICATION

If there is strong evidence of a material appearance premium or discount:

- state the observed public-market spread,
- explain which portion appears plausibly attributable to visual specification,
- explain why it should or should not influence wholesale demand,
- propose a conservative wholesale adjustment RANGE rather than blindly transferring
  the entire retail spread to hammer value.

For example:

"Comparable black/black examples cluster around X while otherwise similar target-color
examples cluster around Y, a public ask spread of approximately Z. After controlling for
mileage/options, there appears to be a meaningful desirability premium. Only part of
that retail premium should transfer to wholesale; evidence supports approximately
+$A to +$B of valuation influence."

Or:

"The target color appears less common, but the available listings do not show a
consistent price premium after controlling for mileage and equipment. No sexiness
adjustment is supported."

Or:

"The target specification consistently trails ordinary-color examples by approximately
$X-$Y despite similar mileage and equipment. This may indicate weaker buyer demand and
supports a modest wholesale discount."

If the evidence is weak, contradictory, too sparse, or materially confounded:

SEXINESS ADJUSTMENT: NONE

Do not force an adjustment.


IMPORTANT WHOLESALE RULE

The sexiness check is supplemental evidence, not the primary valuation anchor.

It must NOT override:
- strong Manheim transaction evidence,
- MMR,
- same-VIN sold evidence,
- materially different condition/history,
- AccuTrade,
- strong GMTV internal sales evidence,
- obvious mechanical/build differences.

Appearance can influence where the vehicle lands WITHIN an evidence-supported wholesale
range and, when there is unusually strong evidence of a large public-market premium or
discount, can support a modest movement of that range.

It may not create unsupported value.


FINAL STRUCTURED RESEARCH RESPONSE

Return JSON matching the exact schema below. Do not wrap it in Markdown or add prose
outside the JSON object.

EXACT JSON SCHEMA:
{schema_contract}

Put the following concise narrative sections
inside `research_memo`, and put the eight mandatory comparison records in `adjustments`:

1. IDENTITY / LISTING DISCOVERY (SUMMARY ONLY; EXCLUDED FROM VALUATION MATH)
    - exact VIN, email, and phone searches performed
    - matched public records
   - URLs
   - active vs sold/archive
    - matched vehicle/status details
    - explicit confirmation that these matches were excluded from valuation math

2. ACCEPTED PUBLIC COMPS
   - strongest genuinely comparable vehicles
   - price, mileage, configuration
   - SOLD vs ACTIVE
   - URLs
   - why accepted

3. EXCLUDED COMPS
   - notable search results that initially looked relevant
   - exact reason each was rejected

4. STRUCTURED ADJUSTMENT RESEARCH

    Populate every required key in the `adjustments` JSON object: mileage, history,
    condition, factory_equipment, aftermarket, color, market_heat, and retail_ceiling.
    Each object must state the claim; normal-vehicle and claimed-feature prices, mileage,
    trim/build, listing status, exact URLs, material differences, signed observed
    difference, SUPPORTED or NOT_SUPPORTED verdict, research summary, and conservative
    wholesale implication. Never mark a claim SUPPORTED unless both comparison objects
    have `found: true`, positive prices, and exact grounded URLs. For an unsuccessful or
    confounded search use NOT_SUPPORTED, `found: false` for the unavailable side, zero
    for unavailable numeric values, and empty strings for unavailable text/URL fields.
    Even when the verdict is NOT_SUPPORTED, retain every useful grounded URL in
    `source_urls` and fully populate any one-sided listing that was found. Describe any
    grounded market direction or range as context, but never infer the missing side of a
    pair or turn contextual evidence into a feature-specific dollar adjustment.
    A NOT_SUPPORTED object proves the claim was investigated; it does not permit skipping
    the search. Also populate `dealer_underwriting` for every category. Use a complete
    NOT_SUPPORTED object with zeros and empty arrays when no source-confirmed impairment
    needs no-pair underwriting. When applicable, include the impairment type, target fact and
    its private source, the dealer question, clean and impaired retail ranges, signed wholesale
    range, methodology, limitations, and finding rows from at least two grounded domains.
    Label each finding's role; SUPPORTED underwriting requires at least one quantified
    `CLEAN_RETAIL` finding and one quantified `IMPAIRED_RETAIL` finding.
    Omit Canadian history from `research_memo` unless qualified evidence
    supports applying its non-zero policy adjustment.

5. SEXINESS / VISUAL DESIRABILITY CHECK

   TARGET COMBO:
   - description of target appearance
   - strongest comparable listings
   - observed pricing

   AVERAGE COMBO:
   - description
   - strongest comparable listings
   - observed pricing

   RAREST / HIGHEST-PRICED OBSERVED COMBO:
   - description
   - evidence supporting why it is considered unusual/desirable
   - strongest comparable listings
   - observed pricing

   PRICE SPREAD:
   - target vs average
   - target vs highest-priced/desirable combo

   SEXINESS ADJUSTMENT:
   - NONE
     or
     - supported wholesale adjustment ranges, separated into COLOR, AFTERMARKET, and
         COSMETIC CONDITION with concise reasoning and exact grounded URLs

6. PUBLIC MARKET IMPLICATION FOR WHOLESALE

   State the implication of all PUBLIC evidence for likely wholesale hammer value.
   Distinguish clearly between:
   - evidence that establishes value,
   - evidence that constrains value,
   - evidence that only indicates desirability/market heat.

7. MARKET TREND
    - chronological GMTV sale-versus-historical-MMR direction
    - whether the market is moving above or below MMR
    - whether MMR appears to be tracking, lagging low, lagging high, or too thin to judge
    - current inventory volume, saturation, mobility, stagnation, and 30-90 day direction
    - whether dated retail evidence may be leading wholesale by roughly one month

8. EVIDENCE VOLUME
    - number of accepted comps and observations
    - number of independent source families
    - recency and comparability
    - contradictions and missing evidence

Keep the memo evidence-driven and concise. The objective is not to find reasons to move
the valuation. The objective is to determine whether the public market genuinely shows
that this exact vehicle's visual specification deserves a meaningful adjustment.
""".strip()

    search_errors = []
    try:
        response = await asyncio.wait_for(
            _run_public_search(prompt),
            timeout=WEBSEARCH_TIMEOUT,
        )
    except Exception as error:
        search_errors.append(_public_research_error_text(error))
        logger.warning("Initial public research failed: %s", search_errors[-1])
        grounding = []
        research = {"research_memo": "", "adjustments": {}}
        attempt_diagnostics = [
            _public_research_error_diagnostics(error, label="initial")
        ]
    else:
        grounding = _grounding_sources(response)
        research, structure_diagnostic = await _parse_or_structure_public_research(
            response,
            grounding,
        )
        attempt_diagnostics = [
            _public_research_response_diagnostics(
                response,
                research,
                label="initial",
                grounding_source_count=len(grounding),
                structure_diagnostic=structure_diagnostic,
            )
        ]
    missing = _missing_research_adjustments(research.get("adjustments"))
    no_initial_grounding = not grounding
    repair_stages = list(RESEARCH_ADJUSTMENT_STAGES) if no_initial_grounding else missing
    if repair_stages:
        repair_prompt = f"""
You are repairing missing public-market evidence for a wholesale vehicle valuation.
Use Google Search now for each required adjustment category: {', '.join(repair_stages)}.

The prior attempt returned no grounded web sources. You MUST execute Google Search and
ground factual public-market claims in its returned sources. Do not report a retail price,
median, listing, sale, supply level, or market direction without grounded source metadata.

TARGET VEHICLE:
{_compact(car, max_chars=20_000)}

For EACH category, search for both:
1. a normal/stock/average retail example of the same vehicle, and
2. a close example carrying the claimed feature or issue.

Return the complete schema. Research the listed categories. For categories that were not
listed, return NOT_SUPPORTED placeholder objects explaining that this repair call did not
re-research them; those placeholders will not replace the original results.
SUPPORTED requires both exact grounded URLs, prices, mileage, trim/build, ACTIVE or SOLD
status, material differences, and signed observed difference. Use NOT_SUPPORTED only
after targeted searches and state what evidence was unavailable or confounded. Preserve
all useful grounded URLs in `source_urls`, fully populate any available one-sided
listing, and report grounded direction/range only as non-dollar context.

For every listed category also populate `dealer_underwriting`. When private evidence
confirms a material impairment but no controlled vehicle pair exists, research the
buying dealer's legal, financing, insurance, buyer-pool, retail-exit, recon, liquidity,
and margin questions. SUPPORTED underwriting requires at least two independent grounded
domains and quantified clean retail, impaired retail exit, and conservative signed
wholesale-adjustment ranges. Public research cannot establish that the target has the
issue. Otherwise return the complete NOT_SUPPORTED underwriting object with zeros and
empty arrays.

Return only JSON matching this exact schema:
{schema_contract}
""".strip()
        try:
            retry = await asyncio.wait_for(
                _run_public_search(repair_prompt),
                timeout=WEBSEARCH_TIMEOUT,
            )
        except Exception as error:
            search_errors.append(_public_research_error_text(error))
            logger.warning("Repair public research failed: %s", search_errors[-1])
            attempt_diagnostics.append(
                _public_research_error_diagnostics(error, label="repair")
            )
        else:
            retry_grounding = _grounding_sources(retry)
            repaired, repair_structure_diagnostic = await _parse_or_structure_public_research(
                retry,
                retry_grounding,
            )
            attempt_diagnostics.append(
                _public_research_response_diagnostics(
                    retry,
                    repaired,
                    label="repair",
                    grounding_source_count=len(retry_grounding),
                    structure_diagnostic=repair_structure_diagnostic,
                )
            )
            repaired_adjustments = repaired.get("adjustments")
            if not isinstance(repaired_adjustments, dict):
                repaired_adjustments = {}
            adjustments = research.get("adjustments")
            if not isinstance(adjustments, dict):
                adjustments = {}
                research["adjustments"] = adjustments
            for stage_id in repair_stages:
                if (
                    _research_adjustment_covered(repaired_adjustments.get(stage_id))
                    and _public_research_row_quality(
                        repaired_adjustments.get(stage_id), retry_grounding
                    )
                    > _public_research_row_quality(adjustments.get(stage_id), grounding)
                ):
                    adjustments[stage_id] = repaired_adjustments[stage_id]
            repair_memo = str(repaired.get("research_memo") or "").strip()
            if repair_memo:
                research["research_memo"] = (
                    str(research.get("research_memo") or "").strip()
                    + "\n\nTARGETED ADJUSTMENT RESEARCH REPAIR\n"
                    + repair_memo
                ).strip()
            grounding.extend(retry_grounding)
            missing = _missing_research_adjustments(research.get("adjustments"))
            if missing:
                logger.warning(
                    "Public research did not explicitly cover adjustment categories after retry: %s",
                    ", ".join(missing),
                )

    grounding = list({row.get("grounding_url") or row.get("url"): row for row in grounding}.values())
    grounding = await _resolve_grounding_sources(grounding)
    _verify_public_research_grounding(research.get("adjustments"), grounding)
    grounded = bool(grounding)
    research_text = str(research.get("research_memo") or "") if grounded else ""
    if not grounded:
        logger.warning("Public market research returned no grounded sources after retry")
    return {
        "research_text": research_text,
        "grounding": grounding,
        "research_status": {
            "attempted": True,
            "grounded": grounded,
            "grounded_source_count": len(grounding),
            "retried_for_missing_grounding": no_initial_grounding,
            "recovered_after_initial_error": bool(search_errors and grounded),
            "errors": search_errors,
            "attempts": attempt_diagnostics,
            "structured_complete": not missing,
            "missing_stages": missing,
            "message": (
                "Grounded public-market research and complete structured adjustment objects are available."
                if grounded and not missing
                else (
                    "Public-market search failed, so valuation continued without public research: "
                    + "; ".join(search_errors)
                )
                if search_errors and not grounded
                else (
                    "Google Search returned grounded sources, but structured adjustment objects remain incomplete: "
                    + ", ".join(missing)
                )
                if grounded
                else (
                    "Complete structured adjustment objects were returned, but neither Search attempt supplied grounding metadata."
                    if not missing
                    else "Public-market research returned neither grounding metadata nor complete structured adjustment objects."
                )
            ),
        },
        "specialty_auction_research": specialty_research,
        "adjustment_research": {
            "required": list(RESEARCH_ADJUSTMENT_STAGES),
            "by_stage": research.get("adjustments") or {},
            "covered": [
                stage_id for stage_id in RESEARCH_ADJUSTMENT_STAGES
                if stage_id not in missing
            ],
            "missing_after_retry": missing,
            "complete": not missing,
        },
    }


async def _run_public_search(prompt: str) -> Any:
    runner = _RUNTIME.get("websearch_runner")
    if runner is not None:
        return await _call(
            runner,
            prompt,
            model=WEBSEARCH_MODEL,
            temperature=0.1,
        )

    def search():
        return _global_vertex_client().models.generate_content(
            model=WEBSEARCH_MODEL,
            contents=prompt,
            config=_public_search_generation_config(),
        )

    return await asyncio.to_thread(search)


def _public_search_generation_config() -> types.GenerateContentConfig:
    """Build a Search-compatible config; controlled generation is unsupported here."""
    return types.GenerateContentConfig(
        **_public_search_generation_kwargs(),
        tools=[types.Tool(google_search=types.GoogleSearch())],
        automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
    )


def _public_search_generation_kwargs() -> dict[str, Any]:
    return {"temperature": 0.1}


def _parse_public_research(response: Any) -> dict[str, Any]:
    text_value = str(getattr(response, "text", "") or "").strip()
    candidates = [text_value]
    fenced = re.fullmatch(r"```(?:json)?\s*([\s\S]*?)\s*```", text_value, re.IGNORECASE)
    if fenced:
        candidates.append(fenced.group(1).strip())
    first_brace = text_value.find("{")
    last_brace = text_value.rfind("}")
    if first_brace >= 0 and last_brace > first_brace:
        candidates.append(text_value[first_brace:last_brace + 1])

    for candidate in dict.fromkeys(candidates):
        try:
            parsed = json.loads(candidate)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(parsed, dict):
            return parsed

    logger.warning("Public research returned invalid structured JSON")
    return {}


async def _parse_or_structure_public_research(
    response: Any,
    grounding: Optional[list[dict[str, str]]] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    parsed = _parse_public_research(response)
    initial_missing = _missing_research_adjustments(parsed.get("adjustments"))
    diagnostic = {
        "attempted": False,
        "improved": False,
        "initial_missing_stages": initial_missing,
        "final_missing_stages": initial_missing,
        "error": "",
    }
    if not initial_missing:
        return parsed, diagnostic

    raw_text = str(getattr(response, "text", "") or "").strip()
    if not raw_text:
        return parsed, diagnostic

    diagnostic["attempted"] = True
    prompt = f"""
Convert the Google Search research response below into the required public-research JSON
schema. This is formatting and evidence classification only. Do not search, add facts,
invent URLs, infer prices, or turn unsupported prose into evidence. Preserve every useful
URL and observed value already present. For unavailable or confounded fields, emit a
complete NOT_SUPPORTED object using false, zero, empty strings, and empty arrays as
appropriate. Populate every required adjustment category and every dealer_underwriting
field. A structurally complete NOT_SUPPORTED row is preferable to invented evidence.

SEARCH RESPONSE:
{raw_text[:120_000]}

GROUNDING SOURCES AND CITED SUPPORT TEXT:
{_compact(grounding or [], max_chars=40_000)}
""".strip()

    def structure():
        return _global_vertex_client().models.generate_content(
            model=WEBSEARCH_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
                response_schema=PUBLIC_RESEARCH_SCHEMA,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(
                    disable=True
                ),
            ),
        )

    try:
        structured_response = await asyncio.wait_for(
            asyncio.to_thread(structure),
            timeout=WEBSEARCH_TIMEOUT,
        )
        structured = _parse_public_research(structured_response)
        final_missing = _missing_research_adjustments(structured.get("adjustments"))
        diagnostic["final_missing_stages"] = final_missing
        if len(final_missing) < len(initial_missing):
            diagnostic["improved"] = True
            return structured, diagnostic
    except Exception as error:
        diagnostic["error"] = f"{type(error).__name__}: {error}"
        logger.warning("Public research schema normalization failed: %s", diagnostic["error"])
    return parsed, diagnostic


def _public_research_response_diagnostics(
    response: Any,
    research: dict[str, Any],
    *,
    label: str,
    grounding_source_count: int,
    structure_diagnostic: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    finish_reasons = []
    for candidate in getattr(response, "candidates", None) or []:
        reason = getattr(candidate, "finish_reason", None)
        if reason is not None:
            finish_reasons.append(str(reason))
    missing = _missing_research_adjustments(research.get("adjustments"))
    return {
        "label": label,
        "model": WEBSEARCH_MODEL,
        "response_text_chars": len(str(getattr(response, "text", "") or "")),
        "parsed_object": bool(research),
        "covered_stages": [
            stage_id for stage_id in RESEARCH_ADJUSTMENT_STAGES
            if stage_id not in missing
        ],
        "missing_stages": missing,
        "grounding_source_count": grounding_source_count,
        "finish_reasons": list(dict.fromkeys(finish_reasons)),
        "schema_normalization": structure_diagnostic or {},
    }


def _public_research_error_text(error: Exception) -> str:
    detail = str(error).strip()
    return f"{type(error).__name__}: {detail}" if detail else type(error).__name__


def _public_research_error_diagnostics(
    error: Exception,
    *,
    label: str,
) -> dict[str, Any]:
    return {
        "label": label,
        "model": WEBSEARCH_MODEL,
        "response_text_chars": 0,
        "parsed_object": False,
        "covered_stages": [],
        "missing_stages": list(RESEARCH_ADJUSTMENT_STAGES),
        "grounding_source_count": 0,
        "finish_reasons": [],
        "schema_normalization": {},
        "error": _public_research_error_text(error),
    }


def _missing_research_adjustments(adjustments: Any) -> list[str]:
    rows = adjustments if isinstance(adjustments, dict) else {}
    return [
        stage_id
        for stage_id in RESEARCH_ADJUSTMENT_STAGES
        if not _research_adjustment_covered(rows.get(stage_id))
    ]


def _public_research_row_richness(row: Any) -> int:
    if not _research_adjustment_covered(row):
        return -1
    score = 1
    if row.get("verdict") == "SUPPORTED":
        score += 100
    score += 10 * len({str(url).strip() for url in row.get("source_urls") or [] if str(url).strip()})
    score += 20 * sum(
        bool((row.get(key) or {}).get("found"))
        for key in ("normal_comparison", "feature_comparison")
    )
    underwriting = row.get("dealer_underwriting") or {}
    if underwriting.get("verdict") == "SUPPORTED":
        score += 80
    score += 5 * len({str(url).strip() for url in underwriting.get("source_urls") or [] if str(url).strip()})
    return score


def _public_research_row_quality(row: Any, grounding: list[dict]) -> int:
    richness = _public_research_row_richness(row)
    if richness < 0:
        return richness
    grounded_urls = {
        _canonical_public_url(value)
        for source in grounding or []
        for value in (source.get("url"), source.get("grounding_url"))
        if _canonical_public_url(value)
    }
    cited_urls = {
        str(url).strip()
        for url in row.get("source_urls") or []
        if str(url).strip()
    }
    for key in ("normal_comparison", "feature_comparison"):
        url = str((row.get(key) or {}).get("url") or "").strip()
        if url:
            cited_urls.add(url)
    underwriting = row.get("dealer_underwriting") or {}
    cited_urls.update(
        str(url).strip()
        for url in underwriting.get("source_urls") or []
        if str(url).strip()
    )
    cited_urls.update(
        str(finding.get("url") or "").strip()
        for finding in underwriting.get("findings") or []
        if isinstance(finding, dict) and str(finding.get("url") or "").strip()
    )
    matched = sum(
        _canonical_public_url(url) in grounded_urls for url in cited_urls
    )
    return richness + (matched * 1_000)


def _research_adjustment_covered(row: Any) -> bool:
    if not isinstance(row, dict):
        return False
    if row.get("verdict") not in {"SUPPORTED", "NOT_SUPPORTED"}:
        return False
    if not set(_PUBLIC_ADJUSTMENT_SCHEMA["required"]).issubset(row):
        return False
    if not isinstance(row.get("observed_difference"), (int, float)) or isinstance(
        row.get("observed_difference"), bool
    ):
        return False
    if not all(
        isinstance(row.get(key), str)
        for key in ("claim", "research_summary", "wholesale_implication")
    ):
        return False
    if not isinstance(row.get("source_urls"), list) or not all(
        isinstance(url, str) for url in row.get("source_urls")
    ):
        return False

    for key in ("normal_comparison", "feature_comparison"):
        comparison = row.get(key)
        if not isinstance(comparison, dict):
            return False
        if not set(_PUBLIC_COMPARISON_SCHEMA["required"]).issubset(comparison):
            return False
        if not isinstance(comparison.get("found"), bool):
            return False
        if any(
            not isinstance(comparison.get(number_key), (int, float))
            or isinstance(comparison.get(number_key), bool)
            for number_key in ("price", "mileage")
        ):
            return False
        if not all(
            isinstance(comparison.get(text_key), str)
            for text_key in ("url", "build", "listing_status", "material_differences")
        ):
            return False

    underwriting = row.get("dealer_underwriting")
    if not isinstance(underwriting, dict):
        return False
    if not set(_PUBLIC_DEALER_UNDERWRITING_SCHEMA["required"]).issubset(underwriting):
        return False
    if underwriting.get("verdict") not in {"SUPPORTED", "NOT_SUPPORTED"}:
        return False
    if not isinstance(underwriting.get("applicable"), bool):
        return False
    if any(
        not isinstance(underwriting.get(key), (int, float))
        or isinstance(underwriting.get(key), bool)
        for key in (
            "clean_retail_reference_low", "clean_retail_reference_high",
            "impaired_retail_exit_low", "impaired_retail_exit_high",
            "recommended_wholesale_adjustment_low",
            "recommended_wholesale_adjustment_high",
        )
    ):
        return False
    if not all(
        isinstance(underwriting.get(key), str)
        for key in (
            "impairment_type", "confirmed_target_fact", "target_fact_source",
            "dealer_question", "methodology", "limitations",
        )
    ):
        return False
    if not isinstance(underwriting.get("source_urls"), list) or not all(
        isinstance(url, str) for url in underwriting.get("source_urls")
    ):
        return False
    if not isinstance(underwriting.get("findings"), list):
        return False
    allowed_roles = set(
        _PUBLIC_UNDERWRITING_FINDING_SCHEMA["properties"]["finding_role"]["enum"]
    )
    for finding in underwriting.get("findings"):
        if not isinstance(finding, dict):
            return False
        if not set(_PUBLIC_UNDERWRITING_FINDING_SCHEMA["required"]).issubset(finding):
            return False
        if finding.get("finding_role") not in allowed_roles:
            return False
        if any(
            not isinstance(finding.get(key), (int, float))
            or isinstance(finding.get(key), bool)
            for key in ("amount_low", "amount_high")
        ):
            return False
        if not all(
            isinstance(finding.get(key), str)
            for key in (
                "url", "source_type", "finding_role", "finding",
                "vehicle_scope", "observation_date",
            )
        ):
            return False
    if row.get("verdict") == "SUPPORTED":
        source_urls = {
            str(url).strip() for url in row.get("source_urls") or []
            if str(url).strip()
        }
        for key in ("normal_comparison", "feature_comparison"):
            comparison = row[key]
            if not comparison.get("found"):
                return False
            if float(comparison.get("price") or 0) <= 0:
                return False
            comparison_url = str(comparison.get("url") or "").strip()
            if not comparison_url or comparison_url not in source_urls:
                return False
            if _number(comparison.get("mileage")) is None:
                return False
            if not str(comparison.get("build") or "").strip():
                return False
            if str(comparison.get("listing_status") or "").strip().upper() not in {
                "ACTIVE", "SOLD"
            }:
                return False
            if not str(comparison.get("material_differences") or "").strip():
                return False
    return True


def _canonical_public_url(value: Any) -> str:
    parsed = urlparse(str(value or "").strip())
    host = str(parsed.hostname or "").lower().removeprefix("www.")
    if not host:
        return ""
    try:
        port = parsed.port
    except ValueError:
        return ""
    if port and not (
        (parsed.scheme.lower() == "http" and port == 80)
        or (parsed.scheme.lower() == "https" and port == 443)
    ):
        host = f"{host}:{port}"
    path = parsed.path.rstrip("/") or "/"
    query = urlencode(sorted(
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
        and key.lower() not in {
            "_ga", "_gl", "dclid", "fbclid", "gclid", "mc_cid", "mc_eid",
            "msclkid",
        }
    ))
    return f"{host}{path}?{query}".rstrip("?")


def _grounding_text_mentions_number(text: Any, value: Any) -> bool:
    number = _number(value)
    if number is None:
        return False
    normalized_text = re.sub(r"(?<=\d),(?=\d)", "", str(text or ""))
    token = str(int(number)) if float(number).is_integer() else str(float(number))
    return re.search(rf"(?<![\d.]){re.escape(token)}(?:\.0+)?(?!\d)", normalized_text) is not None


def _grounded_public_comparison_source(
    comparison: Any,
    grounding: list[dict],
) -> str:
    """Return the grounded publisher URL that proves a priced comparison."""
    if not isinstance(comparison, dict):
        return ""
    cited_url = str(comparison.get("url") or "").strip()
    cited_canonical = _canonical_public_url(cited_url)
    cited_host = str(urlparse(cited_url).hostname or "").lower().removeprefix("www.")
    if not cited_canonical or not cited_host:
        return ""

    for source in grounding:
        if not isinstance(source, dict):
            continue
        source_url = str(source.get("url") or source.get("grounding_url") or "").strip()
        source_values = (source.get("url"), source.get("grounding_url"))
        if any(_canonical_public_url(value) == cited_canonical for value in source_values):
            return source_url or cited_url

    price = _number(comparison.get("price"))
    mileage = _number(comparison.get("mileage"))
    if price is None or price <= 0 or mileage is None or mileage < 0:
        return ""

    for source in grounding:
        if not isinstance(source, dict):
            continue
        source_url = str(source.get("url") or "").strip()
        source_host = str(urlparse(source_url).hostname or "").lower().removeprefix("www.")
        support_text = str(source.get("reasoning") or "")
        if (
            source_host == cited_host
            and _grounding_text_mentions_number(support_text, price)
            and _grounding_text_mentions_number(support_text, mileage)
        ):
            return source_url
    return ""


def _public_research_evidence_tier(
    row: Any, *, adjustment_eligible: bool = False, underwriting_eligible: bool = False
) -> str:
    if adjustment_eligible:
        return VERIFIED_PRICED_PAIR
    if underwriting_eligible:
        return VERIFIED_DEALER_UNDERWRITING
    verification = (row.get("grounding_verification") or {}) if isinstance(row, dict) else {}
    if verification.get("matched_priced_comparison_urls"):
        return VERIFIED_LISTING_CONTEXT
    if verification.get("matched_source_urls"):
        return VERIFIED_MARKET_CONTEXT
    return UNVERIFIED_CONTEXT


def _verify_public_research_grounding(adjustments: Any, grounding: list[dict]) -> None:
    """Prevent ungrounded model URLs from supporting a public adjustment."""
    if not isinstance(adjustments, dict):
        return
    grounded_urls = {
        _canonical_public_url(value)
        for source in grounding
        for value in (source.get("url"), source.get("grounding_url"))
        if _canonical_public_url(value)
    }
    for row in adjustments.values():
        if not isinstance(row, dict):
            continue
        underwriting = row.get("dealer_underwriting") or {}
        underwriting_urls = {
            str(url).strip()
            for url in underwriting.get("source_urls") or []
            if str(url).strip()
        }
        underwriting_urls.update(
            str(finding.get("url") or "").strip()
            for finding in underwriting.get("findings") or []
            if isinstance(finding, dict) and str(finding.get("url") or "").strip()
        )
        context_urls = {
            str(url).strip()
            for url in row.get("source_urls") or []
            if str(url).strip()
        } | underwriting_urls
        comparisons = []
        for key in ("normal_comparison", "feature_comparison"):
            comparison = row.get(key) or {}
            if comparison.get("found") and str(comparison.get("url") or "").strip():
                comparisons.append(comparison)
        comparison_urls = {
            str(comparison["url"]).strip()
            for comparison in comparisons
        }
        cited_urls = context_urls | comparison_urls
        directly_matched_sources = {
            url for url in cited_urls
            if _canonical_public_url(url) in grounded_urls
        }
        comparison_matches = [
            (comparison, _grounded_public_comparison_source(comparison, grounding))
            for comparison in comparisons
        ]
        matched_comparisons = sorted({
            source_url for _, source_url in comparison_matches if source_url
        })
        matched_priced_comparisons = sorted(
            source_url
            for comparison, source_url in comparison_matches
            if source_url and float(_number(comparison.get("price")) or 0) > 0
        )
        unmatched_comparisons = sorted({
            str(comparison.get("url") or "").strip()
            for comparison, source_url in comparison_matches
            if not source_url
        })
        matched_sources = sorted(directly_matched_sources | set(matched_comparisons))
        unmatched_context = sorted(
            url for url in context_urls - comparison_urls
            if _canonical_public_url(url) not in grounded_urls
        )
        matched_underwriting = sorted(
            url for url in underwriting_urls
            if _canonical_public_url(url) in grounded_urls
        )
        unmatched_underwriting = sorted(
            url for url in underwriting_urls
            if _canonical_public_url(url) not in grounded_urls
        )
        row["grounding_verification"] = {
            "verified": not unmatched_comparisons,
            "unmatched_urls": unmatched_comparisons,
            "unmatched_comparison_urls": unmatched_comparisons,
            "unmatched_context_urls": unmatched_context,
            "matched_underwriting_urls": matched_underwriting,
            "unmatched_underwriting_urls": unmatched_underwriting,
            "matched_source_urls": matched_sources,
            "matched_comparison_urls": matched_comparisons,
            "matched_priced_comparison_urls": matched_priced_comparisons,
            "verified_source_count": len(matched_sources),
            "verified_domain_count": len({
                str(urlparse(url).hostname or "").lower().removeprefix("www.")
                for url in matched_sources
                if urlparse(url).hostname
            }),
        }
        if row.get("verdict") == "SUPPORTED" and unmatched_comparisons:
            row["verdict"] = "NOT_SUPPORTED"
            row["observed_difference"] = 0
            row["wholesale_implication"] = "$0; cited comparison URL was not grounded."
            summary = str(row.get("research_summary") or "").strip()
            row["research_summary"] = (
                summary + " Grounding verification failed for one or more cited URLs."
            ).strip()
        row["evidence_tier"] = _public_research_evidence_tier(
            row,
            adjustment_eligible=(
                row.get("verdict") == "SUPPORTED"
                and not unmatched_comparisons
                and len(matched_priced_comparisons) == 2
            ),
        )


def _extract_pdf_text(path_value: Any, expected_vin: str = "") -> tuple[str, dict[str, Any]]:
    path = Path(str(path_value or ""))
    status: dict[str, Any] = {
        "available": bool(path_value),
        "extracted": False,
        "page_count": 0,
        "character_count": 0,
        "truncated": False,
        "fallback_to_pdf": False,
    }
    if not path_value or not path.exists() or path.stat().st_size > MAX_ARTIFACT_BYTES:
        return "", status
    if PdfReader is None:
        status["error"] = "pypdf is not installed"
        return "", status

    try:
        reader = PdfReader(str(path))
        pages = [str(page.extract_text() or "").strip() for page in reader.pages]
        text = "\n\n".join(page for page in pages if page).strip()
    except Exception as error:
        status["error"] = f"{type(error).__name__}: {error}"
        logger.info("Could not extract valuation PDF text from %s: %s", path, error)
        return "", status

    normalized = re.sub(r"\s+", "", text).upper()
    vin = _clean_vin(expected_vin)
    status.update({
        "page_count": len(reader.pages),
        "character_count": len(text),
        "vin_matched": not vin or vin in normalized,
    })
    if not text:
        status["error"] = "pypdf extracted no text"
        return "", status
    if vin and not status["vin_matched"]:
        status["error"] = f"extracted report did not contain expected VIN {vin}"
        return "", status

    status["extracted"] = True
    return text, status


_DOSSIER_OMIT_KEYS = {
    "accutrade_screenshot",
    "carfax_pdf",
    "autocheck_pdf",
    "image_url",
    "grounding_url",
    "raw_html",
    "html",
    "base64",
}


def _dossier_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _dossier_value(item)
            for key, item in value.items()
            if str(key).lower() not in _DOSSIER_OMIT_KEYS
        }
    if isinstance(value, list):
        return [_dossier_value(item) for item in value]
    return _jsonable(value)


def _build_evidence_dossier(evidence: dict, market: dict) -> dict[str, Any]:
    """Retain valuation facts while removing binary paths and duplicate transport data."""
    browser = evidence.get("browser") or {}
    return {
        "manheim": _dossier_value(evidence.get("manheim") or {}),
        "gmtv": _dossier_value(evidence.get("gmtv") or {}),
        "jdpower": _dossier_value(evidence.get("jdpower") or {}),
        "browser_vhr": _dossier_value({
            "accutrade": browser.get("accutrade") or {},
            "report_text": browser.get("report_text") or {},
            "report_text_status": browser.get("report_text_status") or {},
            "stage_status": browser.get("stage_status") or {},
            "autocheck": browser.get("autocheck") or {},
            "artifact_summary": browser.get("artifact_summary") or {},
            "errors": browser.get("errors") or [],
        }),
        "crm_photo_observations": _dossier_value(evidence.get("crm_photos") or {}),
        "public_research": {
            "adjustment_research": _dossier_value(
                market.get("adjustment_research") or {}
            ),
            "grounding": _dossier_value(market.get("grounding") or []),
            "specialty_auction_research": _dossier_value(
                market.get("specialty_auction_research") or {}
            ),
            "research_text": str(market.get("research_text") or ""),
        },
    }


def _artifact_parts(evidence: dict) -> list[Any]:
    browser = evidence.get("browser") or {}
    parts: list[Any] = []

    path_value = browser.get("accutrade_screenshot")
    if path_value:
        path = Path(str(path_value))
        try:
            if path.exists() and path.stat().st_size <= MAX_ARTIFACT_BYTES:
                parts.append(types.Part.from_text(text="AccuTrade appraisal screenshot:"))
                parts.append(types.Part.from_bytes(data=path.read_bytes(), mime_type="image/png"))
        except Exception:
            logger.exception("Could not attach valuation artifact %s", path_value)

    report_text = browser.get("report_text") or {}
    for key, label in (
        ("carfax", "CARFAX report"),
        ("autocheck", "AutoCheck report"),
    ):
        extracted = str(report_text.get(key) or "").strip()
        if extracted:
            parts.append(types.Part.from_text(text=f"{label} extracted text:\n{extracted}"))

    return parts


async def _crm_photo_parts(evidence: dict) -> list[Any]:
    """Attach the CRM photos themselves so pass 2 reads pixels, not only Ava's notes."""

    images = ((evidence.get("crm_photos") or {}).get("images") or [])[:CRM_PHOTO_ATTACH_LIMIT]
    if not images:
        return []

    try:
        from .ava_image_agents import build_image_parts
    except Exception:
        logger.exception("CRM photo attachment unavailable")
        return []

    parts: list[Any] = []
    for index, image in enumerate(images):
        url = str(image.get("image_url") or "").strip()
        if not url:
            continue

        try:
            built = await build_image_parts([], [url])
        except Exception:
            logger.exception("Could not attach CRM photo %s", url)
            continue

        if not built:
            continue

        label = " | ".join(
            part
            for part in (
                image.get("type") or f"CRM photo {index + 1}",
                "AVA FLAGGED DAMAGE" if image.get("damaged") else "",
            )
            if part
        )
        parts.append(
            types.Part.from_text(
                text=f"CRM vehicle photo [{label}]: {image.get('description') or ''}".strip()
            )
        )
        parts.extend(built)

    return parts


async def _pass2_reasoning(car: dict, evidence: dict, market: dict) -> str:
    client = _global_vertex_client()
    dossier = _build_evidence_dossier(evidence, market)
    private_dossier = {
        key: value for key, value in dossier.items() if key != "public_research"
    }
    public_research = dossier.get("public_research") or {}
    public_context = {
        key: value for key, value in public_research.items()
        if key != "adjustment_research"
    }

    prompt = f"""
You are simulating the dealer at the GMTV Auction. This is pass 2 of 3.
Perform the ACTUAL valuation reasoning now. Do not web-search in this pass; pass 1 already
curated the public evidence. Present the final value as TARGET HAMMER. 

TARGET VEHICLE:
{_compact(car, max_chars=20_000)}

PRIVATE/FIRST-PARTY EVIDENCE:
{_compact(private_dossier, max_chars=120_000)}

PASS-1 STRUCTURED ADJUSTMENT AND DEALER-UNDERWRITING RESEARCH:
{_compact(public_research.get('adjustment_research') or {}, max_chars=80_000)}

PASS-1 PUBLIC MARKET CONTEXT, SOURCES, AND MEMO:
{_compact(public_context, max_chars=60_000)}

You are the final valuation analyst for a strong GMTV wholesale auction lane.

Your job is NOT to produce a conservative appraisal or mechanically adjust MMR.

Your job is to predict the most likely TARGET HAMMER:
the price at which a capable, experienced dealer is likely to stop bidding when this
specific vehicle crosses a strong GMTV wholesale lane.

THINK LIKE THE DEALER HOLDING THE BIDDER CARD.

Assume the likely buyers:
- understand the vehicle and their local retail market,
- know their probable recon costs,
- understand common mechanical risks for the model,
- know what colors/configurations/packages their customers want,
- understand what they can retail the vehicle for,
- are willing to take calculated risk when there is enough upside,
- still require enough expected margin to justify tying up capital.
- Dealers generally pay around 15-20% less for vehicles with confirmed Canadian history.
- Canadian history may be confirmed only by the attached CARFAX or AutoCheck report. If
    neither report is available, an exact-VIN grounded public match may confirm it. Never infer
    Canadian history from Manheim, GMTV, AccuTrade, JD Power, a generic search result, or a
    different VIN.

Do not value the vehicle as though every unknown is a guaranteed worst-case loss.

Dealers knowingly buy imperfect vehicles when the expected upside compensates them
for the risk.

Likewise, do not assume retail upside automatically justifies paying more. The upside
must remain after realistic recon, fees, transport, holding costs, mechanical risk,
liquidity risk, and dealer margin. Take into account Accutrade's number, and MMR, when forming your valuation. Don


================================================================
CORE QUESTION
================================================================

At each major decision point ask:

"If I were an experienced dealer watching this vehicle cross the lane, knowing all of
this evidence, would I raise my hand again at this price?"

The TARGET HAMMER is approximately the point where enough rational wholesale buyers
would stop saying yes.

Predict MARKET BEHAVIOR, not theoretical intrinsic value.


================================================================
DEALER RISK / OPPORTUNITY MODEL
================================================================

Dealers price uncertainty as a probability-weighted business risk, not automatically
as the full cost of the worst possible outcome.

For meaningful defects or uncertainty, reason through:

1. What is actually CONFIRMED?
2. What is merely POSSIBLE?
3. What would the likely repair/recon cost be?
4. What is a reasonable adverse-case cost?
5. How obvious is the problem to bidders?
6. How much retail upside remains if the adverse case occurs?
7. Is this the type of vehicle where dealers are likely to accept that risk?
8. How much bidding competition should exist for that opportunity?

Example reasoning:

A vehicle may have:
- approximately $2,000 wholesale book/MMR,
- a possible or known transmission problem,
- a clean and attractive exterior,
- strong retail presentation,
- approximately $9,000 credible retail potential.

Do NOT automatically reason:

"$3,000 transmission risk means subtract $3,000."

A dealer may rationally pay ABOVE the $2,000 MMR if:
- the exterior/interior keeps recon low elsewhere,
- there is substantial retail spread,
- the mechanical problem is repairable and understood,
- the expected repair cost is materially lower than the full retail spread,
- parts/labor availability is reasonable,
- the vehicle has a liquid buyer pool,
- the downside still leaves an acceptable outcome.

For example, a bidder might rationally pay $3,000 for a $2,000-MMR vehicle if a
credible $9,000 retail exit leaves enough room for the expected mechanical repair,
auction expense, transport, recon, carrying cost, and profit.

That does NOT mean "$9,000 retail = $3,000 hammer."
It means the retail opportunity can explain why rational bidders may knowingly
up-pay relative to MMR.

Conversely, a defect may deserve MORE than its nominal repair cost as a wholesale
discount when:
- diagnosis is uncertain,
- the defect could hide a much larger failure,
- the vehicle is difficult to transport or test,
- parts are scarce,
- repair time is long,
- the buyer pool is thin,
- retail liquidity is weak,
- or bidders cannot confidently quantify the downside.


================================================================
EXPECTED VALUE VS WORST CASE
================================================================

For uncertain risks, distinguish:

CONFIRMED COST:
A known expense supported by evidence.

EXPECTED RISK:
Probability-weighted economic exposure.

TAIL RISK:
A lower-probability adverse scenario that should affect confidence and bidding
behavior but should not automatically be deducted dollar-for-dollar.

Do not fabricate numerical probabilities.

When the evidence cannot support a probability, reason qualitatively:
low / moderate / high likelihood and low / moderate / high consequence.

A sophisticated dealer may accept meaningful tail risk when the gross spread is wide.


================================================================
RETAIL-TO-WHOLESALE BID CEILING
================================================================

Use credible retail evidence as more than just a negative "ceiling."

Retail economics can operate in BOTH directions.

A LOW retail ceiling can force wholesale value down.

A LARGE credible retail spread can support aggressive wholesale bidding when the
vehicle's risk/recon profile still leaves meaningful profit.

Conceptually evaluate:

LIKELY RETAIL EXIT
minus
auction/buyer fees
minus
transport
minus
expected recon
minus
expected mechanical exposure
minus
holding/capital cost
minus
required dealer margin
minus
an uncertainty/liquidity buffer

=

APPROXIMATE ECONOMIC BID CEILING

Do not invent expenses simply to complete this formula.

Use the supplied evidence to estimate the relationship qualitatively or quantitatively
when defensible.

The TARGET HAMMER should normally remain below this economic bid ceiling.

A wide gap between current wholesale evidence and economic bid ceiling can explain
why bidders may push ABOVE MMR.

A narrow gap explains why they will not.


================================================================
DEALER PERSONA / MARKET BEHAVIOR
================================================================

Do not behave like a risk-averse bank or inspection company.

A capable dealer:
- buys opportunities,
- accepts repairable imperfections,
- recognizes unusually clean cosmetics,
- recognizes desirable color/equipment,
- understands when MMR misses unusual configurations,
- pays for inventory they believe will retail quickly,
- discounts vehicles that will consume time, shop capacity, or floorplan,
- becomes more aggressive when several buyers can see the same obvious upside.

The dealer does NOT require certainty.

The dealer requires a sufficiently attractive risk-adjusted trade.

Also distinguish between:

PRIVATE UPSIDE:
Something only a particularly knowledgeable buyer might recognize.

OBVIOUS LANE UPSIDE:
Something several bidders will recognize and therefore bid into the hammer.

TARGET HAMMER depends primarily on what the COMPETITIVE BUYER POOL will recognize,
not the maximum value one unusually skilled buyer could extract.


================================================================
EVIDENCE HIERARCHY
================================================================

Start from actual Manheim current MMR and transaction evidence when valid.

Use evidence in roughly this order of importance, while allowing stronger contradictory
evidence to override weaker evidence:

1. Exact-VIN Manheim transactions / sale history.
2. Closely matched Manheim transactions.
3. Current adjusted MMR and its sample quality.
4. Manheim Condition Report.
5. GMTV first-party sold performance.
6. AccuTrade valuation/condition evidence.
7. CARFAX and AutoCheck history.
8. Exact-build/spec evidence from JD Power.
9. Same-VIN public retail evidence.
10. Strong genuinely comparable public listings/sales.
11. Broader market evidence and market heat.

MMR is an anchor, not a command.

Before choosing the baseline, read BOTH current adjusted MMR and AccuTrade Target
Auction. On AccuTrade, Target Auction / Bring Money is the only usable valuation. NEVER
use Give Me Money, Instant Offer, or another direct-purchase offer in the baseline,
range, adjustment ladder, or target hammer. Compare the two valid anchors against sample
quality, exact build/package coverage, mileage, condition, and sold transactions. Select
whichever is the stronger, more vehicle-specific baseline, explain why, and do not
average them or default to MMR merely because it is available.

If and only if neither usable anchor exists, use the verified public-auction contingency
from Pass 1. A public-auction fallback baseline requires at least two close SOLD results
from two independent supported auction domains; three or more are strongly preferred.
It must cite each original result URL and show the retail/collector-auction hammer before
the dealer-economics translation. Never use an active ask, current bid, estimate,
reserve-not-met result, or identity-only VIN/email/phone match. If the public auction
buyer pool differs materially from GMTV wholesale buyers, make the translation explicit
and conservative. If the minimum evidence is unavailable, state that no defensible
baseline exists rather than inventing one.

Do not mechanically force the final hammer toward MMR when the transaction, condition,
retail-spread, configuration, or demand evidence explains why actual bidders would
behave differently.


================================================================
MANHEIM TRANSACTION ANALYSIS
================================================================

Properly analyze ALL available Manheim transaction sets:

LIVE_SALE / VIN TRANSACTIONS:
Treat exact-VIN transaction history as extremely important when legitimate.
Understand sale dates, mileage, condition, announcements, prior no-sales, and whether
the vehicle itself has crossed before.

COMP_SALES / YMM TRANSACTIONS:
Use same year/make/model evidence to understand the immediate wholesale market.
Filter aggressively for meaningful build, trim, drivetrain, mileage, and condition
differences.

ALL_SALES / MAKE-MODEL TRANSACTIONS:
Use broader make/model transactions primarily to understand:
- market distribution,
- liquidity,
- mileage gradient,
- trim/build premiums,
- condition sensitivity,
- demand,
- and unusual outliers.

Do NOT average all_sales indiscriminately.

The broader the comparison set, the more its purpose shifts from direct valuation
anchor toward market behavior/context.


================================================================
EVIDENCE RULES
================================================================

- Consider both current adjusted MMR and AccuTrade Target Auction before choosing the
    baseline. Use the one better supported for this exact vehicle.
- Use GMTV historical sales as first-party empirical evidence.
- Compare mileage/build and infer mileage/history effects only when supplied rows
  actually support them.
- Use JD Power as build/spec/value context, not as a substitute for auction evidence.
- Read the AccuTrade screenshot plus the complete extracted CARFAX and AutoCheck report
    text when present, and reconcile the two vehicle-history reports.
- On an AccuTrade screenshot, use Bring Money as the wholesale auction-value reference.
    Give Me Money is a wholesaler's direct purchase offer, not an auction hammer price or
    auction comparable; never use it anywhere in the valuation.
- The baseline rung must name Manheim adjusted MMR or AccuTrade Target Auction, state
    both anchor amounts, and explain why the selected source is stronger. Only when both
    are unavailable may it name `Verified public-auction sold fallback`; then cite at
    least two original SOLD-result URLs from two independent auction domains and explain
    the dealer-margin translation from public hammer to wholesale baseline.
- Read the attached CRM vehicle photos from the Powerbuyer lead and weigh what you can
  actually see in them for condition, colour, options and damage.
- AutoCheck is collected with the browser evidence. Treat a returned AutoCheck report
    as VHR evidence and explicitly reconcile conflicts between AutoCheck and CARFAX.
- Use Pass-1 public evidence to constrain and contextualize the range, especially
  same-VIN evidence.
- For every proposed adjustment, use Pass 1's ADJUSTMENT CLAIM EVIDENCE MATRIX. Public
        research supports a rung only when it compares a priced normal vehicle against a priced
        claimed-feature vehicle and gives both exact grounded URLs, mileage, trim/build, status,
        material differences, and signed observed difference. Include successful citations in
        the rung evidence; otherwise use $0.
- Exception for source-confirmed material impairments: read the rung's structured
    `dealer_underwriting`. When CARFAX, AutoCheck, Manheim condition evidence, or CRM photos
    confirm that the target actually has the issue, and Pass 1 found no controlled vehicle
    pair, grounded dealer research may establish a conservative negative bid effect by
    quantifying retailability, title/inspection, financing/insurance, buyer-pool, recon,
    liquidity, impaired retail exit, and dealer-margin constraints. Cite the private source
    confirming the target fact plus `[PUBLIC_UNDERWRITING: exact grounded URL]` for every
    external finding. Never use underwriting for a premium, never let public research invent
    the target condition, and prefer any valid GMTV, Manheim, or public vehicle pair.
- Distinguish FACT from INFERENCE.
- Explicitly identify conflicts between sources.
- Never fabricate missing evidence.
- Reject misleading comps rather than averaging them into the answer.


================================================================
CONDITION AND RECON
================================================================

Separate COSMETIC PRESENTATION from MECHANICAL RISK.

A mechanically imperfect vehicle with unusually good cosmetics can still be attractive
because the dealer may only need to solve one concentrated problem.

Likewise, a mechanically healthy vehicle with widespread cosmetic damage can be
unattractive because recon involves many individual costs, time, coordination, and
retail presentation risk.

When assessing condition ask:

- Is the issue localized or systemic?
- Is the cost predictable?
- Does the exterior/interior otherwise present retail-ready?
- Can the dealer repair the issue efficiently?
- Does the likely buyer pool specialize in this kind of vehicle?
- Will the problem materially reduce the retail customer pool?
- Does the retail spread compensate for it?

Do not treat every dollar of retail recon as a dollar-for-dollar reduction in hammer.
A dealer's internal repair economics may differ from consumer retail repair estimates.


================================================================
MARKET HEAT
================================================================

Always cross-check adjustments against market heat.

Market heat represents how aggressively the wholesale buyer pool is likely to compete
for the opportunity.

High heat can:
- push the hammer toward the upper end of the defensible range,
- cause buyers to accept thinner margins,
- cause buyers to tolerate more condition risk,
- increase the amount of visible retail upside captured by the seller.

Low heat can:
- leave apparently attractive spread unbid,
- amplify uncertainty discounts,
- force unusual vehicles below theoretical value,
- increase the margin buyers demand.

Market heat may influence where the car trades within a defensible economic range.

It may NOT create unsupported value.

Use `gmtv.market_trend` explicitly. Distinguish MMR's own chronological direction from
the current `market_vs_mmr_level` and the `sale_to_mmr_gap_direction` (GMTV sold price
minus date-correct historical MMR). State whether GMTV remains above or below MMR before
describing that premium or discount as widening or shrinking. If recent sales exceed
historical MMR, MMR may be lagging low even when that premium is shrinking. If recent
sales trail historical MMR, MMR may be lagging high even when that discount is shrinking.
Do not call either condition from fewer than four usable points; lower confidence when
coverage is incomplete or the model/mileage mix makes the signal contradictory.

Assess likely auction bidding intensity as `heavy`, `lukewarm`, or `light` using:
- inventory saturation and listing volume,
- sell-through, mobility, days on market, and price cuts,
- the separate expected 21-28 day and 30-90 day directions,
- the market's movement around MMR and possible MMR lag,
- how many dealers can retail the vehicle,
- recon/risk burden,
- and expected dealer margin remaining at the proposed hammer.

Use `gmtv.lane_position_heat` as first-party evidence of when comparable inventory
usually sells in the GMTV lane. `RunNumber` is composite lane-position data: for example,
`24-0141` means lane 24, position 141. Ignore `ExoticLane`. Interpret positions 0-40 as
medium/normal heat, positions 41-99 as the lower-heat middle, and triple-digit positions
100-300 as high heat near the end, when more bidders are typically online. Use
`average_position`, not `median_position`, as the vehicle's aggregate in-lane heat
signal. Report the usable comp count, average position, and band counts. Lane position
is only a directional nudge after all price adjustments and the defensible range are
independently established. It may influence `bidding_intensity` and where the hammer
lands inside that range, but it may not create a standalone ladder delta, determine a
dollar amount, serve as a priced comparison pair, widen the range, replace sold-price
evidence, or satisfy any minor/major market-correction threshold. If lane heat conflicts
with trend or web evidence, disclose the conflict, lower confidence, and do not use it to
move the hammer.

Produce two explicit forecasts before setting bidding intensity:
- `NEAR TERM 21-28 DAYS`: direction (`up`, `flat`, `down`, or `uncertain`), confidence,
  dated numerical evidence, inventory/velocity signal, pricing signal, and implication.
- `MEDIUM TERM 30-90 DAYS`: the same fields, independently supported.
Then reconcile the horizons with current/future MMR, `gmtv.market_trend`, average lane
position, and extensive grounded web research. Each horizon must state what each of those
three evidence families contributes, including missing or conflicting signals. Forecasts
are directional risk inputs, not independent valuation adjustments. Any nudge must remain
inside the independently supported range and pass the existing comparison and
anti-overcorrection rules.

Heavy bidding moves the hammer only toward the high end of an independently supported
wholesale range. Light bidding moves it toward the low end. Bidding intensity may not
create value outside the evidence-supported range.


================================================================
VISUAL DESIRABILITY / COLOR / "SEXINESS"
================================================================

Use Pass-1 public research regarding color, appearance, configuration, and observed
market desirability.

Do not add money because a vehicle subjectively "looks good."

Instead determine whether the target specification appears to create:
- a larger buyer pool,
- stronger retail pricing,
- lower days-to-sale,
- unusual scarcity,
- or materially better retail presentation.

If public evidence shows no meaningful market effect, use $0.

If visual desirability supports an adjustment, make sure the amount remains consistent
with the overall retail spread and market heat.

Ask explicitly whether buyers in THIS vehicle segment care about each visible factor:

- `aftermarket`: lifted suspension, wheels/tires, body kits, wraps, accessories, and
    other modifications. Reward modifications only when close market evidence shows a
    broader/stronger buyer pool or higher pricing. Penalize gaudy, polarizing, poorly
    executed, or over-personalized modifications when close evidence shows weaker pricing
    or demand. Stock is not automatically better and modified is not automatically better.
- `condition`: dents, scratches, paint mismatch, body damage, worn cosmetics, or an
    overall beat-up presentation. Separate documented repair/recon cost from market appeal.
    A sexiness discount is justified only when close clean-versus-cosmetically-poor public
    examples show that the market pays less beyond ordinary listing noise.
- `color`: retain the strict evidence-only rule below; do not weaken it.

Place each effect only on its matching ladder rung. Do not put modifications or damage on
the color rung, and do not count the same observed retail spread under condition,
aftermarket, market_heat, and retail_ceiling.

For a non-zero color adjustment, cite close retail listings using
`[PUBLIC_COMP: exact grounded URL]`. Each cited fact must include the listing's price,
mileage, trim/build, and ACTIVE or SOLD status. Do not infer a color premium or discount
when those comparable facts do not establish one.


================================================================
VALUE LADDER REQUIREMENT
================================================================

Give an explicit ladder in this FIXED order:

1. baseline
2. mileage
3. history
4. condition
5. factory_equipment
6. aftermarket
7. color
8. market_heat
9. retail_ceiling

For EVERY rung provide:

- delta in dollars,
- resulting running value,
- method,
- evidence,
- note.

A non-zero delta MUST be supported by specific evidence.

If evidence does not justify an adjustment, use:

delta: $0

and explain why no evidence-supported adjustment was applied.

For every non-zero delta, evidence must contain at least one source-tagged citation in
this exact form: `[SOURCE] factual supporting detail` or
`[PUBLIC_COMP: exact grounded URL] factual supporting detail`.

Allowed SOURCE tags and their meaning:
- `[MANHEIM]` for collected MMR, condition reports, or Manheim transaction data.
- `[GMTV]` for collected 3monthsales.csv data.
- `[ACCUTRADE]` for the attached AccuTrade appraisal screenshot.
- `[CARFAX]` for the attached CARFAX report.
- `[AUTOCHECK]` for the collected structured AutoCheck report.
- `[JDPOWER]` for collected JD Power build/spec data.
- `[CRM_PHOTO]` for attached CRM vehicle photos/Ava photo observations.
- `[PUBLIC_COMP: exact grounded URL]` for pass-1 grounded public-market research.
- `[PUBLIC_UNDERWRITING: exact grounded URL]` for pass-1 grounded dealer due diligence
    supporting a source-confirmed impairment when no controlled vehicle pair exists.

Do not use a tag unless that source actually supplied the stated fact. Do not use vague
evidence such as "market data" or "research." If no valid source citation exists, use $0.
For `[PUBLIC_COMP]`, the URL must exactly match a grounded URL supplied by Pass 1.

GMTV COMPARISON RULE:
`same_year_make_model_vhr_net_profit` analyzes the entire 3monthsales.csv across the target's
exact model year, make, and model. When the target's CARFAX
or other collected VHR confirms bad history and `sufficient_sample` is true, apply
`recommended_bad_vhr_deduction` as the history-rung decrease. Cite `[GMTV]` with the
year/make/model scope, bad and clean sample counts, both average `NetAmount` values, and
`clean_minus_bad_average`. This empirical net-profit gap is the controlled method for
the VHR deduction even though models differ. Do not apply it to a target without
confirmed bad VHR, when either group has no unique VINs, or when the recommended
deduction is zero. If bad-history vehicles performed worse and the target has confirmed
bad history, the deduction is mandatory rather than discretionary. If bad-history
vehicles did not perform worse, discard the history adjustment. Always disclose both
sample counts; a one-versus-one result is valid but thin evidence. Missing flags remain
unknown, never clean.

Do not double-count.

Examples:
- If mileage is already incorporated into adjusted MMR, do not deduct it twice.
- If MMR already reflects a trim/package, do not add the full package premium again.
- If condition is already represented in exact transaction evidence, account for that
  relationship before making another full condition adjustment.
- If retail upside influenced market_heat, do not independently add the same entire
  premium again under retail_ceiling.


================================================================
HOW TO USE THE RETAIL_CEILING RUNG
================================================================

The final retail_ceiling rung is NOT automatically a negative adjustment.

It is the final dealer-economics sanity check.

Ask:

"At the current running wholesale value, does a capable dealer still have enough
risk-adjusted retail upside to raise their hand?"

Possible outcomes:

NEGATIVE DELTA:
The proposed wholesale value leaves insufficient room relative to credible retail exit,
recon, risk, and margin.

$0 DELTA:
The current running value already makes sense within retail economics.

POSITIVE DELTA:
Allowed ONLY when strong evidence shows the prior wholesale anchors materially
understate what competitive dealers would likely pay because the retail opportunity
is unusually large and obvious to the lane.

A positive retail-ceiling adjustment should generally be conservative.

Do not transfer the entire retail spread into wholesale value.


================================================================
FINAL BIDDER CHECK
================================================================

Before finalizing, mentally place the vehicle in the lane at the proposed TARGET HAMMER.

Ask:

1. Would multiple competent dealers plausibly still bid here?
2. What opportunity are they buying?
3. What risk are they accepting?
4. What expected margin remains?
5. Would the risk cause bidding to stop earlier?
6. Would the visible upside cause bidding to continue past MMR?
7. Does market heat support that behavior?
8. Is the final result consistent with actual transaction evidence?
9. Is one piece of evidence being double-counted?
10. Would I actually raise my hand one more time at this price?

If the answer to #10 is clearly no, the target is too high.

If the answer is clearly yes for several rational buyers, the target may be too low.


================================================================
OUTPUT
================================================================

Return a detailed valuation memo containing:

- exactly ONE TARGET HAMMER,
- realistic LOW/HIGH likely hammer range,
- confidence,
- market heat,
- evidence volume: accepted observation count, comparable count, independent source
    count, recency, contradictions, and missing evidence. Confidence MUST rise primarily
    with the volume of accepted, recent, genuinely comparable, independently sourced
    evidence and fall for thin samples, stale evidence, weak comparability, collector
    failures, and contradictions,
- market trend, including MMR direction, movement around MMR, and possible MMR accuracy
    slippage,
- separate 21-28 day and 30-90 day forecasts, each with direction, confidence, dated
    numerical evidence, inventory signal, pricing signal, and implications, plus a
    reconciliation of agreement or conflict between the horizons,
- bidding intensity (`heavy`, `lukewarm`, or `light`) and its saturation, mobility,
    stagnation, heat, and dealer-margin basis,
- identity/listing matches in a separate summary-only section explicitly excluded from
    valuation math,
- key anchors,
- exact-VIN evidence,
- accepted Manheim comps,
- excluded Manheim comps and why,
- GMTV evidence,
- condition/history findings,
- retail opportunity,
- primary risks,
- dealer economic/bidding logic,
- and the complete nine-rung value ladder.

Mention Canada or Canadian history anywhere in this memo only when the history rung applies
a non-zero Canadian policy adjustment. Otherwise omit the check, negative finding, missing
evidence, comparable announcements, and all Canadian references.

Explicitly state the central dealer thesis in 2-4 sentences:

"Why would a rational dealer pay this amount for this vehicle?"

The final TARGET HAMMER MUST exactly reconcile to the final running value of the ladder.

Pass 3 will map this memo into the API schema without re-valuing it.
""".strip()

    contents: list[Any] = [types.Part.from_text(text=prompt)]
    contents.extend(_artifact_parts(evidence))
    contents.extend(await _crm_photo_parts(evidence))

    def do_value():
        return client.models.generate_content(
            model=VALUATION_MODEL,
            contents=contents,
            config=types.GenerateContentConfig(
                temperature=0.15,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )

    response = await asyncio.wait_for(asyncio.to_thread(do_value), timeout=VALUATION_TIMEOUT)
    return str(getattr(response, "text", "") or "")


ADJUSTMENT_AUDIT_SCHEMA = {
        "type": "OBJECT",
        "properties": {
                "audited_memo": {"type": "STRING"},
                "adjustments": {
                        "type": "ARRAY",
                        "items": {
                                "type": "OBJECT",
                                "properties": {
                                        "stage_id": {"type": "STRING"},
                                        "proposed_delta": {"type": "NUMBER"},
                                        "audited_delta": {"type": "NUMBER"},
                                        "decision": {"type": "STRING"},
                                        "evidence": {"type": "ARRAY", "items": {"type": "STRING"}},
                                        "retail_corroborated": {"type": "BOOLEAN"},
                                        "market_supported_wholesale_value": {"type": "NUMBER"},
                                        "past_sales_assessment": {"type": "STRING"},
                                        "lane_position_assessment": {"type": "STRING"},
                                        "market_trend_reasoning": {"type": "STRING"},
                                        "comparison_pairs": {
                                            "type": "ARRAY",
                                            "items": {
                                                "type": "OBJECT",
                                                "properties": {
                                                    "source_type": {"type": "STRING"},
                                                    "without_feature_reference": {"type": "STRING"},
                                                    "with_feature_reference": {"type": "STRING"},
                                                    "without_price": {"type": "NUMBER"},
                                                    "with_price": {"type": "NUMBER"},
                                                    "observed_difference": {"type": "NUMBER"},
                                                    "same_build_explanation": {"type": "STRING"},
                                                },
                                                "required": [
                                                    "source_type", "without_feature_reference",
                                                    "with_feature_reference", "without_price",
                                                    "with_price", "observed_difference",
                                                    "same_build_explanation",
                                                ],
                                            },
                                        },
                                        "reason": {"type": "STRING"},
                                },
                                "required": [
                                        "stage_id", "proposed_delta", "audited_delta", "decision",
                                    "evidence", "retail_corroborated",
                                    "market_supported_wholesale_value",
                                    "past_sales_assessment", "lane_position_assessment",
                                    "market_trend_reasoning",
                                    "comparison_pairs", "reason",
                                ],
                        },
                },
        },
        "required": ["audited_memo", "adjustments"],
}


async def _audit_adjustments(reasoning: str, dossier: dict) -> dict[str, Any]:
    """Audit adjustments and return the final API structure in one model call."""
    client = _global_vertex_client()
    private_dossier = {
        key: value for key, value in dossier.items() if key != "public_research"
    }
    prompt = f"""
You are the evidence auditor for a GMTV wholesale valuation. You do not create new
evidence and you do not search the web. Review the proposed valuation memo against only
the collector payload and grounded public sources below.

For each non-zero value-ladder adjustment, decide whether collected evidence confirms,
requires a reduction of, or denies it. The adjustment is allowed only when its cited
facts actually support the direction and a reasonable wholesale magnitude.

AUDIT RULES
- Every non-zero claim must be checked against applicable GMTV and Manheim sold comps.
    Compare only close vehicles and account for year, trim/build, mileage, condition, and
    history. If those sales conflict with the claim, reduce or deny the adjustment.
- Review every object in `public_research.adjustment_research.by_stage` for each proposed
    adjustment. Public research supports a claim only when its structured object has a
    SUPPORTED verdict, successful grounding verification, a priced normal vehicle and a
    priced claimed-feature vehicle, both exact grounded URLs, mileage, trim/build, status,
    material differences, and a signed observed difference. Copy successful comparisons
    into `comparison_pairs` and retain their exact `[PUBLIC_COMP: URL]` citations.
- Read `public_research.adjustment_research.missing_after_retry`. For any listed category,
    do not claim that public research supports the adjustment, do not create a public
    comparison pair, list the missing investigation in confidence.missing_evidence, and
    reduce confidence. A non-zero adjustment may survive only when independent first-party
    evidence satisfies that rung's existing rules without relying on public research.
- A wider year-window result is only usable after Pass 1 classifies the years as the same
    basic generation/build. Discard redesigns, different chassis/powertrain families, or
    materially different equipment even when make/model names match.
- Color: require close public retail listings and, when available, close GMTV/Manheim sold
    comps that establish a meaningful price or exit difference after controlling for the
    other vehicle differences. A color name or subjective desirability is not support.
- Aftermarket market appeal: require grounded close modified-versus-stock public
    comparisons for the target's actual modification. Confirm a premium only when this
    segment demonstrably rewards it; confirm a discount when the modification is
    polarizing or weakens demand. Do not price parts or installation cost as value.
- Cosmetic sexiness: keep documented repair/recon facts separate from market stigma.
    Any additional dent/damage/beat-up-presentation adjustment requires grounded close
    clean-versus-cosmetically-poor comparisons showing a meaningful market difference.
    Do not infer that every used vehicle segment cares equally about cosmetics.
- History: when collected target evidence confirms bad VHR, use GMTV's
    `same_year_make_model_vhr_net_profit.recommended_bad_vhr_deduction` when its sample is
    sufficient. Audit against the exact year/make/model clean-versus-bad average `NetAmount`
    gap. When
    bad-history performance is worse, retain this deduction as mandatory. When it is not
    worse or either group is absent, the history adjustment must be $0.
- Canadian history is a required policy adjustment only when CARFAX or AutoCheck explicitly
    confirms Canadian import/history. When neither report is available, an exact-target-VIN
    grounded public match may confirm it. Manheim, GMTV, AccuTrade, JD Power, generic public
    research, and different-VIN results can never establish Canadian history. Do not deny the adjustment solely because
    GMTV or Manheim comps lack a Canadian-origin field. Apply the configured 15-20%
    Canadian wholesale discount conservatively, use close sales to calibrate it when
    available, and cite `[CARFAX]`, `[AUTOCHECK]`, or `[PUBLIC_COMP: exact grounded URL]` plus
    `[CANADIAN_POLICY]` in the audit evidence.
- Condition and recon: do not ignore a documented repair, damage, or recon issue merely
    because there is no exact matching sale. CARFAX, AutoCheck, AccuTrade, CRM photos, or Manheim
    condition evidence can establish that the issue exists. Then use applicable GMTV,
    Manheim, and public comps to test the magnitude of its auction impact. When no suitable
    comp can isolate the issue, retain only a conservative, clearly documented adjustment;
    do not invent a repair cost or treat a generic retail ask as proof.
- Dealer underwriting without a vehicle pair: inspect the rung's structured
    `dealer_underwriting`. Public sources may quantify a negative effect only when a private
    collector proves the target fact, at least two independent grounded domains address the
    dealer's actual exit/risk questions, and the research supplies coherent clean-retail,
    impaired-retail-exit, and signed wholesale ranges with limitations. Prefer government or
    primary sources for title/legal requirements and current market sources for monetary
    effects. Generic percentages, a single source, or public speculation that the target has
    the issue are insufficient. A valid GMTV, Manheim, or public vehicle pair takes precedence.
    This path can only reduce value; it cannot create a premium.
- Equipment: require the equipment to be documented by AccuTrade, JD Power, photos, or
    other collected evidence, then check applicable GMTV/Manheim/public comps for evidence
    that it changes the hammer.
- Retail ceiling and market heat: require actual retail/public or transaction evidence;
    never transfer an unsupported retail ask directly into wholesale hammer value.
- Forecasts: audit the 21-28 day and 30-90 day forecasts separately against dated evidence.
    Preserve disagreements between horizons and reduce confidence when evidence is stale,
    thin, contradictory, or merely repeated across sources. Neither forecast creates an
    adjustment by itself.
- Lane position: treat `average_position` only as a directional nudge for bidding intensity
    and placement inside an independently supported range. It must never establish a dollar
    magnitude, serve as a comparison pair, create a standalone adjustment, widen the range,
    or satisfy minor/major correction evidence requirements. The market-heat amount must
    remain fully supportable after all lane/run-position evidence is removed.
- Historical market trend: verify every claimed direction against
    `gmtv.market_trend.graph_points`. Preserve the distinction among MMR direction, current
    `market_vs_mmr_level`, and `sale_to_mmr_gap_direction`. Always state whether GMTV is
    above or below MMR before noting that the premium or discount is widening or shrinking.
    Treat `lagging_low`/`lagging_high` as an accuracy-risk warning, not a trim-specific
    percentage that transfers directly to the target. The aggregate shift proves market
    direction and strength only. Thin trend evidence reduces confidence.
- Apply the JD Power GMTV hierarchy in `market_trend.relevance_tiers`: exact trim/build
    first, then same model, then decoded same segment/body type, then same-make background.
    Higher tiers control target pricing when populated. Lower tiers may establish GMTV-wide
    direction but must be shrunk toward zero and corroborated by target-relevant retail and
    past sales. Never promote a row by guessing attributes absent from its JD Power decode.
- For `market_heat`, reason explicitly about whether relevant grounded retail asks and the
    target's likely retail exit corroborate a modest wholesale correction. Consider trim and
    configuration relevance, mileage, condition/history, dealer margin, and whether the
    retail evidence is active and current. Set `retail_corroborated` true only when that
    evidence leaves credible room above the selected baseline after normal dealer economics.
- For `market_heat`, do not convert the aggregate sale-to-MMR percentage into a fixed
    percentage bump. Instead estimate `market_supported_wholesale_value`: the furthest
    reasonable hammer supported after triangulating (A) aggregate MMR lag direction and
    strength, (B) relevant current retail pricing, velocity/stagnation, and sustainable
    dealer margin, and (C) relevant GMTV/Manheim past sales. Explain that reconciliation in
    `market_trend_reasoning` and separately summarize the past-sales constraint in
    `past_sales_assessment`.
- Use lane `average_position` only after that independently supported wholesale range exists.
    In `lane_position_assessment`, explain whether late/early placement makes the upper,
    middle, or lower part of that range more likely. Lane placement may select within the
    range but may not expand it, create dollars, or override retail/past-sales constraints.
- Set `retail_corroborated` true only when grounded retail evidence supports the direction
    and leaves credible dealer economics at the audited hammer. If retail contradicts the
    trend, reduce or deny the adjustment. For non-market rungs set
    `market_supported_wholesale_value` to 0, false for retail corroboration, and the three
    assessment/reasoning strings to empty strings.
- Treat a combined `market_heat` plus `retail_ceiling` movement beyond 3% of the
    selected baseline as a MAJOR market correction. Confirm one only when at least six
    date-matched trend points with at least 70% lookup coverage show a meaningful shift
    in the same direction, three independent dated web-source domains corroborate it,
    at least two grounded public sources are cited on the ladder, and a priced retail
    comparison pair shows that dealer margin still supports the result. Otherwise keep
    the combined movement within 3% (minimum $500). Even fully supported major market
    corrections may not exceed 10% of baseline.
- A reasoned `market_heat` correction may proceed without a trim-specific controlled pair
    only when the strong aggregate trend establishes direction, `retail_corroborated` is
    true, `market_supported_wholesale_value` bounds the audited hammer in the same direction,
    all three reasoning fields explain retail margin, past sales, and lane placement, and
    audit evidence cites both `[GMTV]` and at least one exact grounded
    `[PUBLIC_COMP: URL]`. The normal market-correction guard still limits weakly corroborated
    adjustments; support for a larger correction must satisfy its stronger retail and
    comparison requirements.
- Identity matches found by exact VIN, email, or phone are summary-only and must never
    support or preserve a valuation adjustment, range, anchor, or confidence increase.
- Retail direction may lead wholesale by roughly one month only when dated public
    evidence supports the sequence. Retail remains a ceiling/directional check, never a
    direct wholesale comp.
- For every non-zero adjustment other than the explicit Canadian policy adjustment, the
    sufficient-sample GMTV same-year/make/model VHR aggregate, and the qualified reasoned
    `market_heat` correction above, or validated negative dealer underwriting for a
    collector-confirmed impairment, require at least one controlled comparison pair with
    both prices and an observed difference. A
    normal/average listing must be compared against a close listing that has the claimed
    feature, holding the rest of the build as constant as possible.
- Keep sexiness effects on their proper rung: cosmetic damage/presentation on `condition`,
    modifications on `aftermarket`, and color alone on `color`. Reject duplicated effects
    across those rungs or again under market heat/retail ceiling.
- Prefer comparison pairs from GMTV sold transactions over Manheim pairs, and both over
    public retail asks. If a GMTV pair exists, its observed difference controls the
    audited amount; public research cannot override it.
- The audited delta must be within $500 of the selected observed difference. If the
    proposed amount is too high or too low, change audited_delta toward that observed
    difference and mark the decision `reduced` or `confirmed`. If no valid pair exists,
    set audited_delta to 0, except for the documented Canadian policy discount and the
    qualified reasoned `market_heat` correction or validated negative dealer underwriting.
- `[PUBLIC_COMP: URL]` citations must use an exact URL from GROUNDED PUBLIC SOURCES.
- `[PUBLIC_UNDERWRITING: URL]` citations must use exact grounded URLs and accompany the
    private-source citation that confirms the target impairment.
- `[MANHEIM]`, `[GMTV]`, `[ACCUTRADE]`, `[CARFAX]`, `[AUTOCHECK]`, `[JDPOWER]`, and `[CRM_PHOTO]` may
    cite only collectors that returned data.
- When support is absent, contradictory, or too weak to isolate the claimed effect, set
    audited_delta to 0. When the direction is supported but magnitude is too aggressive,
    reduce it conservatively.

Return JSON containing:
1. `adjustments`: one audit result for each non-baseline ladder rung, with proposed and
    audited deltas, decision (`confirmed`, `reduced`, `denied`, or `no_adjustment`),
    source-tagged evidence, the market-support and reasoning fields, `comparison_pairs`,
    and a concise reason. Each pair must
    identify the normal listing/transaction and the feature listing/transaction, with
    prices and the signed observed difference (with_feature_price minus
    without_feature_price).
2. `final.carfax`: a concise AI review of the complete extracted CARFAX text. Set
    `status` to `clean`, `adverse`, `mixed`, `unknown`, or `unavailable`, and write a
    single short sentence in `summary` stating the decision-driving history facts. Do
    not reproduce service-history rows or report prose. Use `unavailable` only when no
    extracted CARFAX text exists, and `unknown` when the text exists but is inconclusive.
3. `final`: the complete audited valuation mapped directly into the required API schema.
    Do not return an intermediate memo. Its target hammer and every ladder running value
    must reconcile using audited_delta values only. Every non-zero rung must retain valid
    source-tagged evidence. Denied adjustments must be $0. Use the fixed stage IDs exactly:
    {', '.join(STAGE_ORDER)}. Baseline delta is 0; each later running value equals the prior
    running value plus its audited delta; valuation.amount equals the final running value.
    Preserve both forecast horizons, evidence-volume confidence, bidding intensity, identity
    matches excluded from valuation, accepted/excluded public comps, and the user-facing
    explanation in `text`. Mention Canada or Canadian history only when the audited history
    rung applies a non-zero Canadian policy adjustment.

PROPOSED VALUATION MEMO:
{reasoning}

PRIVATE EVIDENCE DOSSIER:
{_compact(private_dossier, max_chars=100_000)}

STRUCTURED PUBLIC RESEARCH (ADJUSTMENTS AND UNDERWRITING FIRST):
{_compact(dossier.get('public_research') or {}, max_chars=100_000)}
""".strip()

    def do_audit():
        return client.models.generate_content(
            model=VALUATION_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                response_mime_type="application/json",
                response_schema=AUDITED_FINAL_SCHEMA,
                automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
            ),
        )

    response = await asyncio.wait_for(asyncio.to_thread(do_audit), timeout=AUDIT_TIMEOUT)
    result = json.loads(str(getattr(response, "text", "") or "").strip())
    if not isinstance(result.get("final"), dict):
        raise ValueError("Adjustment audit returned no structured final valuation")
    return result


FINAL_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "valuation": {
            "type": "OBJECT",
            "properties": {
                "amount": {"type": "NUMBER"},
                "source": {"type": "STRING"},
            },
            "required": ["amount", "source"],
        },
        "likely_range": {
            "type": "OBJECT",
            "properties": {
                "low": {"type": "NUMBER"},
                "high": {"type": "NUMBER"},
            },
            "required": ["low", "high"],
        },
        "market_forecast": {
            "type": "OBJECT",
            "properties": {
                "near_term_21_28_days": {
                    "type": "OBJECT",
                    "properties": {
                        "direction": {"type": "STRING", "enum": ["up", "flat", "down", "uncertain"]},
                        "confidence": {"type": "INTEGER"},
                        "evidence_summary": {"type": "STRING"},
                        "inventory_signal": {"type": "STRING"},
                        "pricing_signal": {"type": "STRING"},
                        "implication": {"type": "STRING"},
                    },
                    "required": [
                        "direction", "confidence", "evidence_summary", "inventory_signal",
                        "pricing_signal", "implication"
                    ],
                },
                "medium_term_30_90_days": {
                    "type": "OBJECT",
                    "properties": {
                        "direction": {"type": "STRING", "enum": ["up", "flat", "down", "uncertain"]},
                        "confidence": {"type": "INTEGER"},
                        "evidence_summary": {"type": "STRING"},
                        "inventory_signal": {"type": "STRING"},
                        "pricing_signal": {"type": "STRING"},
                        "implication": {"type": "STRING"},
                    },
                    "required": [
                        "direction", "confidence", "evidence_summary", "inventory_signal",
                        "pricing_signal", "implication"
                    ],
                },
                "reconciliation": {"type": "STRING"},
            },
            "required": ["near_term_21_28_days", "medium_term_30_90_days", "reconciliation"],
        },
        "confidence": {
            "type": "OBJECT",
            "properties": {
                "score": {"type": "INTEGER"},
                "level": {"type": "STRING"},
                "missing_evidence": {"type": "ARRAY", "items": {"type": "STRING"}},
                "accepted_evidence_count": {"type": "INTEGER"},
                "independent_source_count": {"type": "INTEGER"},
                "contradiction_count": {"type": "INTEGER"},
                "basis": {"type": "STRING"},
            },
            "required": [
                "score", "level", "missing_evidence", "accepted_evidence_count",
                "independent_source_count", "contradiction_count", "basis"
            ],
        },
        "heat": {
            "type": "OBJECT",
            "properties": {
                "level": {"type": "STRING"},
                "reason": {"type": "STRING"},
            },
            "required": ["level", "reason"],
        },
        "bidding_intensity": {
            "type": "OBJECT",
            "properties": {
                "level": {"type": "STRING", "enum": ["heavy", "lukewarm", "light"]},
                "reason": {"type": "STRING"},
                "inventory_saturation": {"type": "STRING"},
                "mobility": {"type": "STRING"},
                "dealer_margin": {"type": "STRING"},
            },
            "required": ["level", "reason", "inventory_saturation", "mobility", "dealer_margin"],
        },
        "carfax": {
            "type": "OBJECT",
            "properties": {
                "status": {
                    "type": "STRING",
                    "enum": ["clean", "adverse", "mixed", "unknown", "unavailable"],
                },
                "summary": {"type": "STRING"},
            },
            "required": ["status", "summary"],
        },
        "identity_listing_matches": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "matched_by": {"type": "STRING"},
                    "url": {"type": "STRING"},
                    "status": {"type": "STRING"},
                    "summary": {"type": "STRING"},
                    "excluded_from_valuation": {"type": "BOOLEAN"},
                },
                "required": ["matched_by", "url", "status", "summary", "excluded_from_valuation"],
            },
        },
        "anchors": {
            "type": "OBJECT",
            "properties": {
                "mmr": {"type": "NUMBER", "nullable": True},
                "accutrade": {"type": "NUMBER", "nullable": True},
                "same_vin_public_ask": {"type": "NUMBER", "nullable": True},
            },
        },
        "text": {"type": "STRING"},
        "logic": {"type": "ARRAY", "items": {"type": "STRING"}},
        "accepted_public_comps": {"type": "ARRAY", "items": {"type": "STRING"}},
        "excluded_public_comps": {"type": "ARRAY", "items": {"type": "STRING"}},
        "value_ladder": {
            "type": "ARRAY",
            "items": {
                "type": "OBJECT",
                "properties": {
                    "stage_id": {"type": "STRING"},
                    "label": {"type": "STRING"},
                    "delta": {"type": "NUMBER"},
                    "running_value": {"type": "NUMBER"},
                    "method": {"type": "STRING"},
                    "note": {"type": "STRING"},
                    "evidence": {"type": "ARRAY", "items": {"type": "STRING"}},
                },
                "required": [
                    "stage_id", "label", "delta", "running_value",
                    "method", "note", "evidence"
                ],
            },
        },
    },
    "required": [
        "valuation",
        "likely_range",
        "market_forecast",
        "confidence",
        "heat",
        "bidding_intensity",
        "carfax",
        "identity_listing_matches",
        "anchors",
        "text",
        "logic",
        "value_ladder",
    ],
}


AUDITED_FINAL_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "final": FINAL_SCHEMA,
        "adjustments": ADJUSTMENT_AUDIT_SCHEMA["properties"]["adjustments"],
    },
    "required": ["final", "adjustments"],
}


# ---------------------------------------------------------------------------
# Ladder / card rendering
# ---------------------------------------------------------------------------


def _deep_get(obj: Any, path: tuple[str, ...]) -> Any:
    current = obj
    for key in path:
        if not isinstance(current, dict):
            return None
        if key in current:
            current = current[key]
            continue
        lowered = {str(k).lower().replace("-", "_"): v for k, v in current.items()}
        normalized = key.lower().replace("-", "_")
        if normalized not in lowered:
            return None
        current = lowered[normalized]
    return current


def _number(value: Any) -> Optional[float]:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except Exception:
        return None


def _round_to_nearest_500(value: float) -> float:
    """Round positive valuation amounts half-up to the nearest $500."""
    return float(int((float(value) + 250.0) // 500.0) * 500)


def _publish_final_ladder(
    final: dict[str, Any],
    rows: list[dict[str, Any]],
    ladder_value: float,
) -> float:
    """Publish rounded valuation output without rewriting an audited adjustment."""
    if ladder_value <= 0:
        raise ValueError(
            f"Audited value ladder produced a non-positive wholesale value: {ladder_value}"
        )
    amount = _round_to_nearest_500(ladder_value)
    if amount <= 0:
        raise ValueError(
            f"Audited value ladder rounded to a non-positive wholesale value: {amount}"
        )
    final.setdefault("valuation", {})["amount"] = amount
    final["final_valuation_number"] = amount
    final["rounding"] = {
        "unrounded_value": ladder_value,
        "rounded_value": amount,
        "delta": round(amount - ladder_value, 2),
        "method": "nearest_500_half_up",
    }
    final.pop("value_ladder", None)
    final["Value_ladder"] = rows
    return amount


def _extract_mmr_payload_value(current: Any) -> tuple[Optional[float], str, str]:
    """Return the best Manheim wholesale value from a current or historical payload."""
    current = current if isinstance(current, dict) else {}

    items = current.get("items") if isinstance(current, dict) else None
    if isinstance(items, list) and items:
        item = next(
            (row for row in items if isinstance(row, dict) and row.get("bestMatch") is True),
            None,
        )
        if item is None:
            item = next((row for row in items if isinstance(row, dict)), None)
        if isinstance(item, dict):
            for path, source in (
                (("adjustedPricing", "wholesale", "average"), "Manheim adjusted MMR"),
                (("adjusted_pricing", "wholesale", "average"), "Manheim adjusted MMR"),
                (("wholesale", "average"), "Manheim base MMR"),
            ):
                value = _number(_deep_get(item, path))
                if value and value > 0:
                    return value, source, "items[]." + ".".join(path)

    # Some wrappers place the publisher result beneath `data`.
    data = current.get("data") if isinstance(current, dict) else None
    if isinstance(data, dict):
        wrapped_items = data.get("items")
        if isinstance(wrapped_items, list) and wrapped_items:
            item = next(
                (row for row in wrapped_items if isinstance(row, dict) and row.get("bestMatch") is True),
                None,
            ) or next((row for row in wrapped_items if isinstance(row, dict)), None)
            if isinstance(item, dict):
                for path, source in (
                    (("adjustedPricing", "wholesale", "average"), "Manheim adjusted MMR"),
                    (("adjusted_pricing", "wholesale", "average"), "Manheim adjusted MMR"),
                    (("wholesale", "average"), "Manheim base MMR"),
                ):
                    value = _number(_deep_get(item, path))
                    if value and value > 0:
                        return value, source, "data.items[]." + ".".join(path)

    # Backward-compatible normalized shapes used by older Andrew/valuation code.
    candidates = [
        (("current", "adjusted_wholesale", "average"), "Manheim adjusted MMR"),
        (("adjusted_wholesale", "average"), "Manheim adjusted MMR"),
        (("current", "base_wholesale", "average"), "Manheim base MMR"),
        (("base_wholesale", "average"), "Manheim base MMR"),
        (("current", "adjustedWholesale", "average"), "Manheim adjusted MMR"),
        (("adjustedWholesale", "average"), "Manheim adjusted MMR"),
        (("current", "baseWholesale", "average"), "Manheim base MMR"),
        (("baseWholesale", "average"), "Manheim base MMR"),
        (("adjustedMmr",), "Manheim adjusted MMR"),
        (("mmr",), "Manheim MMR"),
        (("average",), "Manheim MMR"),
    ]
    for path, source in candidates:
        value = _number(_deep_get(current, path))
        if value and value > 0:
            return value, source, ".".join(path)
    return None, "", ""


def _extract_mmr_baseline(evidence: dict) -> tuple[Optional[float], str, str]:
    """Return the best current Manheim wholesale anchor from every supported shape."""
    current = ((evidence.get("manheim") or {}).get("current_mmr") or {})
    return _extract_mmr_payload_value(current)


def _evidence_ref(line: str, *, kind: str = "mmr_table", vin: str = "", source_url: str = "") -> dict:
    return {
        "kind": kind,
        "line": line,
        "source_url": source_url,
        "vin": vin,
        "with_feature": {},
        "without_feature": {},
    }


LADDER_EVIDENCE_KINDS = {
    "MANHEIM": "mmr_table",
    "GMTV": "listing",
    "ACCUTRADE": "listing",
    "CARFAX": "listing",
    "AUTOCHECK": "listing",
    "JDPOWER": "listing",
    "CRM_PHOTO": "listing",
    "PUBLIC_COMP": "public_comp",
    "PUBLIC_UNDERWRITING": "public_underwriting",
    "CANADIAN_POLICY": "policy",
}

CANADIAN_HISTORY_MIN_DISCOUNT = 0.15
CANADIAN_HISTORY_MAX_DISCOUNT = 0.20


def _available_ladder_evidence_tags(evidence: dict) -> set[str]:
    available = set()
    manheim = evidence.get("manheim") or {}
    if any(
        value and not (isinstance(value, dict) and value.get("error"))
        for key, value in manheim.items()
        if key != "errors"
    ):
        available.add("MANHEIM")

    if (evidence.get("gmtv") or {}).get("sales"):
        available.add("GMTV")

    browser = evidence.get("browser") or {}
    if browser.get("accutrade_screenshot"):
        available.add("ACCUTRADE")
    if browser.get("carfax_pdf"):
        available.add("CARFAX")
    if browser.get("autocheck_pdf") or browser.get("autocheck"):
        available.add("AUTOCHECK")

    if (evidence.get("jdpower") or {}).get("data"):
        available.add("JDPOWER")
    if (evidence.get("crm_photos") or {}).get("images"):
        available.add("CRM_PHOTO")
    if (evidence.get("market_websearch") or {}).get("grounding"):
        available.add("PUBLIC_COMP")
        available.add("PUBLIC_UNDERWRITING")
    return available


def _validated_ladder_evidence(raw_evidence, available_tags: set[str], evidence: dict) -> list[tuple[str, str, str]]:
    public_urls = {
        str(source.get(key) or "").strip()
        for source in (evidence.get("market_websearch") or {}).get("grounding") or []
        if isinstance(source, dict)
        for key in ("url", "grounding_url")
        if str(source.get(key) or "").strip()
    }
    validated = []
    for raw_line in raw_evidence or []:
        line = str(raw_line).strip()
        match = re.match(r"^\[([A-Z_]+)(?:\s*:\s*(https?://\S+))?\]\s+(.+)$", line)
        if not match:
            continue
        tag, source_url, detail = match.groups()
        if tag not in LADDER_EVIDENCE_KINDS or tag not in available_tags:
            continue
        if tag in {"PUBLIC_COMP", "PUBLIC_UNDERWRITING"}:
            source_url = (source_url or "").rstrip(".,)")
            if _canonical_public_url(source_url) not in {
                _canonical_public_url(url) for url in public_urls
            }:
                continue
        elif source_url:
            source_url = source_url.rstrip(".,)")
        validated.append((tag, detail.strip(), source_url or ""))
    return validated


CANADIAN_HISTORY_PATTERN = re.compile(
    r"\b(?:canadian\s+(?:import|origin|history)|import(?:ed)?\s+from\s+canada|"
    r"(?:first\s+)?registered\s+in\s+(?:[a-z]+,?\s+)?canada|canada\s+history)\b",
    re.IGNORECASE,
)


def _confirmed_canadian_history_source(
    validated_evidence: list[tuple[str, str, str]],
    evidence: dict,
    vin: str,
) -> Optional[tuple[str, str, str]]:
    """Return the permitted source that explicitly confirms Canadian history."""
    browser = evidence.get("browser") or {}
    report_tags = set()
    if browser.get("carfax_pdf"):
        report_tags.add("CARFAX")
    if browser.get("autocheck_pdf") or browser.get("autocheck"):
        report_tags.add("AUTOCHECK")

    for tag, detail, source_url in validated_evidence:
        if tag in report_tags and CANADIAN_HISTORY_PATTERN.search(detail):
            return tag, detail, source_url

    if report_tags:
        return None

    target_vin = re.sub(r"[^A-Z0-9]", "", str(vin or "").upper())
    if not target_vin:
        return None

    grounding_by_url = {}
    for source in (evidence.get("market_websearch") or {}).get("grounding") or []:
        if not isinstance(source, dict):
            continue
        for key in ("url", "grounding_url"):
            source_url = str(source.get(key) or "").strip()
            if source_url:
                grounding_by_url[source_url] = source

    for tag, detail, source_url in validated_evidence:
        if tag != "PUBLIC_COMP" or not source_url:
            continue
        source = grounding_by_url.get(source_url) or {}
        source_text = " ".join((
            detail,
            str(source.get("title") or ""),
            str(source.get("reasoning") or ""),
        ))
        normalized_source_text = re.sub(r"[^A-Z0-9]", "", source_text.upper())
        if (
            target_vin in normalized_source_text
            and CANADIAN_HISTORY_PATTERN.search(source_text)
        ):
            return tag, detail, source_url

    return None


def _canadian_history_policy_delta(
    confirmation: Optional[tuple[str, str, str]],
    baseline: float,
    proposed_delta: Optional[float],
) -> Optional[float]:
    """Return a bounded policy deduction for source-qualified Canadian history."""
    if confirmation is None or baseline <= 0:
        return None

    minimum = float(baseline) * CANADIAN_HISTORY_MIN_DISCOUNT
    maximum = float(baseline) * CANADIAN_HISTORY_MAX_DISCOUNT
    proposed_amount = abs(float(proposed_delta or 0.0))
    if proposed_amount == 0:
        proposed_amount = minimum
    return -round(min(max(proposed_amount, minimum), maximum), 2)


def _public_auction_baseline_validation(
    raw_evidence: list[Any],
    evidence: dict,
) -> tuple[bool, list[tuple[str, str, str]], str]:
    validated = _validated_ladder_evidence(raw_evidence, {"PUBLIC_COMP"}, evidence)
    sold_rows = []
    domains = set()
    for row in validated:
        _, detail, source_url = row
        domain = urlparse(source_url).netloc.lower().removeprefix("www.")
        supported_domain = next(
            (
                candidate for candidate in PUBLIC_AUCTION_BASELINE_DOMAINS
                if domain == candidate or domain.endswith("." + candidate)
            ),
            None,
        )
        if not supported_domain:
            continue
        if not re.search(r"\b(sold|hammer|winning bid|sale result)\b", detail, re.IGNORECASE):
            continue
        if re.search(r"\b(active|for sale|current bid|reserve not met|no[- ]sale)\b", detail, re.IGNORECASE):
            continue
        sold_rows.append(row)
        domains.add(supported_domain)

    if len(sold_rows) < 2 or len(domains) < 2:
        return False, sold_rows, (
            "Public-auction fallback requires at least two verified SOLD-result citations "
            "from two independent supported auction domains."
        )
    return True, sold_rows, ""


def _structured_public_research_pair(
    stage_id: str,
    evidence: dict,
) -> tuple[Optional[dict[str, Any]], list[str]]:
    """Build an auditable pair only from verified Pass 1 adjustment research."""
    research = (
        ((evidence.get("market_websearch") or {}).get("adjustment_research") or {})
        .get("by_stage") or {}
    )
    row = research.get(stage_id) if isinstance(research, dict) else None
    if not isinstance(row, dict) or row.get("verdict") != "SUPPORTED":
        return None, []
    if (row.get("grounding_verification") or {}).get("verified") is not True:
        return None, []

    grounding = (evidence.get("market_websearch") or {}).get("grounding") or []
    comparisons = []
    for key in ("normal_comparison", "feature_comparison"):
        comparison = row.get(key)
        if not isinstance(comparison, dict) or comparison.get("found") is not True:
            return None, []
        price = _number(comparison.get("price"))
        mileage = _number(comparison.get("mileage"))
        url = str(comparison.get("url") or "").strip()
        grounded_source_url = _grounded_public_comparison_source(comparison, grounding)
        if (
            price is None or price <= 0
            or mileage is None or mileage < 0
            or not url or not grounded_source_url
            or not str(comparison.get("build") or "").strip()
            or str(comparison.get("listing_status") or "").strip().upper()
            not in {"ACTIVE", "SOLD"}
            or not str(comparison.get("material_differences") or "").strip()
        ):
            return None, []
        comparisons.append((comparison, float(price), grounded_source_url))

    normal, normal_price, normal_url = comparisons[0]
    feature, feature_price, feature_url = comparisons[1]
    observed_difference = feature_price - normal_price
    reported_difference = _number(row.get("observed_difference"))
    if observed_difference == 0 or reported_difference is None:
        return None, []
    if abs(float(reported_difference) - observed_difference) > 1.0:
        return None, []

    pair = {
        "source_type": "public_comp",
        "without_feature_reference": normal_url,
        "with_feature_reference": feature_url,
        "without_price": normal_price,
        "with_price": feature_price,
        "observed_difference": observed_difference,
        "same_build_explanation": (
            f"Normal: {normal.get('build')} at {int(normal.get('mileage') or 0):,} miles, "
            f"{normal.get('listing_status')}; differences: {normal.get('material_differences')}. "
            f"Claimed feature: {feature.get('build')} at "
            f"{int(feature.get('mileage') or 0):,} miles, {feature.get('listing_status')}; "
            f"differences: {feature.get('material_differences')}."
        ),
    }
    citations = [
        f"[PUBLIC_COMP: {normal_url}] Normal comparison at {_money(normal_price)}: "
        f"{normal.get('build')}, {int(normal.get('mileage') or 0):,} miles, "
        f"{normal.get('listing_status')}.",
        f"[PUBLIC_COMP: {feature_url}] Claimed-feature comparison at "
        f"{_money(feature_price)}: {feature.get('build')}, "
        f"{int(feature.get('mileage') or 0):,} miles, {feature.get('listing_status')}."
    ]
    return pair, citations


_BRANDED_TITLE_PATTERNS = {
    "SALVAGE_TITLE": re.compile(r"\bsalvage(?:d)?\s+(?:title|brand)|\btitle\s+(?:is\s+)?salvage", re.IGNORECASE),
    "REBUILT_TITLE": re.compile(r"\b(?:rebuilt|reconstructed)\s+(?:title|brand)", re.IGNORECASE),
    "FLOOD_TITLE": re.compile(r"\b(?:flood|water damage)\s+(?:title|brand)|\btitle\s+(?:is\s+)?flood", re.IGNORECASE),
    "FIRE_TITLE": re.compile(r"\bfire\s+(?:title|brand)|\btitle\s+(?:is\s+)?fire", re.IGNORECASE),
    "LEMON_TITLE": re.compile(r"\b(?:lemon|manufacturer buyback)\s+(?:title|brand|law)", re.IGNORECASE),
    "ODOMETER_BRAND": re.compile(r"\b(?:odometer|mileage)\s+(?:brand|rollback|discrepancy|not actual)", re.IGNORECASE),
    "TOTAL_LOSS": re.compile(r"\btotal(?:ed|\s+loss)\b", re.IGNORECASE),
}


def _grounded_dealer_underwriting(
    stage_id: str,
    evidence: dict,
    vin: str,
    baseline: float,
) -> tuple[Optional[float], list[str], str]:
    """Return a conservative no-pair deduction for a report-confirmed title brand."""
    if stage_id != "history" or baseline <= 0:
        return None, [], ""

    research = (
        ((evidence.get("market_websearch") or {}).get("adjustment_research") or {})
        .get("by_stage") or {}
    )
    row = research.get(stage_id) if isinstance(research, dict) else None
    underwriting = row.get("dealer_underwriting") if isinstance(row, dict) else None
    if not isinstance(underwriting, dict):
        return None, [], ""
    if underwriting.get("verdict") != "SUPPORTED" or underwriting.get("applicable") is not True:
        return None, [], ""

    impairment_type = str(underwriting.get("impairment_type") or "").strip().upper()
    impairment_pattern = _BRANDED_TITLE_PATTERNS.get(impairment_type)
    if impairment_pattern is None:
        return None, [], ""

    source_tag = str(underwriting.get("target_fact_source") or "").strip().upper()
    browser = evidence.get("browser") or {}
    report_text = browser.get("report_text") or {}
    if source_tag == "CARFAX":
        source_available = bool(browser.get("carfax_pdf"))
        source_text = str(report_text.get("carfax") or "")
    elif source_tag == "AUTOCHECK":
        source_available = bool(browser.get("autocheck_pdf") or browser.get("autocheck"))
        source_text = str(report_text.get("autocheck") or "")
    else:
        return None, [], ""

    target_vin = re.sub(r"[^A-Z0-9]", "", str(vin or "").upper())
    normalized_report = re.sub(r"[^A-Z0-9]", "", source_text.upper())
    confirmed_fact = str(underwriting.get("confirmed_target_fact") or "").strip()
    if not all((
        source_available,
        source_text,
        confirmed_fact,
        impairment_pattern.search(source_text),
        not target_vin or target_vin in normalized_report,
    )):
        return None, [], ""

    grounding = (evidence.get("market_websearch") or {}).get("grounding") or []
    grounded_urls = {
        _canonical_public_url(source.get(key))
        for source in grounding
        if isinstance(source, dict)
        for key in ("url", "grounding_url")
        if _canonical_public_url(source.get(key))
    }
    findings = []
    domains = set()
    finding_roles = set()
    for finding in underwriting.get("findings") or []:
        if not isinstance(finding, dict):
            continue
        url = str(finding.get("url") or "").strip()
        canonical_url = _canonical_public_url(url)
        finding_text = str(finding.get("finding") or "").strip()
        scope = str(finding.get("vehicle_scope") or "").strip()
        if not canonical_url or canonical_url not in grounded_urls or not finding_text or not scope:
            continue
        domain = str(urlparse(url).hostname or "").lower().removeprefix("www.")
        if not domain:
            continue
        domains.add(domain)
        finding_roles.add(str(finding.get("finding_role") or "").strip().upper())
        findings.append((finding, url))
    if (
        len(findings) < 2
        or len(domains) < 2
        or not {"CLEAN_RETAIL", "IMPAIRED_RETAIL"}.issubset(finding_roles)
    ):
        return None, [], ""

    clean_low = _number(underwriting.get("clean_retail_reference_low"))
    clean_high = _number(underwriting.get("clean_retail_reference_high"))
    impaired_low = _number(underwriting.get("impaired_retail_exit_low"))
    impaired_high = _number(underwriting.get("impaired_retail_exit_high"))
    recommended_low = _number(underwriting.get("recommended_wholesale_adjustment_low"))
    recommended_high = _number(underwriting.get("recommended_wholesale_adjustment_high"))
    values = (
        clean_low, clean_high, impaired_low, impaired_high,
        recommended_low, recommended_high,
    )
    if any(value is None for value in values):
        return None, [], ""
    if not all((
        0 < clean_low <= clean_high,
        0 < impaired_low <= impaired_high,
        impaired_high < clean_low,
        recommended_low <= recommended_high < 0,
        str(underwriting.get("methodology") or "").strip(),
        str(underwriting.get("limitations") or "").strip(),
    )):
        return None, [], ""

    minimum_retail_loss = float(clean_low - impaired_high)
    maximum_deduction = min(float(baseline) * 0.40, minimum_retail_loss)
    deduction = -round(min(abs(float(recommended_high)), maximum_deduction), 2)
    if deduction >= 0:
        return None, [], ""

    citations = [f"[{source_tag}] {confirmed_fact}"]
    for finding, url in findings:
        amount_low = _number(finding.get("amount_low")) or 0.0
        amount_high = _number(finding.get("amount_high")) or 0.0
        amount_text = (
            f" Quantified range {_money(amount_low)}-{_money(amount_high)}."
            if amount_low > 0 and amount_high >= amount_low
            else ""
        )
        citations.append(
            f"[PUBLIC_UNDERWRITING: {url}] {finding.get('finding')} "
            f"Scope: {finding.get('vehicle_scope')}.{amount_text}"
        )
    detail = (
        f"Applied {_signed(deduction)} from grounded dealer underwriting because "
        f"{source_tag} confirms {impairment_type.lower().replace('_', ' ')}. The closest-to-zero "
        f"recommended deduction was bounded by the minimum public retail-exit loss "
        f"({_money(minimum_retail_loss)}) and the 40% no-pair ceiling."
    )
    return deduction, citations, detail


def _reconditioned_mmr_pair(
    evidence: dict,
) -> tuple[Optional[dict[str, Any]], list[str]]:
    """Build the deterministic average-grade versus photo-grade Manheim pair."""
    row = ((evidence.get("manheim") or {}).get("reconditioned_mmr") or {})
    if row.get("status") != "complete":
        return None, []
    average_grade = _number(row.get("average_grade"))
    adjusted_grade = _number(row.get("adjusted_grade"))
    api_grade = _number(row.get("api_grade"))
    average_mmr = _number(row.get("average_grade_mmr"))
    adjusted_mmr = _number(row.get("adjusted_grade_mmr"))
    mmr_delta = _number(row.get("mmr_delta"))
    if None in (average_grade, adjusted_grade, api_grade, average_mmr, adjusted_mmr, mmr_delta):
        return None, []
    if adjusted_mmr - average_mmr != mmr_delta or mmr_delta == 0:
        return None, []

    observation_summary = "; ".join(
        f"{item.get('type') or 'vehicle'} photo {item.get('photo_id') or ''}: "
        f"{item.get('description') or ''}".strip()
        for item in row.get("observations") or []
        if isinstance(item, dict)
    )
    pair = {
        "source_type": "manheim",
        "without_feature_reference": f"Current MMR at average grade {average_grade:.1f}",
        "with_feature_reference": (
            f"Current MMR re-pulled at photo-supported grade {adjusted_grade:.1f} "
            f"(API grade {int(api_grade)})"
        ),
        "without_price": average_mmr,
        "with_price": adjusted_mmr,
        "observed_difference": mmr_delta,
        "same_build_explanation": (
            "Same VIN, mileage, region, and build; only Manheim condition grade changed "
            f"from the current MMR average {average_grade:.1f} to {adjusted_grade:.1f}."
        ),
    }
    citations = [
        f"[MANHEIM] Same-VIN current MMR moved from {_money(average_mmr)} at average "
        f"grade {average_grade:.1f} to {_money(adjusted_mmr)} at photo-supported grade "
        f"{adjusted_grade:.1f} (API grade {int(api_grade)}), a {_signed(mmr_delta)} difference.",
        f"[CRM_PHOTO] {observation_summary or 'Vehicle photos support the selected condition grade.'}",
    ]
    return pair, citations


def _audit_comparison_difference(
    audit_row: dict[str, Any], *, reject_lane_position: bool = False
) -> tuple[Optional[float], str]:
    """Choose the controlling observed difference from audited comparison pairs."""
    pairs = audit_row.get("comparison_pairs") or []
    ranked = {"gmtv": 0, "manheim": 1, "public": 2, "public_comp": 2}
    candidates = []

    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        comparison_text = " ".join(
            str(pair.get(field) or "")
            for field in (
                "without_feature_reference",
                "with_feature_reference",
                "same_build_explanation",
            )
        )
        if reject_lane_position and re.search(
            r"\b(?:lane|run)\s*(?:position|order|number)\b|\baverage_position\b",
            comparison_text,
            re.IGNORECASE,
        ):
            continue
        source_type = str(pair.get("source_type") or "").strip().lower()
        if source_type not in ranked:
            continue
        observed = _number(pair.get("observed_difference"))
        if observed is None:
            without_price = _number(pair.get("without_price"))
            with_price = _number(pair.get("with_price"))
            if without_price is not None and with_price is not None:
                observed = with_price - without_price
        if observed is None or observed == 0:
            continue
        candidates.append((ranked[source_type], observed, source_type))

    if not candidates:
        return None, ""
    _, observed, source_type = min(candidates, key=lambda item: item[0])
    return float(observed), source_type


def _clear_market_trend(trend: dict[str, Any], direction: str) -> bool:
    expected_accuracy = "lagging_low" if direction == "up" else "lagging_high"
    shift = _number(trend.get("sale_to_mmr_shift_pct_points"))
    recent_variance = _number(trend.get("recent_sale_to_mmr_pct"))
    expected_sign = 1 if direction == "up" else -1
    usable_points = int(_number(trend.get("usable_points")) or 0)
    coverage = _number(trend.get("coverage_pct")) or 0.0
    return all((
        usable_points >= 6,
        coverage >= 70.0,
        trend.get("chronological") is True,
        trend.get("market_around_mmr_direction") == direction,
        trend.get("mmr_accuracy_status") == expected_accuracy,
        shift is not None and shift * expected_sign >= 4.0,
        recent_variance is not None and recent_variance * expected_sign >= 5.0,
    ))


def _market_trend_reasoned_adjustment(
    delta: float,
    baseline: float,
    evidence: dict,
    audit_row: dict[str, Any],
    validated_evidence: list[tuple[str, str, str]],
) -> tuple[Optional[float], str]:
    """Bound an auditor-reasoned correction by its supported wholesale value."""
    if delta == 0:
        return None, ""

    trend = ((evidence.get("gmtv") or {}).get("market_trend") or {})
    direction = "up" if delta > 0 else "down"
    if not _clear_market_trend(trend, direction):
        return None, ""

    supported_wholesale = _number(audit_row.get("market_supported_wholesale_value"))
    reasoning = str(audit_row.get("market_trend_reasoning") or "").strip()
    past_sales = str(audit_row.get("past_sales_assessment") or "").strip()
    lane_position = str(audit_row.get("lane_position_assessment") or "").strip()
    retail_corroborated = audit_row.get("retail_corroborated") is True
    expected_sign = 1 if direction == "up" else -1
    has_gmtv = any(tag == "GMTV" for tag, _, _ in validated_evidence)
    has_grounded_retail = any(
        tag == "PUBLIC_COMP" and source_url
        for tag, _, source_url in validated_evidence
    )
    if not all((
        supported_wholesale is not None,
        (supported_wholesale - baseline) * expected_sign > 0,
        retail_corroborated,
        bool(reasoning),
        bool(past_sales),
        bool(lane_position),
        has_gmtv,
        has_grounded_retail,
    )):
        return None, ""

    supported_delta = float(supported_wholesale) - baseline
    adjusted = min(abs(delta), abs(supported_delta)) * expected_sign
    adjusted = round(adjusted, 2)
    detail = (
        f"{reasoning} Past sales: {past_sales} Lane placement: {lane_position} "
        "Date-matched GMTV sales establish general market direction: "
        f"{int(_number(trend.get('usable_points')) or 0)} usable points, "
        f"{(_number(trend.get('coverage_pct')) or 0.0):.1f}% coverage, "
        f"recent sale-to-MMR {(_number(trend.get('recent_sale_to_mmr_pct')) or 0.0):+.2f}%, "
        f"shift {(_number(trend.get('sale_to_mmr_shift_pct_points')) or 0.0):+.2f} points, "
        f"MMR status {trend.get('mmr_accuracy_status')}. The retail- and sales-supported "
        f"wholesale value is {_money(float(supported_wholesale))}; the adjustment is "
        f"{_signed(adjusted)} from the {_money(baseline)} baseline rather than a direct "
        "transfer of the aggregate percentage."
    )
    return adjusted, detail


def _guard_market_correction(
    delta: float,
    *,
    baseline: float,
    already_used: float,
    evidence: dict,
    validated_evidence: list[tuple[str, str, str]],
    audit_row: dict[str, Any],
) -> tuple[float, str]:
    """Cap market/retail corrections unless trend and retail corroboration are strong."""
    if delta == 0:
        return 0.0, ""

    trend = ((evidence.get("gmtv") or {}).get("market_trend") or {})
    direction = "up" if delta > 0 else "down"
    clear_trend = _clear_market_trend(trend, direction)

    grounded_domains = {
        urlparse(str(source.get("url") or source.get("grounding_url") or "")).netloc.lower()
        for source in (evidence.get("market_websearch") or {}).get("grounding") or []
        if isinstance(source, dict)
        and urlparse(str(source.get("url") or source.get("grounding_url") or "")).netloc
    }
    cited_public_urls = {
        source_url for tag, _, source_url in validated_evidence
        if tag == "PUBLIC_COMP" and source_url
    }
    public_priced_pairs = []
    for pair in audit_row.get("comparison_pairs") or []:
        if not isinstance(pair, dict):
            continue
        if str(pair.get("source_type") or "").strip().lower() not in {"public", "public_comp"}:
            continue
        without_price = _number(pair.get("without_price"))
        with_price = _number(pair.get("with_price"))
        if without_price is None or with_price is None:
            continue
        observed = _number(pair.get("observed_difference"))
        if observed is None:
            observed = with_price - without_price
        if observed and delta * observed > 0:
            public_priced_pairs.append(pair)
    strong_web_support = (
        len(grounded_domains) >= 3
        and len(cited_public_urls) >= 2
        and bool(public_priced_pairs)
    )

    minor_budget = max(500.0, baseline * 0.03)
    major_budget = max(500.0, baseline * 0.10)
    major_requested = already_used + abs(delta) > minor_budget
    major_allowed = clear_trend and strong_web_support
    budget = major_budget if major_allowed else minor_budget
    remaining = max(0.0, budget - already_used)
    guarded = min(abs(delta), remaining)
    guarded_delta = guarded if delta > 0 else -guarded

    if abs(guarded_delta) < abs(delta):
        if major_requested and not major_allowed:
            missing = []
            if not clear_trend:
                missing.append("clear date-matched sale-to-MMR trend")
            if not strong_web_support:
                missing.append("three web domains, two cited sources, and a priced retail pair")
            return guarded_delta, (
                f"Market correction capped at the minor-evidence budget ({_money(minor_budget)}) "
                f"because major-correction support lacked {' and '.join(missing)}."
            )
        return guarded_delta, (
            f"Market correction capped at 10% of baseline ({_money(major_budget)}) to prevent overcorrection."
        )

    if major_requested:
        return guarded_delta, (
            "Major market correction retained only because the date-matched GMTV/MMR trend, "
            "independent web sources, cited retail evidence, and priced comparison pair agree."
        )
    return guarded_delta, "Minor market correction remained within the 3% evidence budget."


def _ladder_row(
    *,
    stage_id: str,
    label: str,
    delta: float,
    running_value: float,
    method: str,
    note: str,
    evidence: list[dict],
    unsupported: bool = False,
    capped: bool = False,
) -> dict[str, Any]:
    if stage_id == "baseline":
        impact = _money(running_value)
        tone = "neu"
    elif unsupported:
        impact = f"{_signed(delta)} dropped - no evidence"
        tone = "neu"
    else:
        impact = _signed(delta)
        tone = "pos" if delta > 0 else "neg" if delta < 0 else "neu"

    return {
        "stage_id": stage_id,
        "label": label,
        "delta": float(delta),
        "impact": impact,
        "tone": tone,
        "running_value": float(running_value),
        "running_display": _money(running_value),
        "method": method or "none",
        "unsupported": bool(unsupported),
        "capped": bool(capped),
        "note": note or "",
        "evidence": evidence,
    }


def _partial_ladder(evidence: dict, vin: str) -> dict[str, Any]:
    """Build the durable in-progress ladder from evidence that has actually landed.

    The frontend merges rows by stage_id, so these provisional zero-dollar rows are
    intentionally stable patch points. As collectors finish, the same stage IDs gain
    evidence/notes without resetting the MMR anchor. The final reasoning pass replaces
    these provisional rows with evidence-supported numeric deltas.
    """
    # AccuTrade requires image interpretation, so an MMR-only provisional baseline would
    # bias the later anchor decision. Keep the in-progress ladder empty until analysis.
    if not evidence.get("valuation_anchor_selected"):
        return {
            "vin": vin,
            "baseline_source": "Awaiting MMR and AccuTrade Target Auction comparison",
            "stages": [],
            "final_value": 0.0,
            "rounded_value": None,
            "complete": False,
            "implausible": False,
            "implausible_reason": "",
        }

    baseline, source, path = _extract_mmr_baseline(evidence)
    rows: list[dict[str, Any]] = []
    if baseline is None:
        return {
            "vin": vin,
            "baseline_source": source,
            "stages": [],
            "final_value": 0.0,
            "rounded_value": None,
            "complete": False,
            "implausible": False,
            "implausible_reason": "",
        }

    running = float(baseline)
    rows.append(
        _ladder_row(
            stage_id="baseline",
            label=f"Baseline - {source}",
            # Baseline is an anchor, not an adjustment. Keep delta=0 so Pass 3
            # can never confuse the hammer with the starting value again.
            delta=0.0,
            running_value=running,
            method="mmr_table",
            note=f"Anchored on {source}; collector evidence is still arriving.",
            evidence=[
                _evidence_ref(
                    f"{source}: {_money(baseline)} ({path})",
                    kind="mmr_table",
                    vin=vin,
                )
            ],
        )
    )

    manheim = evidence.get("manheim") or {}
    current_mmr = manheim.get("current_mmr") or {}
    gmtv = evidence.get("gmtv") or {}
    browser = evidence.get("browser") or {}
    jdpower = evidence.get("jdpower") or {}
    market = evidence.get("market_websearch") or {}

    # MMR/GMTV patch the mileage rung as soon as they arrive. The adjusted MMR
    # is already the baseline, so this provisional rung remains $0 until the
    # analyst has enough evidence to justify a separate mileage adjustment.
    mileage_evidence: list[dict[str, Any]] = []
    if current_mmr:
        mileage_evidence.append(
            _evidence_ref(
                "Current Manheim MMR collected for the target VIN/odometer; adjusted MMR is the live anchor.",
                kind="mmr_table",
                vin=vin,
            )
        )
    gmtv_sales = gmtv.get("sales") or []
    if gmtv_sales:
        mileage_evidence.append(
            _evidence_ref(
                f"{len(gmtv_sales)} recent GMTV same make/model sale(s) from 3monthsales.csv collected for mileage/market comparison.",
                kind="listing",
                vin=vin,
            )
        )
    if mileage_evidence:
        rows.append(
            _ladder_row(
                stage_id="mileage",
                label=STAGE_LABELS["mileage"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="Mileage evidence collected; final adjustment is pending the valuation reasoning pass.",
                evidence=mileage_evidence,
            )
        )

    # Browser evidence is intentionally not interpreted in Python. Its arrival
    # still patches the history/condition rows immediately so the rep can see
    # that the evidence is present while the model is reading the artifacts.
    carfax_ready = bool(browser.get("carfax_pdf"))
    autocheck_ready = bool(browser.get("autocheck_pdf") or browser.get("autocheck"))
    accutrade_ready = bool(browser.get("accutrade_screenshot"))
    if carfax_ready:
        rows.append(
            _ladder_row(
                stage_id="history",
                label=STAGE_LABELS["history"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="CARFAX collected; history impact is pending artifact review.",
                evidence=[
                    _evidence_ref("CARFAX report collected and queued for valuation review.", kind="listing", vin=vin)
                ],
            )
        )
    if autocheck_ready and not carfax_ready:
        rows.append(
            _ladder_row(
                stage_id="history",
                label=STAGE_LABELS["history"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="AutoCheck collected; history impact is pending report review.",
                evidence=[
                    _evidence_ref("AutoCheck report collected and queued for valuation review.", kind="listing", vin=vin)
                ],
            )
        )
    elif autocheck_ready and carfax_ready:
        history_row = next((row for row in rows if row.get("stage_id") == "history"), None)
        if history_row:
            history_row["note"] = "CARFAX and AutoCheck collected; history impact is pending report review."
            history_row["evidence"].append(
                _evidence_ref("AutoCheck report collected and queued for valuation review.", kind="listing", vin=vin)
            )
    if accutrade_ready:
        rows.append(
            _ladder_row(
                stage_id="condition",
                label=STAGE_LABELS["condition"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="AccuTrade appraisal collected; condition impact is pending artifact review.",
                evidence=[
                    _evidence_ref("AccuTrade appraisal screenshot collected and queued for valuation review.", kind="listing", vin=vin)
                ],
            )
        )

    # JD Power provides build/spec context. It does not move value by itself,
    # but its arrival patches the factory-equipment rung instead of leaving a
    # blank UI while evidence collection continues.
    if jdpower.get("data") is not None:
        rows.append(
            _ladder_row(
                stage_id="factory_equipment",
                label=STAGE_LABELS["factory_equipment"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="JD Power build/spec evidence collected; equipment adjustment pending reasoning.",
                evidence=[
                    _evidence_ref("JD Power VIN/build data collected.", kind="listing", vin=vin)
                ],
            )
        )

    # Once public search lands, patch the market/ceiling rungs. Again these are
    # provisional zero-dollar rows; the final pass is the only place that may
    # apply a non-zero adjustment.
    grounding = market.get("grounding") or []
    if market.get("research_text") or grounding:
        market_ref = _evidence_ref(
            f"Public market research complete with {len(grounding)} grounded source(s).",
            kind="listing",
            vin=vin,
        )
        rows.append(
            _ladder_row(
                stage_id="market_heat",
                label=STAGE_LABELS["market_heat"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="Public market evidence collected; market-heat adjustment pending final reasoning.",
                evidence=[market_ref],
            )
        )
        rows.append(
            _ladder_row(
                stage_id="retail_ceiling",
                label=STAGE_LABELS["retail_ceiling"],
                delta=0.0,
                running_value=running,
                method="collector_pending_reasoning",
                note="Retail/public-market context collected; ceiling adjustment pending final reasoning.",
                evidence=[market_ref],
            )
        )

    return {
        "vin": vin,
        "baseline_source": source,
        "stages": rows,
        "final_value": running,
        # Critical: an in-progress ladder is never exposed as a final valuation.
        "rounded_value": None,
        "complete": False,
        "implausible": False,
        "implausible_reason": "",
    }


def _normalize_final_ladder(final: dict, evidence: dict, vin: str) -> tuple[list[dict[str, Any]], float, str]:
    """Preserve the analyst ladder and validate its arithmetic without re-valuing it.

    Pass 2 and the audit own the valuation decision. Python validates that every
    non-policy adjustment has a controlled observed comparison difference. GMTV sold
    pairs take precedence over Manheim and public pairs; unsupported adjustments are
    denied rather than preserved merely because they have a citation.
    """
    structured_rows = final.get("value_ladder") or []

    by_stage: dict[str, dict[str, Any]] = {}
    for row in structured_rows:
        if not isinstance(row, dict):
            continue
        stage_id = str(row.get("stage_id") or "").strip()
        if not stage_id:
            continue
        if stage_id in by_stage:
            raise ValueError(f"Value ladder contains duplicate stage_id {stage_id!r}")
        by_stage[stage_id] = row

    mmr_baseline, mmr_source, _ = _extract_mmr_baseline(evidence)
    stated_amount = _number(((final.get("valuation") or {}).get("amount")))
    baseline_row = by_stage.get("baseline") or {}

    # Preserve the analyst's explicit MMR-versus-AccuTrade choice. MMR is only a
    # fallback when the mapped baseline is absent or invalid.
    baseline = _number(baseline_row.get("running_value"))
    if baseline is None or baseline <= 0:
        # Backward compatibility for older mapped schemas that put the baseline in delta.
        baseline = _number(baseline_row.get("delta"))
    if baseline is None or baseline <= 0:
        baseline = mmr_baseline
    if baseline is None or baseline <= 0:
        baseline = stated_amount
    if baseline is None or baseline <= 0:
        raise ValueError("Final ladder has no positive baseline")

    baseline_label = str(baseline_row.get("label") or "").strip()
    baseline_method = str(baseline_row.get("method") or "").strip()
    source = baseline_label or mmr_source or "Analyst evidence baseline"

    rows: list[dict[str, Any]] = []
    running = float(baseline)
    market_correction_used = 0.0
    available_tags = _available_ladder_evidence_tags(evidence)
    audit_rows = {
        str(row.get("stage_id") or "").strip(): row
        for row in (evidence.get("adjustment_audit") or {}).get("adjustments") or []
        if isinstance(row, dict) and str(row.get("stage_id") or "").strip()
    }

    baseline_evidence = baseline_row.get("evidence") or []
    if not baseline_evidence:
        baseline_evidence = [f"Analyst-selected baseline: {_money(running)}"]

    accutrade_anchor = _number((final.get("anchors") or {}).get("accutrade"))
    baseline_identity = f"{baseline_label} {baseline_method}".lower()
    public_auction_baseline = any(
        marker in baseline_identity
        for marker in ("public auction", "public-auction", "auction sold fallback")
    )
    if public_auction_baseline:
        if mmr_baseline is not None or accutrade_anchor is not None:
            raise ValueError(
                "Public-auction fallback baseline is allowed only when both MMR and "
                "AccuTrade Target Auction are unavailable"
            )
        valid_fallback, public_baseline_evidence, fallback_error = (
            _public_auction_baseline_validation(list(baseline_evidence), evidence)
        )
        if not valid_fallback:
            raise ValueError(fallback_error)
        baseline_evidence = [
            f"[PUBLIC_COMP: {source_url}] {detail}"
            for _, detail, source_url in public_baseline_evidence
        ]

    baseline_kind = (
        "mmr_table"
        if "mmr" in baseline_method.lower() or "manheim" in baseline_label.lower()
        else "public_comp" if public_auction_baseline else "listing"
    )
    baseline_refs = [
        _evidence_ref(str(line), kind=baseline_kind, vin=vin)
        for line in baseline_evidence
        if str(line).strip()
    ]

    baseline_note = str(baseline_row.get("note") or f"Anchored on {source}.")
    baseline_delta = _number(baseline_row.get("delta"))
    if baseline_delta not in (None, 0.0):
        baseline_note = (
            baseline_note
            + f" Baseline delta {_signed(baseline_delta)} ignored; baseline running_value is authoritative."
        ).strip()

    rows.append(
        _ladder_row(
            stage_id="baseline",
            label=baseline_label or f"Baseline - {source}",
            delta=0.0,
            running_value=running,
            method=baseline_method or ("mmr_table" if baseline_kind == "mmr_table" else "listing_spread"),
            note=baseline_note,
            evidence=baseline_refs,
        )
    )

    for stage_id in STAGE_ORDER[1:]:
        proposed = by_stage.get(stage_id) or {}
        delta = _number(proposed.get("delta"))
        if delta is None:
            delta = 0.0
        analyst_delta = float(delta)

        audit_row = dict(audit_rows.get(stage_id) or {})
        structured_public_pair, structured_public_evidence = (
            _structured_public_research_pair(stage_id, evidence)
        )
        dealer_underwriting_delta, dealer_underwriting_evidence, dealer_underwriting_note = (
            _grounded_dealer_underwriting(stage_id, evidence, vin, float(baseline))
        )
        reconditioned_mmr_pair, reconditioned_mmr_evidence = (
            _reconditioned_mmr_pair(evidence)
            if stage_id == "condition"
            else (None, [])
        )
        audit_row["comparison_pairs"] = [
            pair for pair in audit_row.get("comparison_pairs") or []
            if isinstance(pair, dict)
            and str(pair.get("source_type") or "").strip().lower()
            not in {"public", "public_comp"}
            and not (
                stage_id == "condition"
                and str(pair.get("source_type") or "").strip().lower() == "manheim"
            )
        ]
        if reconditioned_mmr_pair is not None:
            audit_row["comparison_pairs"].append(reconditioned_mmr_pair)
        if structured_public_pair is not None:
            audit_row["comparison_pairs"].append(structured_public_pair)
        raw_evidence = list(proposed.get("evidence") or []) + list(
            audit_row.get("evidence") or []
        ) + reconditioned_mmr_evidence + structured_public_evidence + dealer_underwriting_evidence
        validated_evidence = _validated_ladder_evidence(raw_evidence, available_tags, evidence)
        evidence_refs = [
            _evidence_ref(detail, kind=LADDER_EVIDENCE_KINDS[tag], vin=vin, source_url=source_url)
            for tag, detail, source_url in validated_evidence
        ]

        note = str(proposed.get("note") or "")
        method = str(proposed.get("method") or "none")

        audited_delta = _number(audit_row.get("audited_delta"))
        if audited_delta is not None:
            delta = audited_delta

        condition_mmr_already_in_baseline = False
        if stage_id == "condition" and reconditioned_mmr_pair is not None:
            adjusted_grade_mmr = _number(reconditioned_mmr_pair.get("with_price"))
            condition_mmr_already_in_baseline = bool(
                adjusted_grade_mmr is not None
                and abs(float(baseline) - adjusted_grade_mmr) < 1.0
                and "manheim" in baseline_identity
            )
            if condition_mmr_already_in_baseline and delta != 0:
                original = delta
                delta = 0.0
                note = (
                    (note + " " if note else "")
                    + f"The {_signed(original)} condition-grade effect is already embedded "
                    + "in the selected Manheim condition-grade baseline; it was not counted twice."
                ).strip()

        capped = False
        trend_directional_nudge = False
        if stage_id == "market_heat" and delta != 0:
            observed_difference, _ = _audit_comparison_difference(
                audit_row,
                reject_lane_position=True,
            )
            if observed_difference is None:
                original = delta
                trend_delta, trend_detail = _market_trend_reasoned_adjustment(
                    float(delta),
                    float(baseline),
                    evidence,
                    audit_row,
                    validated_evidence,
                )
                if trend_delta is not None:
                    delta = trend_delta
                    trend_directional_nudge = True
                    capped = abs(delta) < abs(original)
                    if not any(tag == "GMTV" for tag, _, _ in validated_evidence):
                        validated_evidence.append(("GMTV", trend_detail, ""))
                        evidence_refs.append(
                            _evidence_ref(
                                trend_detail,
                                kind=LADDER_EVIDENCE_KINDS["GMTV"],
                                vin=vin,
                            )
                        )
                    note = (
                        (note + " " if note else "")
                        + f"Bound market adjustment from {_signed(original)} to "
                        + f"{_signed(delta)} using the auditor's retail-, past-sales-, "
                        + "and lane-corroborated wholesale range; the aggregate GMTV "
                        + "shift established pressure but was not transferred directly."
                    ).strip()

        unsupported = bool(delta != 0 and not evidence_refs)
        denied_for_comparison = False
        aggregate_vhr_adjustment = False
        dealer_underwriting_adjustment = False
        if stage_id == "history":
            history_text = " ".join(
                str(value)
                for value in (
                    list(raw_evidence)
                    + list(audit_row.get("evidence") or [])
                    + [audit_row.get("reason") or "", proposed.get("note") or ""]
                )
            )
            audit_proposed_delta = _number(audit_row.get("proposed_delta"))
            policy_candidate = (
                audit_proposed_delta
                if audit_proposed_delta is not None and audit_proposed_delta < 0
                else analyst_delta
            )
            canadian_claimed = bool(CANADIAN_HISTORY_PATTERN.search(history_text))
            canadian_confirmation = _confirmed_canadian_history_source(
                validated_evidence,
                evidence,
                vin,
            )
            canadian_policy_delta = _canadian_history_policy_delta(
                canadian_confirmation,
                float(baseline),
                policy_candidate,
            )
            if canadian_policy_delta is not None:
                original = delta
                delta = canadian_policy_delta
                unsupported = False
                confirmation_tag, confirmation_detail, confirmation_url = canadian_confirmation
                if not any(tag == confirmation_tag for tag, _, _ in validated_evidence):
                    validated_evidence.append(canadian_confirmation)
                    evidence_refs.append(_evidence_ref(
                        confirmation_detail,
                        kind=LADDER_EVIDENCE_KINDS[confirmation_tag],
                        vin=vin,
                        source_url=confirmation_url,
                    ))
                if not any(tag == "CANADIAN_POLICY" for tag, _, _ in validated_evidence):
                    validated_evidence.append((
                        "CANADIAN_POLICY",
                        f"{confirmation_tag}-confirmed Canadian history requires a 15-20% wholesale discount.",
                        "",
                    ))
                    evidence_refs.append(_evidence_ref(
                        f"{confirmation_tag}-confirmed Canadian history requires a 15-20% wholesale discount.",
                        kind=LADDER_EVIDENCE_KINDS["CANADIAN_POLICY"],
                        vin=vin,
                    ))
                if original != delta:
                    note = (
                        (note + " " if note else "")
                        + f"Restored the {confirmation_tag}-confirmed Canadian-history policy adjustment "
                        + "at "
                        + _signed(delta)
                        + "; a controlled comparison pair is not required."
                    ).strip()
            elif canadian_claimed:
                delta = 0.0
                denied_for_comparison = True
                method = "History review - Canadian history unconfirmed"
                note = (
                    "Canadian history was not confirmed by CARFAX or AutoCheck, or by an "
                    "exact-VIN grounded public match when both reports were unavailable. "
                    "Manheim and other valuation sources cannot establish Canadian history; "
                    "the Canadian-history adjustment was discarded."
                )

            target_bad_vhr = canadian_policy_delta is None and delta < 0 and bool(
                re.search(
                    r"\b(bad\s+(?:carfax|autocheck|vhr)|accident|damage|branded|salvage|total loss)\b",
                    history_text,
                    re.IGNORECASE,
                )
            )
            vhr_performance = (evidence.get("gmtv") or {}).get(
                "same_year_make_model_vhr_net_profit"
            ) or {}
            recommended = _number(vhr_performance.get("recommended_bad_vhr_deduction")) or 0.0
            sufficient = bool(vhr_performance.get("sufficient_sample"))
            if target_bad_vhr:
                if sufficient and recommended > 0:
                    delta = -abs(recommended)
                    aggregate_vhr_adjustment = True
                    unsupported = False
                    note = (
                        (note + " " if note else "")
                        + "Applied the mandatory same-year/make/model bad-versus-clean VHR performance deduction."
                    ).strip()
                else:
                    delta = 0.0
                    denied_for_comparison = True
                    note = (
                        (note + " " if note else "")
                        + "Bad-history vehicles did not perform worse than clean-history vehicles in the same-year/make/model evidence; history adjustment discarded."
                    ).strip()

            existing_difference, _ = _audit_comparison_difference(audit_row)
            if all((
                canadian_policy_delta is None,
                not aggregate_vhr_adjustment,
                existing_difference is None,
                dealer_underwriting_delta is not None,
            )):
                delta = float(dealer_underwriting_delta)
                dealer_underwriting_adjustment = True
                denied_for_comparison = False
                unsupported = False
                method = "Grounded dealer underwriting"
                note = (
                    (note + " " if note else "")
                    + dealer_underwriting_note
                ).strip()

        is_policy_adjustment = any(tag == "CANADIAN_POLICY" for tag, _, _ in validated_evidence)
        if (
            delta != 0
            and not is_policy_adjustment
            and not aggregate_vhr_adjustment
            and not trend_directional_nudge
            and not condition_mmr_already_in_baseline
            and not dealer_underwriting_adjustment
        ):
            observed_difference, comparison_source = _audit_comparison_difference(
                audit_row,
                reject_lane_position=stage_id == "market_heat",
            )
            if observed_difference is None:
                denied_for_comparison = True
                original = delta
                delta = 0.0
                note = (
                    (note + " " if note else "")
                    + f"Proposed {_signed(original)} but no controlled comparison pair was audited; adjustment denied."
                ).strip()
            elif delta * observed_difference < 0:
                denied_for_comparison = True
                original = delta
                delta = 0.0
                note = (
                    (note + " " if note else "")
                    + f"Proposed {_signed(original)} conflicts with the {comparison_source} observed difference; adjustment denied."
                ).strip()
            elif stage_id == "condition" and comparison_source == "manheim":
                original = delta
                delta = float(observed_difference)
                if original != delta:
                    note = (
                        (note + " " if note else "")
                        + f"Replaced {_signed(original)} with the controlling same-VIN "
                        + f"condition-grade MMR difference of {_signed(delta)}."
                    ).strip()
            elif abs(delta) > abs(observed_difference) + 500.0:
                original = delta
                delta = float(observed_difference)
                note = (
                    (note + " " if note else "")
                    + f"Capped {_signed(original)} at the controlling {comparison_source} "
                    + f"difference of {_signed(delta)} within $500."
                ).strip()

        if unsupported:
            original = delta
            delta = 0.0
            note = (
                (note + " " if note else "")
                + f"Proposed {_signed(original)} but supplied no valid collected-source evidence; adjustment dropped."
            )
        elif denied_for_comparison:
            pass
        elif not proposed:
            note = "No adjustment supplied for this stage; using $0."

        if stage_id in {"market_heat", "retail_ceiling"} and delta != 0:
            guard_note = ""
            if not trend_directional_nudge:
                before_guard = delta
                delta, guard_note = _guard_market_correction(
                    float(delta),
                    baseline=float(baseline),
                    already_used=market_correction_used,
                    evidence=evidence,
                    validated_evidence=validated_evidence,
                    audit_row=audit_row,
                )
                capped = capped or abs(delta) < abs(before_guard)
            market_correction_used += abs(delta)
            if guard_note:
                note = ((note + " ") if note else "") + guard_note

        expected_running = round(running + float(delta), 2)
        running = expected_running
        rows.append(
            _ladder_row(
                stage_id=stage_id,
                label=str(proposed.get("label") or STAGE_LABELS[stage_id]),
                delta=float(delta),
                running_value=running,
                method=method,
                note=note,
                evidence=evidence_refs,
                unsupported=unsupported,
                capped=capped,
            )
        )

    return rows, float(running), source

def _manheim_parent_status(status: dict[str, Any]) -> dict[str, Any]:
    names = (
        "manheim_current_mmr",
        "manheim_historical_mmr",
        "manheim_future_mmr",
        "manheim_condition",
        "manheim_vin_sales",
        "manheim_ymm_sales",
        "manheim_mm_sales",
    )
    rows = [status.get(name) or {} for name in names]
    states = [str(row.get("state") or "running") for row in rows]
    if all(state == "complete" for state in states):
        state = "complete"
    elif all(state in {"complete", "error"} for state in states):
        state = "partial_error" if any(s == "error" for s in states) else "complete"
    else:
        state = "running"
    return {"state": state, "parts": {name: copy.deepcopy(status.get(name) or {}) for name in names}}


def _vhr_summary(value: Any, *, max_chars: int = 1200) -> str:
    """Flatten parsed VHR content into a concise embed-safe summary."""
    lines: list[str] = []

    def visit(item: Any, label: str = "") -> None:
        if item in (None, "", [], {}):
            return
        if isinstance(item, dict):
            for key, child in item.items():
                child_label = str(key).replace("_", " ").strip()
                visit(child, " ".join(part for part in (label, child_label) if part))
            return
        if isinstance(item, list):
            for child in item:
                visit(child, label)
            return

        text = re.sub(r"\s+", " ", str(item)).strip()
        if text:
            lines.append(f"{label}: {text}" if label else text)

    visit(value)
    summary = " | ".join(dict.fromkeys(lines))
    if len(summary) <= max_chars:
        return summary
    return summary[:max_chars].rstrip() + "..."


def _public_market_scan_rows(
    evidence: dict,
    ladder_rows: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, Any]]:
    market = evidence.get("market_websearch") or {}
    adjustment_research = market.get("adjustment_research") or {}
    by_stage = adjustment_research.get("by_stage") or {}
    rows: list[dict[str, Any]] = []

    for stage_id in RESEARCH_ADJUSTMENT_STAGES:
        research = by_stage.get(stage_id) if isinstance(by_stage, dict) else None
        covered = _research_adjustment_covered(research)
        verdict = str((research or {}).get("verdict") or "MISSING") if isinstance(research, dict) else "MISSING"
        pair, _ = _structured_public_research_pair(stage_id, evidence)
        adjustment_eligible = pair is not None
        underwriting_eligible = any(
            ladder_row.get("stage_id") == stage_id
            and float(ladder_row.get("delta") or 0) != 0
            and any(
                item.get("kind") == LADDER_EVIDENCE_KINDS["PUBLIC_UNDERWRITING"]
                for item in ladder_row.get("evidence") or []
                if isinstance(item, dict)
            )
            for ladder_row in ladder_rows or []
            if isinstance(ladder_row, dict)
        )
        verification = (
            research.get("grounding_verification") or {}
            if isinstance(research, dict)
            else {}
        )
        evidence_tier = _public_research_evidence_tier(
            research,
            adjustment_eligible=adjustment_eligible,
            underwriting_eligible=underwriting_eligible,
        )
        verified_source_count = int(verification.get("verified_source_count") or 0)
        verified_domain_count = int(verification.get("verified_domain_count") or 0)
        context_eligible = evidence_tier != UNVERIFIED_CONTEXT
        directional_context_eligible = (
            not adjustment_eligible and verified_domain_count >= 2
        )

        details = [verdict.replace("_", " ").title() + "."]
        if isinstance(research, dict):
            for key in ("claim", "research_summary"):
                value = str(research.get(key) or "").strip()
                if value and value not in details:
                    details.append(value)
        if adjustment_eligible:
            details.append("Verified comparison pair; eligible to support this adjustment.")
        elif underwriting_eligible:
            details.append(
                "Verified dealer underwriting applied to a collector-confirmed impairment; "
                "the deduction was conservatively bounded without treating the sources as a vehicle pair."
            )
        elif evidence_tier == VERIFIED_LISTING_CONTEXT:
            details.append(
                "Verified listing evidence retained as context; no controlled pair, "
                "so it cannot support a feature-specific dollar adjustment."
            )
        elif directional_context_eligible:
            details.append(
                "Multiple independent grounded sources may corroborate a bounded market "
                "direction or range; they cannot set a feature-specific dollar adjustment."
            )
        elif evidence_tier == VERIFIED_MARKET_CONTEXT:
            details.append(
                "Verified market source retained as context; it cannot support a "
                "feature-specific dollar adjustment."
            )
        elif covered:
            details.append("Investigation retained as evidence of research; not eligible to support a nonzero adjustment.")
        else:
            details.append("Required investigation did not return a complete research object.")

        rows.append({
            "name": STAGE_LABELS[stage_id],
            "url": "",
            "grounding_url": "",
            "note": " ".join(details),
            "category": stage_id,
            "verdict": verdict,
            "research_complete": covered,
            "adjustment_eligible": adjustment_eligible,
            "underwriting_eligible": underwriting_eligible,
            "context_eligible": context_eligible,
            "directional_context_eligible": directional_context_eligible,
            "evidence_tier": evidence_tier,
            "verified_source_count": verified_source_count,
            "verified_domain_count": verified_domain_count,
            "verified_source_urls": list(verification.get("matched_source_urls") or []),
            "verified_comparison_urls": list(
                verification.get("matched_comparison_urls") or []
            ),
            "kind": "investigation",
        })

    rows.extend(
        {
            "name": row.get("title") or "Web result",
            "url": row.get("url") or row.get("grounding_url") or "",
            "grounding_url": row.get("grounding_url") or "",
            "note": row.get("reasoning") or "",
            "kind": "grounded_source",
            "adjustment_eligible": False,
            "context_eligible": True,
            "directional_context_eligible": False,
            "evidence_tier": VERIFIED_MARKET_CONTEXT,
        }
        for row in (market.get("grounding") or [])[:30]
        if isinstance(row, dict)
    )
    return rows


def _cards(evidence: dict, ladder_rows: list[dict], final: Optional[dict] = None) -> dict[str, Any]:
    manheim = evidence.get("manheim") or {}
    gmtv = evidence.get("gmtv") or {}
    browser = evidence.get("browser") or {}
    market = evidence.get("market_websearch") or {}
    photos = (evidence.get("crm_photos") or {}).get("images") or []

    manheim_rows = []
    for label, key in (
        ("Current MMR", "current_mmr"),
        ("Historical MMR", "historical_mmr"),
        ("Future MMR", "future_mmr"),
        ("Condition report", "condition_report"),
        ("VIN transactions", "live_sale"),
        ("YMM transactions", "comp_sales"),
        ("Make/model transactions", "all_sales"),
    ):
        if manheim.get(key) is not None:
            manheim_rows.append({"label": label, "data": manheim.get(key)})

    artifact_summary = browser.get("artifact_summary") or {}
    report_text = browser.get("report_text") or {}
    report_text_status = browser.get("report_text_status") or {}
    carfax_status = report_text_status.get("carfax") or {}
    carfax_text = str(report_text.get("carfax") or "").strip()
    ai_carfax = (final or {}).get("carfax") or {}
    if not isinstance(ai_carfax, dict):
        ai_carfax = {}
    ai_carfax_status = str(ai_carfax.get("status") or "").strip().lower()
    ai_carfax_summary = re.sub(
        r"\s+", " ", str(ai_carfax.get("summary") or "")
    ).strip()
    report_collected = bool(browser.get("carfax_pdf"))
    report_parsed = (
        bool(carfax_text)
        and carfax_status.get("extracted") is True
        and carfax_status.get("vin_matched", True) is True
    )
    if report_parsed and ai_carfax_status == "unavailable":
        ai_carfax_status = "unknown"
        if "unavailable" in ai_carfax_summary.lower():
            ai_carfax_summary = "CARFAX report parsed; history classification was inconclusive."
    carfax_card = {
        "report_collected": report_collected,
        "parsed": report_parsed,
        "status": ai_carfax_status or ("pending" if report_collected else "unavailable"),
        "summary": ai_carfax_summary or (
            "AI CARFAX review pending."
            if report_collected
            else "CARFAX unavailable."
        ),
    }

    autocheck = browser.get("autocheck") or {}
    if not isinstance(autocheck, dict):
        autocheck = {}
    autocheck_card = copy.deepcopy(autocheck)
    if browser.get("autocheck_pdf"):
        autocheck_card.setdefault("report_collected", True)
        autocheck_card.setdefault("artifact", "autocheck_pdf")
    sections = autocheck_card.get("sections")
    autocheck_card["parsed"] = isinstance(sections, dict) and bool(sections)
    autocheck_card["summary"] = _vhr_summary(sections)

    return {
        "Accutrade_comps": ([artifact_summary.get("accutrade_screenshot")]
                            if artifact_summary.get("accutrade_screenshot") else []),
        "Carfax_scan": carfax_card,
        "Autocheck_scan": autocheck_card,
        "GMTV_comps": (gmtv.get("sales") or [])[:50],
        "Market_trend": gmtv.get("market_trend") or {},
        "Image_scans": [
            {
                "source": image.get("type") or image.get("name") or f"CRM photo {index + 1}",
                "image": image.get("image_url") or "",
                "image_description": image.get("description") or "",
                "impact": "Ava flagged damage" if image.get("damaged") else "",
            }
            for index, image in enumerate(photos[:50])
            if isinstance(image, dict)
        ],
        "Logic_path": (final or {}).get("logic") or [],
        "Manheim_comps": manheim_rows,
        "Market_scan": _public_market_scan_rows(evidence, ladder_rows),
        "Market_scan_status": _dossier_value(market.get("research_status") or {
            "attempted": bool(market),
            "grounded": bool(market.get("grounding")),
            "grounded_source_count": len(market.get("grounding") or []),
        }),
        "Value_ladder": ladder_rows,
    }


def _partial_andrew(evidence: dict, stage: str, ladder: dict) -> dict[str, Any]:
    mmr, _, _ = _extract_mmr_baseline(evidence)
    accutrade = _number(
        (((evidence.get("browser") or {}).get("accutrade") or {}).get("target_auction"))
    )
    return {
        "valuation": {"amount": None, "source": "pending"},
        "final_valuation_number": None,
        "text": "Evidence is being collected; valuation is not final.",
        "likely_range": {"low": None, "high": None},
        "confidence": None,
        "heat": None,
        "anchors": {
            "mmr": mmr,
            "accutrade": accutrade,
            "same_vin_public_ask": None,
        },
        "stage": stage,
        "Value_ladder": ladder.get("stages") or [],
    }


def _normalize_final(data: dict) -> dict[str, Any]:
    valuation = data.get("valuation") or {}
    amount = valuation.get("amount") if isinstance(valuation, dict) else valuation
    amount = _number(amount)
    if amount is None or amount <= 0:
        raise ValueError(f"Structured valuation has invalid amount: {amount!r}")

    data.setdefault("valuation", {})["amount"] = amount
    data["valuation"]["source"] = data["valuation"].get("source") or "prism_three_pass"

    low = _number((data.get("likely_range") or {}).get("low"))
    high = _number((data.get("likely_range") or {}).get("high"))
    if low is None or high is None:
        low, high = amount * 0.95, amount * 1.05
    if low > high:
        low, high = high, low
    data["likely_range"] = {
        "low": round(low / 100.0) * 100.0,
        "high": round(high / 100.0) * 100.0,
    }
    return data


def _apply_collector_anchors(final: dict, evidence: dict) -> dict:
    collector_value = _number(
        ((evidence.get("browser") or {}).get("accutrade") or {}).get("target_auction")
    )
    if collector_value is None or collector_value <= 0:
        return final

    anchors = final.setdefault("anchors", {})
    if _number(anchors.get("accutrade")) is None:
        anchors["accutrade"] = collector_value
    return final


def _enforce_public_research_availability(final: dict, evidence: dict) -> dict:
    """Prevent an empty web scan from being represented as grounded market evidence."""
    market = evidence.get("market_websearch") or {}
    if market.get("grounding"):
        return final

    final["accepted_public_comps"] = []
    anchors = final.setdefault("anchors", {})
    anchors["same_vin_public_ask"] = None
    confidence = final.setdefault("confidence", {})
    missing = list(confidence.get("missing_evidence") or [])
    message = "No grounded public-market sources were returned after the required retry."
    if message not in missing:
        missing.append(message)
    confidence["missing_evidence"] = missing
    basis = str(confidence.get("basis") or "").strip()
    confidence["basis"] = (
        (basis + " " if basis else "")
        + "Public-market claims were excluded because the web scan returned no grounded sources."
    )
    return final


def _has_canadian_policy_adjustment(ladder_rows: list[dict[str, Any]]) -> bool:
    for row in ladder_rows:
        if row.get("stage_id") != "history" or float(row.get("delta") or 0) == 0:
            continue
        if any(
            str(item.get("kind") or "") == LADDER_EVIDENCE_KINDS["CANADIAN_POLICY"]
            for item in (row.get("evidence") or [])
            if isinstance(item, dict)
        ):
            return True
    return False


def _remove_canadian_sentences(text: str) -> str:
    lines = []
    for line in str(text).splitlines():
        sentences = re.split(r"(?<=[.!?])\s+", line)
        kept = [
            sentence for sentence in sentences
            if not re.search(r"\bcanad(?:a|ian)\b", sentence, flags=re.IGNORECASE)
        ]
        if kept:
            lines.append(" ".join(kept))
    return "\n".join(lines).strip()


def _suppress_unused_canadian_mentions(value: Any) -> Any:
    if isinstance(value, str):
        return _remove_canadian_sentences(value)
    if isinstance(value, list):
        return [
            cleaned for item in value
            if (cleaned := _suppress_unused_canadian_mentions(item)) not in ("", None)
        ]
    if isinstance(value, dict):
        return {
            key: _suppress_unused_canadian_mentions(item)
            for key, item in value.items()
        }
    return value


# ---------------------------------------------------------------------------
# Persistence / orchestration
# ---------------------------------------------------------------------------


async def _persist_terminal_error(
    job_id: str,
    state: dict,
    state_lock: asyncio.Lock,
    persist_lock: asyncio.Lock,
    *,
    failed_stage: str,
    error: Exception | str,
) -> None:
    text = str(error)
    async with state_lock:
        state["status"] = "error"
        state["stage"] = "error"
        if text not in state["errors"]:
            state["errors"].append(text)
        ladder = _partial_ladder(state["evidence"], state["vin"])
        cards = _cards(state["evidence"], ladder.get("stages") or [])
        stream = {
            "status": "error",
            "stage": "error",
            "failed_stage": failed_stage,
            "error": text,
            "updated_at": _utc_now(),
            "evidence": copy.deepcopy(_jsonable(state["evidence"])),
            "collector_status": copy.deepcopy(_jsonable(state["collector_status"])),
            "last_collector_event": copy.deepcopy(_jsonable(state["last_collector_event"])),
            "cards": cards,
            "errors": list(state["errors"]),
            "workflow": "prism_cloudtask_three_pass_v4_live_ladder",
        }
        partial_andrew = _partial_andrew(state["evidence"], "error", ladder)
        partial_andrew["text"] = text

    async with persist_lock:
        await _job_update(
            job_id,
            vin=state["vin"],
            job_date=state["job_date"],
            car_details=json.dumps(state["car"], default=str),
            SA_stream=json.dumps(stream, default=str),
            Andrew_stream=json.dumps(partial_andrew, default=str),
            LPA_stream=json.dumps(ladder, default=str),
        )


async def run_prism_valuation(
    car_details: dict,
    job_id: str,
    *,
    job_date: Optional[str] = None,
) -> dict[str, Any]:
    """Run one valuation inside one active Cloud Tasks HTTP worker request."""

    job_id = str(job_id or "").strip()
    if not job_id:
        raise ValueError("job_id is required")

    car = dict(car_details or {})
    vin = _clean_vin(_first(car, "vin", "VIN", "Vin"))
    if not vin:
        raise ValueError("car_details must include VIN")
    car["vin"] = vin

    fixed_job_date = str(job_date or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
    state: dict[str, Any] = {
        "vin": vin,
        "job_id": job_id,
        "job_date": fixed_job_date,
        "car": car,
        "status": "running",
        "stage": "collecting",
        "evidence": {"manheim": {}},
        "collector_status": {},
        "last_collector_event": None,
        "errors": [],
    }

    state_lock = asyncio.Lock()
    persist_lock = asyncio.Lock()

    required_runtime = (
        "manheim_headers_provider",
        "manheim_current_mmr_collector",
        "manheim_historical_mmr_collector",
        "manheim_future_mmr_collector",
        "manheim_condition_collector",
        "manheim_vin_sales_collector",
        "manheim_ymm_sales_collector",
        "manheim_mm_sales_collector",
        "job_updater",
    )
    missing = [name for name in required_runtime if _RUNTIME.get(name) is None]
    if missing:
        raise RuntimeError(f"Prism valuation runtime missing: {', '.join(missing)}")

    async def get_manheim_headers():
        return await asyncio.wait_for(
            _call(_RUNTIME["manheim_headers_provider"]),
            timeout=MANHEIM_AUTH_TIMEOUT,
        )

    # Authorization is shared, but it is itself a task so GMTV/JD/browser do NOT wait
    # for Manheim authentication before starting.
    manheim_headers_task = asyncio.create_task(get_manheim_headers())
    target_jdpower_task = asyncio.create_task(_collect_jdpower(car))

    async def manheim_part(runtime_name: str):
        headers = await asyncio.shield(manheim_headers_task)
        return await _call(_RUNTIME[runtime_name], car, headers)

    async def gmtv_with_market_trend():
        target_jdpower = await asyncio.shield(target_jdpower_task)
        target_build = _jdpower_build(target_jdpower)
        async with state_lock:
            target_manheim_mmr = copy.deepcopy(
                (state["evidence"].get("manheim") or {}).get("current_mmr") or {}
            )
        gmtv_car = dict(car)
        decoded_model = str(target_build.get("model") or "").strip()
        if decoded_model:
            gmtv_car["input_model"] = _first(car, "model", "MODEL", "Model")
            gmtv_car["model"] = decoded_model
            gmtv_car["model_source"] = "jdpower"
        year_filter = await _select_gmtv_comparable_years(gmtv_car, target_jdpower)
        gmtv = await _collect_gmtv(
            gmtv_car,
            comparable_years=year_filter["comparable_years"],
            year_filter=year_filter,
        )
        if not gmtv.get("sales"):
            return gmtv
        headers = await asyncio.shield(manheim_headers_task)
        return await _enrich_gmtv_market_trend(
            gmtv,
            _RUNTIME["manheim_historical_mmr_collector"],
            headers,
            car=gmtv_car,
            jdpower_decoder=_collect_jdpower,
            target_jdpower=target_jdpower,
            target_manheim_mmr=target_manheim_mmr,
        )

    async def target_jdpower_result():
        return await asyncio.shield(target_jdpower_task)

    anchor_source_specs: dict[str, tuple[Awaitable[Any], float]] = {
        "manheim_current_mmr": (
            manheim_part("manheim_current_mmr_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_MMR_TIMEOUT,
        ),
        "browser": (_collect_browser(car), BROWSER_TIMEOUT + 5),
    }
    detail_source_specs: dict[str, tuple[Awaitable[Any], float]] = {
        "manheim_historical_mmr": (
            manheim_part("manheim_historical_mmr_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_MMR_TIMEOUT,
        ),
        "manheim_future_mmr": (
            manheim_part("manheim_future_mmr_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_MMR_TIMEOUT,
        ),
        "manheim_condition": (
            manheim_part("manheim_condition_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_CONDITION_TIMEOUT,
        ),
        "manheim_vin_sales": (
            manheim_part("manheim_vin_sales_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_SALES_TIMEOUT,
        ),
        "manheim_ymm_sales": (
            manheim_part("manheim_ymm_sales_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_SALES_TIMEOUT,
        ),
        "manheim_mm_sales": (
            manheim_part("manheim_mm_sales_collector"),
            MANHEIM_AUTH_TIMEOUT + MANHEIM_SALES_TIMEOUT,
        ),
        "gmtv": (
            gmtv_with_market_trend(),
            max(GMTV_TIMEOUT, MARKET_TREND_TIMEOUT),
        ),
        "jdpower": (target_jdpower_result(), JDPOWER_TIMEOUT + 2),
        "crm_photos": (_collect_crm_photos(car), CRM_PHOTOS_TIMEOUT),
    }
    source_specs = {**anchor_source_specs, **detail_source_specs}

    now = _utc_now()
    for name in source_specs:
        state["collector_status"][name] = (
            {"state": "running", "started_at": now}
            if name in anchor_source_specs
            else {"state": "pending", "waiting_for": "MMR and AccuTrade Target Auction"}
        )
    state["collector_status"]["manheim"] = _manheim_parent_status(state["collector_status"])

    def merge_evidence(name: str, result: Any) -> None:
        result = _jsonable(result)
        manheim_key_map = {
            "manheim_current_mmr": "current_mmr",
            "manheim_historical_mmr": "historical_mmr",
            "manheim_future_mmr": "future_mmr",
            "manheim_condition": "condition_report",
            "manheim_vin_sales": "live_sale",
            "manheim_ymm_sales": "comp_sales",
            "manheim_mm_sales": "all_sales",
        }

        if name in manheim_key_map:
            state["evidence"].setdefault("manheim", {})[manheim_key_map[name]] = result
        else:
            if not isinstance(result, dict):
                result = {"data": result}
            state["evidence"][name] = result

        errors: list[str] = []
        if isinstance(result, dict):
            errors.extend(str(x) for x in (result.get("errors") or []) if x)
            if result.get("error"):
                errors.append(str(result.get("error")))

        if errors and name in manheim_key_map:
            manheim = state["evidence"].setdefault("manheim", {})
            manheim["errors"] = list(dict.fromkeys(list(manheim.get("errors") or []) + errors))

        for message in errors:
            if message not in state["errors"]:
                state["errors"].append(message)

    async def persist_snapshot(stage: Optional[str] = None, **extra_fields) -> None:
        # Evidence calls run outside this lock. Only the short DB write is serialized.
        async with persist_lock:
            async with state_lock:
                if stage:
                    state["stage"] = stage
                state["collector_status"]["manheim"] = _manheim_parent_status(
                    state["collector_status"]
                )

                ladder = _partial_ladder(state["evidence"], vin)
                cards = _cards(state["evidence"], ladder.get("stages") or [])
                partial_andrew = _partial_andrew(state["evidence"], state["stage"], ladder)
                stream = {
                    "status": state["status"],
                    "stage": state["stage"],
                    "updated_at": _utc_now(),
                    "evidence": copy.deepcopy(_jsonable(state["evidence"])),
                    "collector_status": copy.deepcopy(_jsonable(state["collector_status"])),
                    "last_collector_event": copy.deepcopy(_jsonable(state["last_collector_event"])),
                    "cards": cards,
                    "errors": list(state["errors"]),
                    "workflow": "prism_cloudtask_three_pass_v4_live_ladder",
                }

            fields = {
                "vin": vin,
                # Never replace this with collector completion time.
                "job_date": fixed_job_date,
                "car_details": json.dumps(car, default=str),
                "SA_stream": json.dumps(stream, default=str),
                "Andrew_stream": json.dumps(partial_andrew, default=str),
                "LPA_stream": json.dumps(ladder, default=str),
                **extra_fields,
            }
            ok = await _job_update(job_id, **fields)
            if not ok:
                raise RuntimeError(f"Could not persist valuation job {job_id}")

    async def run_source(name: str, awaitable: Awaitable[Any], timeout: float) -> dict[str, Any]:
        started = time.monotonic()
        status = "complete"
        try:
            result = await asyncio.wait_for(awaitable, timeout=timeout)
            if not isinstance(result, dict):
                result = {"data": _jsonable(result)}
            if result.get("errors") or result.get("error"):
                status = "error"
        except asyncio.TimeoutError:
            result = {"errors": [f"{name} exceeded {timeout:.0f}s"]}
            status = "error"
        except Exception as error:
            result = {"errors": [f"{name} failed: {type(error).__name__}: {error}"]}
            status = "error"

        elapsed = round(time.monotonic() - started, 3)
        async with state_lock:
            merge_evidence(name, result)
            state["collector_status"][name] = {
                **(state["collector_status"].get(name) or {}),
                "state": status,
                "completed_at": _utc_now(),
                "elapsed_seconds": elapsed,
                "errors": [str(x) for x in (result.get("errors") or []) if x],
            }
            if result.get("stage_status"):
                state["collector_status"][name]["stages"] = copy.deepcopy(
                    _jsonable(result["stage_status"])
                )
            state["collector_status"]["manheim"] = _manheim_parent_status(state["collector_status"])
            state["last_collector_event"] = {
                "collector": name,
                "state": status,
                "at": _utc_now(),
                "elapsed_seconds": elapsed,
            }

        # This is the requested behavior: as soon as evidence lands, the durable job
        # contains it. Other evidence requests continue concurrently while this write runs.
        await persist_snapshot("collecting")
        return result

    try:
        await persist_snapshot("collecting")
        anchor_tasks = {
            name: asyncio.create_task(run_source(name, awaitable, timeout))
            for name, (awaitable, timeout) in anchor_source_specs.items()
        }
        await asyncio.gather(*anchor_tasks.values())
        await persist_snapshot("anchor_collection_complete")

        async with state_lock:
            detail_started_at = _utc_now()
            for name in detail_source_specs:
                state["collector_status"][name] = {
                    "state": "running",
                    "started_at": detail_started_at,
                }
        await persist_snapshot("collecting_vehicle_details_and_history")

        detail_tasks = {
            name: asyncio.create_task(run_source(name, awaitable, timeout))
            for name, (awaitable, timeout) in detail_source_specs.items()
        }
        await asyncio.gather(*detail_tasks.values())

        async with state_lock:
            evidence = copy.deepcopy(state["evidence"])

        # Pass 1: agent-curated Google Search using Prism's existing GoogleSearch tool.
        await persist_snapshot("web_research")
        market = await _pass1_websearch(car, evidence)
        async with state_lock:
            state["evidence"]["market_websearch"] = _jsonable(market)
        await persist_snapshot(
            "web_research_complete",
            Web_searches=json.dumps(market.get("grounding") or [], default=str),
        )

        dossier = _build_evidence_dossier(state["evidence"], market)

        # Pass 2: valuation logic + explicit ladder.
        await persist_snapshot("valuation_reasoning")
        reasoning = await _pass2_reasoning(car, state["evidence"], market)

        # Pass 3: independently audit adjustments and emit the final API schema.
        await persist_snapshot("adjustment_audit", Prompt=reasoning[:100_000])
        audited = await _audit_adjustments(reasoning, dossier)
        audit = {"adjustments": audited["adjustments"]}
        async with state_lock:
            state["evidence"]["adjustment_audit"] = _jsonable(audit)

        final = _normalize_final(audited["final"])
        final = _enforce_public_research_availability(final, state["evidence"])
        final = _apply_collector_anchors(final, state["evidence"])

        rows, ladder_value, baseline_source = _normalize_final_ladder(final, state["evidence"], vin)

        if not _has_canadian_policy_adjustment(rows):
            final = _suppress_unused_canadian_mentions(final)
            rows = _suppress_unused_canadian_mentions(rows)

        # The audited ladder is authoritative. Model-mapped running values are
        # presentation fields and cannot override the selected baseline plus audited deltas.
        ladder_value = round(float(ladder_value), 2)
        amount = _publish_final_ladder(final, rows, ladder_value)

        low = _number((final.get("likely_range") or {}).get("low"))
        high = _number((final.get("likely_range") or {}).get("high"))
        if low is not None and high is not None:
            final["likely_range"] = {
                "low": min(low, amount),
                "high": max(high, amount),
            }

        final["valuation"]["source"] = final["valuation"].get("source") or "prism_three_pass"
        final["market_trend"] = copy.deepcopy(
            ((state["evidence"].get("gmtv") or {}).get("market_trend") or {})
        )

        cards = _cards(state["evidence"], rows, final)
        final["cards"] = cards
        ladder = {
            "vin": vin,
            "baseline_source": baseline_source,
            "complete": True,
            "final_value": ladder_value,
            "rounded_value": amount,
            "stages": rows,
            "implausible": False,
            "implausible_reason": "",
        }

        async with state_lock:
            state["status"] = "complete"
            state["stage"] = "complete"
            stream = {
                "status": "complete",
                "stage": "complete",
                "updated_at": _utc_now(),
                "evidence": copy.deepcopy(_jsonable(state["evidence"])),
                "collector_status": copy.deepcopy(_jsonable(state["collector_status"])),
                "last_collector_event": copy.deepcopy(_jsonable(state["last_collector_event"])),
                "cards": cards,
                "errors": list(state["errors"]),
                "workflow": "prism_cloudtask_three_pass_v4_live_ladder",
            }

        async with persist_lock:
            ok = await _job_update(
                job_id,
                vin=vin,
                job_date=fixed_job_date,
                car_details=json.dumps(car, default=str),
                SA_stream=json.dumps(stream, default=str),
                Andrew_stream=json.dumps(final, default=str),
                LBA_stream=json.dumps(final, default=str),
                LPA_stream=json.dumps(ladder, default=str),
                Web_searches=json.dumps(market.get("grounding") or [], default=str),
                Prompt=reasoning[:100_000],
            )
            if not ok:
                raise RuntimeError(f"Could not persist final valuation job {job_id}")

        return {
            "job_id": job_id,
            "vin": vin,
            "predicted_hammer": amount,
            "valuation": final,
            "collector_errors": list(state["errors"]),
            "workflow": "prism_cloudtask_three_pass_v4_live_ladder",
        }

    except Exception as error:
        logger.exception("Prism valuation failed job=%s vin=%s", job_id, vin)
        await _persist_terminal_error(
            job_id,
            state,
            state_lock,
            persist_lock,
            failed_stage=state.get("stage") or "valuation",
            error=f"{type(error).__name__}: {error}",
        )
        return {"error": f"{type(error).__name__}: {error}", "job_id": job_id, "vin": vin}