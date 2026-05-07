"""MongoDB client and aggregation helpers."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from bson import ObjectId
from django.conf import settings
from pymongo import DESCENDING, MongoClient
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_SENTIMENT_MAP = {0: "negative", 1: "neutral", 2: "positive"}


class MongoUnavailable(Exception):
    """Raised when MongoDB cannot be reached."""


_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return a cached MongoDB client."""
    global _client
    if _client is None:
        uri = settings.MONGO_URI or ""
        if not uri or "27017" in uri or "mongo:" in uri:
            uri = "mongodb://localhost:27018"
        logger.info("Connecting Mongo with URI=%s", uri)
        _client = MongoClient(uri, serverSelectionTimeoutMS=2000)
    return _client


def get_collection():
    """Return the predictions collection."""
    return get_client()[settings.MONGO_DB]["predictions"]


def _safe(fn):
    """Decorator that converts pymongo errors into MongoUnavailable."""

    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except PyMongoError as exc:
            logger.warning("Mongo error in %s: %s", fn.__name__, exc)
            raise MongoUnavailable(str(exc)) from exc

    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    return wrapper


def _serialize(doc: dict) -> dict:
    """Convert a Mongo document into JSON-safe primitives."""
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _format_prediction(doc: dict) -> dict:
    """Map a raw Mongo document to the frontend-friendly shape."""
    sentiment = doc.get("sentiment_label")
    if not sentiment:
        sentiment = _SENTIMENT_MAP.get(int(doc.get("prediction") or 0), "neutral")
    time_value = doc.get("Time")
    time_str = ""
    if isinstance(time_value, (int, float)):
        ts = int(time_value)
        # Heuristic: if value looks like milliseconds (> year 2100 in seconds), divide
        if ts > 4_000_000_000:
            ts = ts // 1000
        # Sanity check: only accept timestamps between 1995-01-01 and 2030-01-01
        if 788_918_400 <= ts <= 1_893_456_000:
            time_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
    confidence = doc.get("confidence")
    try:
        confidence = float(confidence) if confidence is not None else None
    except (TypeError, ValueError):
        confidence = None
    return {
        "id": doc.get("Id") or str(doc.get("_id")),
        "_id": str(doc.get("_id")),
        "productId": doc.get("ProductId", ""),
        "userId": doc.get("UserId", ""),
        "summary": doc.get("Summary", ""),
        "text": doc.get("Text", ""),
        "time": time_str,
        "sentiment": sentiment,
        "confidence": confidence,
        "trueSentiment": doc.get("true_sentiment"),
        "prediction": doc.get("prediction"),
    }


@_safe
def get_kpi_stats() -> dict[str, Any]:
    """Return KPI metrics for the dashboard cards."""
    coll = get_collection()
    total = coll.estimated_document_count()
    unique_products = len(coll.distinct("ProductId"))

    now = datetime.utcnow()
    one_minute_ago = datetime.utcfromtimestamp(now.timestamp() - 60)
    recent = coll.count_documents({"inserted_at": {"$gte": one_minute_ago}})
    throughput = round(recent / 60.0, 2)

    return {
        "totalPredictions": total,
        "throughput": throughput,
        "uniqueProducts": unique_products,
        "f1Score": 82.74,
    }


@_safe
def get_global_distribution() -> dict[str, int]:
    """Return total counts per sentiment label."""
    coll = get_collection()
    pipeline = [{"$group": {"_id": "$sentiment_label", "n": {"$sum": 1}}}]
    out = {"positive": 0, "neutral": 0, "negative": 0}
    for row in coll.aggregate(pipeline):
        label = row.get("_id")
        if label in out:
            out[label] = row["n"]
    return out


@_safe
def get_yearly_trend() -> list[dict[str, Any]]:
    """Return yearly aggregation of sentiment counts."""
    coll = get_collection()
    pipeline = [
        {"$match": {"Time": {"$gte": 788918400, "$lte": 1893456000}}},
        {"$addFields": {"date": {"$toDate": {"$multiply": ["$Time", 1000]}}}},
        {
            "$group": {
                "_id": {"year": {"$year": "$date"}, "label": "$sentiment_label"},
                "n": {"$sum": 1},
            }
        },
    ]
    rows = list(coll.aggregate(pipeline))
    by_year: dict[int, dict[str, int]] = {}
    for r in rows:
        y = r["_id"]["year"]
        label = r["_id"]["label"] or "neutral"
        by_year.setdefault(y, {"positive": 0, "neutral": 0, "negative": 0})
        if label in by_year[y]:
            by_year[y][label] = r["n"]
    return [
        {"year": str(y), **by_year[y]}
        for y in sorted(by_year)
    ]


@_safe
def get_monthly_trend(year: int) -> list[dict[str, Any]]:
    """Return monthly breakdown for a given year."""
    coll = get_collection()
    pipeline = [
        {"$match": {"Time": {"$gte": 788918400, "$lte": 1893456000}}},
        {"$addFields": {"date": {"$toDate": {"$multiply": ["$Time", 1000]}}}},
        {"$match": {"date": {
            "$gte": datetime(year, 1, 1),
            "$lt": datetime(year + 1, 1, 1),
        }}},
        {
            "$group": {
                "_id": {"month": {"$month": "$date"}, "label": "$sentiment_label"},
                "n": {"$sum": 1},
            }
        },
    ]
    rows = list(coll.aggregate(pipeline))
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    by_month: dict[int, dict[str, int]] = {
        i + 1: {"positive": 0, "neutral": 0, "negative": 0} for i in range(12)
    }
    for r in rows:
        m = r["_id"]["month"]
        label = r["_id"]["label"] or "neutral"
        if label in by_month[m]:
            by_month[m][label] = r["n"]
    return [{"month": months[m - 1], **by_month[m]} for m in range(1, 13)]


@_safe
def get_top_products(sentiment: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    """Return top product ids by count, optionally filtered by sentiment."""
    coll = get_collection()
    match: dict[str, Any] = {}
    if sentiment and sentiment != "all":
        match["sentiment_label"] = sentiment
    pipeline = []
    if match:
        pipeline.append({"$match": match})
    pipeline += [
        {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]
    return [{"id": r["_id"], "count": r["count"]} for r in coll.aggregate(pipeline)]


_WORD_STOPWORDS = {
    "the","and","for","with","this","that","from","have","has","had","not","but","you",
    "are","was","were","its","their","they","them","there","here","into","also","just",
    "very","much","more","most","some","any","all","one","two","get","got","than","then",
    "out","our","your","his","her","him","she","could","would","should","about","because",
    "what","when","where","which","who","why","how","like","really","made","make","makes",
    "amazon","product","item","ordered","order","bought","buy","received","received",
    "good","great","love","loved","best","nice","perfect","awesome","excellent","tasty",
    "bad","worst","horrible","terrible","awful","disappointed","disappointing",
}


@_safe
def get_word_frequencies(limit: int = 25) -> dict[str, list[dict[str, Any]]]:
    """Return top frequent words per sentiment class from cleaned text."""
    coll = get_collection()
    out: dict[str, list[dict[str, Any]]] = {"positive": [], "neutral": [], "negative": []}
    for label in out.keys():
        pipeline = [
            {"$match": {"sentiment_label": label}},
            {"$sample": {"size": 5000}},
            {"$addFields": {"_src": {"$cond": {
                "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$cleaned", ""]}}, 2]},
                "then": "$cleaned",
                "else": {"$ifNull": ["$Text", ""]},
            }}}},
            {"$project": {"tokens": {"$split": [{"$toLower": "$_src"}, " "]}}},
            {"$unwind": "$tokens"},
            {"$match": {"tokens": {"$nin": list(_WORD_STOPWORDS)}}},
            {"$match": {"tokens": {"$regex": "^[a-z]{3,15}$"}}},
            {"$group": {"_id": "$tokens", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}},
            {"$limit": int(limit)},
        ]
        out[label] = [{"text": r["_id"], "value": r["n"]} for r in coll.aggregate(pipeline)]
    return out


@_safe
def get_all_products(limit: int = 2000) -> list[dict[str, Any]]:
    """Return all distinct product IDs with their review counts."""
    coll = get_collection()
    pipeline = [
        {"$group": {"_id": "$ProductId", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": int(limit)},
    ]
    return [{"id": r["_id"], "count": r["count"]} for r in coll.aggregate(pipeline) if r["_id"]]


@_safe
def get_confusion_matrix() -> list[list[int]]:
    """Return a 3x3 confusion matrix (rows=true, cols=predicted)."""
    coll = get_collection()
    pipeline = [
        {"$match": {"true_sentiment": {"$in": [0, 1, 2]}, "prediction": {"$ne": None}}},
        {
            "$group": {
                "_id": {"t": "$true_sentiment", "p": {"$toInt": "$prediction"}},
                "n": {"$sum": 1},
            }
        },
    ]
    matrix = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
    for r in coll.aggregate(pipeline):
        t = int(r["_id"]["t"])
        p = int(r["_id"]["p"])
        if 0 <= t <= 2 and 0 <= p <= 2:
            matrix[t][p] = r["n"]
    # Frontend uses positive,neutral,negative ordering
    order = [2, 1, 0]
    return [[matrix[i][j] for j in order] for i in order]


@_safe
def get_confidence_distribution() -> dict[str, Any]:
    """Return histogram bins of confidence per sentiment."""
    coll = get_collection()
    bins = [0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95]
    result = {"bins": bins, "positive": [0] * len(bins),
              "neutral": [0] * len(bins), "negative": [0] * len(bins)}
    pipeline = [
        {"$match": {"sentiment_label": {"$in": ["positive", "neutral", "negative"]}}},
        {
            "$group": {
                "_id": {
                    "label": "$sentiment_label",
                    "bin": {
                        "$switch": {
                            "branches": [
                                {"case": {"$lt": ["$confidence", 0.55]}, "then": 0},
                                {"case": {"$lt": ["$confidence", 0.6]}, "then": 1},
                                {"case": {"$lt": ["$confidence", 0.65]}, "then": 2},
                                {"case": {"$lt": ["$confidence", 0.7]}, "then": 3},
                                {"case": {"$lt": ["$confidence", 0.75]}, "then": 4},
                                {"case": {"$lt": ["$confidence", 0.8]}, "then": 5},
                                {"case": {"$lt": ["$confidence", 0.85]}, "then": 6},
                                {"case": {"$lt": ["$confidence", 0.9]}, "then": 7},
                                {"case": {"$lt": ["$confidence", 0.95]}, "then": 8},
                            ],
                            "default": 9,
                        }
                    },
                },
                "n": {"$sum": 1},
            }
        },
    ]
    for r in coll.aggregate(pipeline):
        label = r["_id"]["label"]
        b = r["_id"]["bin"]
        if label in result and 0 <= b < len(bins):
            result[label][b] = r["n"]
    return result


@_safe
def get_recent_predictions(limit: int = 50, sentiment: str | None = None) -> list[dict]:
    """Return most recent predictions sorted by inserted_at desc."""
    coll = get_collection()
    query: dict[str, Any] = {}
    if sentiment and sentiment != "all":
        query["sentiment_label"] = sentiment
    cursor = coll.find(query).sort("inserted_at", DESCENDING).limit(int(limit))
    return [_format_prediction(d) for d in cursor]


@_safe
def get_predictions_after(object_id: str | None, limit: int = 20) -> list[dict]:
    """Return predictions inserted strictly after the given ObjectId."""
    coll = get_collection()
    query: dict[str, Any] = {}
    if object_id:
        try:
            query["_id"] = {"$gt": ObjectId(object_id)}
        except Exception:
            pass
    cursor = coll.find(query).sort("_id", DESCENDING).limit(int(limit))
    return [_format_prediction(d) for d in cursor]


@_safe
def get_product_detail(product_id: str) -> dict:
    """Return aggregated detail for a single product."""
    coll = get_collection()
    base = {"ProductId": product_id}
    total = coll.count_documents(base)
    if total == 0:
        return {}
    counts = {"positive": 0, "neutral": 0, "negative": 0}
    for r in coll.aggregate([
        {"$match": base},
        {"$group": {"_id": "$sentiment_label", "n": {"$sum": 1}}},
    ]):
        if r["_id"] in counts:
            counts[r["_id"]] = r["n"]

    yearly: dict[int, dict[str, int]] = {}
    for r in coll.aggregate([
        {"$match": {**base, "Time": {"$gte": 788918400, "$lte": 1893456000}}},
        {"$addFields": {"date": {"$toDate": {"$multiply": ["$Time", 1000]}}}},
        {"$group": {"_id": {"y": {"$year": "$date"}, "l": "$sentiment_label"}, "n": {"$sum": 1}}},
    ]):
        y = r["_id"]["y"]
        label = r["_id"]["l"] or "neutral"
        yearly.setdefault(y, {"positive": 0, "neutral": 0, "negative": 0})
        if label in yearly[y]:
            yearly[y][label] = r["n"]
    yearly_list = [{"year": str(y), **yearly[y]} for y in sorted(yearly)]

    reviews_cursor = coll.find(base).sort("Time", DESCENDING).limit(20)
    reviews = [_format_prediction(d) for d in reviews_cursor]

    return {
        "id": product_id,
        "totalReviews": total,
        **counts,
        "yearlyBreakdown": yearly_list,
        "reviews": reviews,
    }


@_safe
def get_product_reviews(
    product_id: str, sentiment: str | None = None, page: int = 1, page_size: int = 20
) -> dict:
    """Return paginated reviews for a product."""
    coll = get_collection()
    query: dict[str, Any] = {"ProductId": product_id}
    if sentiment and sentiment not in ("all", ""):
        query["sentiment_label"] = sentiment
    page = max(1, int(page))
    skip = (page - 1) * page_size
    total = coll.count_documents(query)
    cursor = coll.find(query).sort("Time", DESCENDING).skip(skip).limit(page_size)
    return {
        "page": page,
        "pageSize": page_size,
        "total": total,
        "reviews": [_format_prediction(d) for d in cursor],
    }


@_safe
def search_predictions(query: str, limit: int = 20) -> list[dict]:
    """Return predictions whose Summary or Text matches the query."""
    coll = get_collection()
    if not query:
        return []
    regex = {"$regex": query, "$options": "i"}
    cursor = coll.find({"$or": [{"Summary": regex}, {"Text": regex}]}).limit(int(limit))
    return [_format_prediction(d) for d in cursor]


@_safe
def ping() -> bool:
    """Return True if MongoDB is reachable."""
    get_client().admin.command("ping")
    return True
