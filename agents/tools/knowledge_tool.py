import os
import re
import json
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from utils.logging import logger, debug_enabled

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
KNOWLEDGE_BASE_ID = os.getenv("BEDROCK_KB_ID")
TENANT_ID = (os.getenv("TENANT_ID") or "").strip()

client = boto3.client("bedrock-agent-runtime", region_name=REGION)

TOP_K_DEFAULT = 5
MAX_CHARS_PER_CHUNK = 1200
MAX_TOTAL_CONTEXT_CHARS = 4500

# mismatch gating
MIN_KEYWORDS_REQUIRED = 1


def _keywords(query: str) -> list[str]:
    words = re.findall(r"[a-zA-Z0-9]{3,}", (query or "").lower())
    stop = {
        "what", "tell", "about", "are", "the", "and", "with", "from",
        "this", "that", "your", "you", "how", "can", "could", "please"
    }
    return [w for w in words if w not in stop][:12]


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()


def _content_shape(obj) -> str:
    # tiny non-invasive schema hint
    if isinstance(obj, dict):
        return "dict:" + ",".join(list(obj.keys())[:12])
    if isinstance(obj, list):
        return f"list(len={len(obj)})"
    return type(obj).__name__


def _find_first_string(obj, *, max_depth: int = 5, max_len: int = 20000) -> str | None:
    """
    Recursively search for the first meaningful string in a nested structure.
    This handles BDA / multimodal schemas where text isn't at content.text.
    """
    if obj is None or max_depth < 0:
        return None

    if isinstance(obj, str):
        s = re.sub(r"\s+", " ", obj).strip()
        if len(s) >= 20:  # ignore tiny junk strings
            return s[:max_len]
        return None

    if isinstance(obj, dict):
        # Prefer likely keys first if present
        for key in ("text", "extractedText", "value", "body", "content", "transcript"):
            if key in obj:
                found = _find_first_string(obj.get(key), max_depth=max_depth - 1, max_len=max_len)
                if found:
                    return found

        # Otherwise scan remaining keys
        for _, v in obj.items():
            found = _find_first_string(v, max_depth=max_depth - 1, max_len=max_len)
            if found:
                return found

    if isinstance(obj, list):
        for item in obj[:25]:  # bound scan
            found = _find_first_string(item, max_depth=max_depth - 1, max_len=max_len)
            if found:
                return found

    return None


def retrieve_knowledge_base(query: str, top_k: int = TOP_K_DEFAULT) -> str:
    """
    Tenant-locked retrieval from Bedrock Knowledge Base. use this tool anytime you need to search something outside of scope as the knowledge base might have it.

    """
    if debug_enabled():
        logger.info(
            "tool.call",
            tool="retrieve_knowledge_base",
            query_preview=(query or "")[:120],
            top_k=top_k,
        )
    if not KNOWLEDGE_BASE_ID:
        return json.dumps({"error": "Missing BEDROCK_KB_ID in env"}, ensure_ascii=False)
    if not TENANT_ID:
        return json.dumps({"error": "Missing TENANT_ID in env (e.g., TENANT_ID=EBM)"}, ensure_ascii=False)

    q_keys = _keywords(query)
    tenant_filter = {"equals": {"key": "tenantId", "value": TENANT_ID}}

    try:
        resp = client.retrieve(
            knowledgeBaseId=KNOWLEDGE_BASE_ID,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": int(top_k),
                    "filter": tenant_filter,
                }
            },
        )

        results = resp.get("retrievalResults", []) or []
        top_score = (results[0].get("score") if results else None)

        chunks = []
        sources = []
        context_parts = []
        total_chars = 0
        keyword_hits_total = 0
        extracted_count = 0

        # For debugging schema without dumping everything:
        first_content_shape = None

        for r in results:
            content = r.get("content")
            if first_content_shape is None:
                first_content_shape = _content_shape(content)

            # Recursively find a usable text snippet anywhere in the result
            raw = _find_first_string(content) or _find_first_string(r)
            if not raw:
                continue

            extracted_count += 1
            snippet = re.sub(r"\s+", " ", raw).strip()

            if len(snippet) > MAX_CHARS_PER_CHUNK:
                snippet = snippet[:MAX_CHARS_PER_CHUNK] + "…"

            norm = _normalize(snippet)
            hits = sum(1 for k in q_keys if k in norm)
            keyword_hits_total += hits

            remaining = MAX_TOTAL_CONTEXT_CHARS - total_chars
            if remaining <= 0:
                break
            if len(snippet) > remaining:
                snippet = snippet[:remaining] + "…"

            loc = r.get("location") or {}
            source_uri = None
            if isinstance(loc, dict):
                source_uri = (loc.get("s3Location") or {}).get("uri")
            if source_uri and source_uri not in sources:
                sources.append(source_uri)

            chunks.append({
                "text": snippet,
                "score": r.get("score"),
                "source_uri": source_uri,
                "keyword_hits": hits
            })
            context_parts.append(snippet)
            total_chars += len(snippet)

        context = "\n\n---\n\n".join(context_parts)

        # If we literally couldn't extract text, return a clear error-like payload
        if extracted_count == 0:
            return json.dumps({
                "tenantId": TENANT_ID,
                "query": query,
                "no_relevant_context": True,
                "reason": "Retrieved results but could not extract any text from them (schema differs).",
                "top_score": top_score,
                "context": "",
                "chunks": [],
                "sources": sources,
                "debug": {
                    "query_keywords": q_keys,
                    "results_returned": len(results),
                    "results_extracted_text": extracted_count,
                    "chunks_kept": len(chunks),
                    "keyword_hits_total": keyword_hits_total,
                    "content_shape_first": first_content_shape
                }
            }, ensure_ascii=False)

        # Hard mismatch rule
        if len(q_keys) >= MIN_KEYWORDS_REQUIRED and keyword_hits_total == 0:
            return json.dumps({
                "tenantId": TENANT_ID,
                "query": query,
                "no_relevant_context": True,
                "reason": "No keyword overlap between query and retrieved context",
                "top_score": top_score,
                "context": "",
                "chunks": chunks[:2],  # small debug sample
                "sources": sources,
                "debug": {
                    "query_keywords": q_keys,
                    "results_returned": len(results),
                    "results_extracted_text": extracted_count,
                    "chunks_kept": len(chunks),
                    "keyword_hits_total": keyword_hits_total,
                    "content_shape_first": first_content_shape
                }
            }, ensure_ascii=False)

        return json.dumps({
            "tenantId": TENANT_ID,
            "query": query,
            "no_relevant_context": False,
            "top_score": top_score,
            "context": context,
            "chunks": chunks,
            "sources": sources,
            "debug": {
                "query_keywords": q_keys,
                "results_returned": len(results),
                "results_extracted_text": extracted_count,
                "chunks_kept": len(chunks),
                "keyword_hits_total": keyword_hits_total,
                "content_shape_first": first_content_shape
            },
            "note": "Feed `context` into Gemini as grounding. If no_relevant_context=true, request relevant docs."
        }, ensure_ascii=False)

    except ClientError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": f"Unexpected error: {e}"}, ensure_ascii=False)
