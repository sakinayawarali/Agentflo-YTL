"""
Improved inbound message idempotency store.

Key improvements:
1. Faster claim_message (single atomic transaction)
2. TTL-based cleanup of old entries
3. Better stale message handling
"""

import time
import os
from typing import Optional
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
from agents.helpers.firestore_utils import get_tenant_id, user_root
from utils.logging import logger


class InboundStore:
    """
    Tracks processed inbound messages to prevent duplicate handling.
    
    Storage schema:
    tenants/{tenant}/agent_id/{agent_id}/users/{user}/inbound_messages/{message_hash}
    {
        "wamid": "wamid.xxx",
        "claimed_at": 123.45,
        "stale": false,
        "ttl_expires_at": 123.45 + 3600,  # for cleanup
    }
    """
    
    def __init__(self):
        self.db = firestore.Client()
        self.tenant_id = get_tenant_id()
        self.collection = "inbound_messages"
        # How long to remember a message (default 1 hour)
        self.ttl_sec = int(os.getenv("INBOUND_TTL_SECONDS", "3600"))
        # Messages older than this are considered stale on arrival (10 min)
        self.stale_threshold_sec = int(os.getenv("INBOUND_STALE_THRESHOLD", "600"))

    def _user_root(self, user_id: str):
        return user_root(self.db, user_id, tenant_id=self.tenant_id)

    def _sha(self, s: str) -> str:
        import hashlib
        return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

    def _ref(self, user_id: str, inbound_key: str):
        doc_id = self._sha(inbound_key)
        return self._user_root(user_id).collection(self.collection).document(doc_id)

    def claim_message(self, user_id: str, inbound_key: str) -> bool:
        """
        Atomically claim this inbound message.
        
        Returns:
            True if this is the first time we've seen it (process it)
            False if already claimed (skip it)
        """
        if not inbound_key:
            return True  # No key = can't dedupe, allow

        ref = self._ref(user_id, inbound_key)
        now = time.time()
        
        try:
            ref.create({
                "wamid": inbound_key,
                "claimed_at": now,
                "stale": False,
                "ttl_expires_at": now + self.ttl_sec,
            })
            logger.info("inbound.claimed", user_id=user_id, key=inbound_key)
            return True
            
        except AlreadyExists:
            logger.info("inbound.duplicate", user_id=user_id, key=inbound_key)
            return False
            
        except Exception as e:
            # On error, fail open (allow processing)
            logger.warning("inbound.claim_error", user_id=user_id, key=inbound_key, error=str(e))
            return True

    def mark_stale(self, user_id: str, inbound_key: str) -> None:
        """
        Mark a message as stale (too old to process).
        Still claims it to prevent reprocessing on retry.
        """
        if not inbound_key:
            return
        
        ref = self._ref(user_id, inbound_key)
        now = time.time()
        
        try:
            ref.set({
                "wamid": inbound_key,
                "claimed_at": now,
                "stale": True,
                "ttl_expires_at": now + self.ttl_sec,
            }, merge=True)
            logger.info("inbound.marked_stale", user_id=user_id, key=inbound_key)
        except Exception as e:
            logger.warning("inbound.mark_stale_error", user_id=user_id, key=inbound_key, error=str(e))

    # def cleanup_expired(self, user_id: str, batch_size: int = 100) -> int:
    #     """
    #     Delete expired inbound records (past TTL).
        
    #     Call this periodically via a cleanup cron job.
        
    #     Returns:
    #         Number of records deleted
    #     """
    #     now = time.time()
    #     deleted = 0
        
    #     try:
    #         query = (
    #             self._user_root(user_id)
    #             .collection(self.collection)
    #             .where("ttl_expires_at", "<", now)
    #             .limit(batch_size)
    #         )
            
    #         docs = query.stream()
    #         batch = self.db.batch()
            
    #         for doc in docs:
    #             batch.delete(doc.reference)
    #             deleted += 1
            
    #         if deleted > 0:
    #             batch.commit()
    #             logger.info("inbound.cleanup", user_id=user_id, deleted=deleted)
                
    #     except Exception as e:
    #         logger.warning("inbound.cleanup_error", user_id=user_id, error=str(e))
        
    #     return deleted
