import os
import time
from typing import Optional, Dict, List, Tuple
from google.cloud import firestore
from google.api_core import exceptions as gexc
from agents.helpers.firestore_utils import get_tenant_id, user_root
from utils.logging import logger

class MessageBufferStore:
    def __init__(self):
        self.db = firestore.Client()
        self.tenant_id = get_tenant_id()
        self.collection = "message_buffer"

    def _user_root(self, user_id: str):
        return user_root(self.db, user_id, tenant_id=self.tenant_id)

    def _ref(self, user_id: str):
        return self._user_root(user_id).collection(self.collection).document("buffer")

    def append_message(
        self,
        user_id: str,
        text: str,
        reply_to_message_id: Optional[str] = None,
        inbound_message_id: Optional[str] = None,
        source_kind: str = "text",
        conversation_id: Optional[str] = None,  # NEW
        session_id: Optional[str] = None,        # NEW
    ) -> int:
        """
        Appends a message and increments the generation ID.
        Returns the new 'generation' ID.
        """
        ref = self._ref(user_id)
        
        @firestore.transactional
        def _txn_append(transaction):
            snap = ref.get(transaction=transaction)
            data = snap.to_dict() if snap.exists else {}

            messages = data.get("messages", [])
            current_gen = int(data.get("generation", 0))

            new_msg = {
                "text": text,
                "ts": time.time(),
                "reply_to": reply_to_message_id,
                "message_id": inbound_message_id,
                "source": source_kind
            }
            messages.append(new_msg)
            
            new_gen = current_gen + 1

            # NEW: Store context metadata for the entire buffer
            transaction.set(ref, {
                "messages": messages,
                "generation": new_gen,
                "updated_at": time.time(),
                "conversation_id": conversation_id,  # NEW
                "session_id": session_id,            # NEW
            }, merge=True)
            
            return new_gen

        return _txn_append(self.db.transaction())


    def get_metadata(self, user_id: str) -> dict:
        """Lightweight check to see the current generation ID."""
        snap = self._ref(user_id).get()
        return snap.to_dict() or {}

    def pop_all(
        self,
        user_id: str,
        expected_generation: int,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]:
        """
        Returns: (combined_text, reply_to_id, conversation_id, session_id, latest_message_id)
        """
        ref = self._ref(user_id)
        
        @firestore.transactional
        def _txn_pop(transaction):
            snap = ref.get(transaction=transaction)
            if not snap.exists:
                return None, None, None, None, None
            
            data = snap.to_dict()
            current_gen = int(data.get("generation", 0))
            
            if current_gen != expected_generation:
                return None, None, None, None, None

            messages = data.get("messages", [])
            if not messages:
                return None, None, None, None, None

            combined_text = "\n".join([m["text"] for m in messages])
            first_reply_id = next((m["reply_to"] for m in messages if m.get("reply_to")), None)
            latest_message_id = next((m.get("message_id") for m in reversed(messages) if m.get("message_id")), None)
            
            # NEW: Extract context
            conversation_id = data.get("conversation_id")
            session_id = data.get("session_id")

            transaction.delete(ref)
            
            return combined_text, first_reply_id, conversation_id, session_id, latest_message_id

        return _txn_pop(self.db.transaction())
