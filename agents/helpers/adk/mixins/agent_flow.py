import asyncio
import os
import threading
import time
from typing import Any, Optional

from google.genai.types import Content, Part
from agents.guardrails import adk_guardrails
from agents.tools.templates import MULTI_MESSAGE_DELIMITER

from utils.logging import logger


class AgentFlowMixin:
    def _run_async(self, coro):
        """
        Run async coroutines safely from sync contexts.
        Avoid asyncio.run() when an event loop is already running.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        result: dict = {"value": None, "error": None}

        def _runner():
            try:
                result["value"] = asyncio.run(coro)
            except Exception as e:
                result["error"] = e

        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        t.join()
        if result["error"]:
            raise result["error"]
        return result["value"]

    def handle_message(
        self,
        message: str,
        user_id: str,
        *,
        is_voice_input: bool = False,
        inbound_key: Optional[str] = None,
        reply_to_message_id: Optional[str] = None
    ) -> str:
        return self._run_async(
            self.handle_message_async(
                message,
                user_id,
                is_voice_input=is_voice_input,
                inbound_key=inbound_key,
                reply_to_message_id=reply_to_message_id,
            )
        )

    async def _summarize_turn(self, running_summary: str, user_text: str, assistant_text: str) -> str:
        """
        Update a concise, durable session summary: decisions, commitments, preferences,
        order state. Keep ~250–400 words. No chit-chat.
        """
        try:
            if self.GenerativeModel is None:
                return running_summary or ""
            model = self.GenerativeModel(os.getenv("SUMMARIZER_MODEL", "gemini-2.5-flash"))
            sys = ("You maintain a concise session summary for a WhatsApp sales assistant. "
                "Record only durable facts, decisions, commitments, and active order status. "
                "Keep it brief (<= 400 words).")
            prompt = (
                f"[PRIOR]\n{running_summary or ''}\n[/PRIOR]\n"
                f"[TURN]\n[U]{(user_text or '').strip()}[/U]\n[A]{(assistant_text or '').strip()}[/A]\n[/TURN]\n"
                "[TASK]Return ONLY the updated summary.[/TASK]"
            )
            resp = await asyncio.to_thread(model.generate_content, [sys, prompt])
            out = (getattr(resp, "text", "") or "").strip()
            return out or (running_summary or "")
        except Exception:
            return running_summary or ""

    async def _update_summary_bg(self, user_id: str, prior_summary: str, user_text: str, assistant_text: str):
        try:
            new_summary = await self._summarize_turn(prior_summary, user_text, assistant_text)
            if new_summary and new_summary != prior_summary:
                self.session_helper.set_summary(user_id, new_summary)
        except Exception:
            pass

    async def handle_message_async(
        self,
        message: str,
        user_id: str,
        *,
        is_voice_input: bool = False,
        inbound_key: Optional[str] = None,
        reply_to_message_id: Optional[str] = None
    ) -> str:
        try:
            # 0) --- AUTHENTICATION + CANONICAL USER MAPPING ---
            wa_user_id = user_id  # explicit naming: this is the WhatsApp number
            is_auth = self.session_helper.get_auth_status(wa_user_id)

            # If we already have an external_user_id in Firestore, treat as authenticated
            existing_external = self.get_external_user_id(wa_user_id)
            if existing_external and not is_auth:
                logger.info(
                    "Auth: existing external_user_id found, marking as authenticated",
                    wa_user_id=wa_user_id,
                    external_user_id=existing_external,
                )
                self.session_helper.set_auth_status(wa_user_id, True)
                is_auth = True

            if not is_auth:
                logger.info(
                    "Auth check: User not yet authenticated. Bootstrapping from API...",
                    wa_user_id=wa_user_id,
                )
                # This will check Firestore and, if needed, call the API
                external_id = await asyncio.to_thread(self._bootstrap_user_from_api, wa_user_id)

                if not external_id:
                    logger.warning(
                        "Auth check rejected. No canonical user_id available.",
                        wa_user_id=wa_user_id,
                    )
                    rejection_msg = (
                        "You are not registered in our system.\n"
                        "Please contact your Order Booker.\n\n"
                        "If you believe this is a mistake, kindly share:\n\n"
                        "1. Name\n"
                        "2. Mobile number\n"
                        "3. Recent EBM invoice\n\n"
                        "Send these details to *support@agentflo.com*"
                    )
                    self._send_text_once(wa_user_id, rejection_msg, reply_to_message_id=reply_to_message_id)
                    return rejection_msg

                logger.info(
                    "Auth check success; canonical user_id resolved.",
                    wa_user_id=wa_user_id,
                    external_user_id=external_id,
                )
                self.session_helper.set_auth_status(wa_user_id, True)

            # Best-effort metadata fetch & persist (store_code, store_name, etc.)
            try:
                await asyncio.to_thread(self._ensure_customer_metadata, wa_user_id)
            except Exception:
                pass

            # 1) --- Inbound dedupe ---
            if inbound_key and self.inbound_dedupe_enabled:
                first_time = self._inbound_mark_if_new(wa_user_id, inbound_key)
                if not first_time:
                    logger.info("inbound.duplicate.skip", inbound=inbound_key, user_id=wa_user_id)
                    return ""

            # 2) --- Lazy enforcement for pending end / inactivity ---
            due, reason = self.session_helper.should_end_now(
                wa_user_id,
                inactivity_sec=int(os.getenv("SESSION_INACTIVITY_SEC", str(7 * 60 * 60))),
            )
            if due:
                # End current lifecycle state
                self.session_helper.end_now(wa_user_id, reason=reason or "inactivity")
                # Rotate Vertex session so the next message starts fresh
                session_id_existing = self._get_cached_session_id(wa_user_id)
                if session_id_existing:
                    await self.save_and_create_new_session(wa_user_id, session_id_existing)
                # Touch as new user activity after rotation (this very message)
                self.session_helper.touch(wa_user_id, inbound_key=inbound_key, source="user")
                self.session_helper.cancel_pending_end(wa_user_id)

            # Always touch + cancel on legit inbound (defensive)
            self.session_helper.touch(wa_user_id, inbound_key=inbound_key, source="user")
            self.session_helper.cancel_pending_end(wa_user_id)

            # ---- IMPORTANT: we NO LONGER count user texts here (fixes off-by-one) ----
            # (removed the inc_text_count for user messages)

            # 3) --- optional legacy turn-budget rotation: DISABLE by default ---
            try:
                MAX_TURNS = int(os.getenv("SESSION_TURN_BUDGET", "0"))  # 0 disables legacy budget
            except Exception:
                MAX_TURNS = 0
            try:
                turns = self.session_helper.inc_turn(wa_user_id) if MAX_TURNS > 0 else 0
            except Exception:
                turns = 0

            if MAX_TURNS > 0 and turns >= MAX_TURNS:
                # End & rotate BEFORE answering this inbound
                self.session_helper.end_now(wa_user_id, reason="turn_budget")
                session_id_existing = self._get_cached_session_id(wa_user_id)
                if session_id_existing:
                    await self.save_and_create_new_session(wa_user_id, session_id_existing)
                try:
                    self.session_helper.cancel_pending_end(wa_user_id)
                    self.session_helper.reset_turns(wa_user_id)
                except Exception:
                    pass
                # touch again so the current inbound is counted in the fresh session
                self.session_helper.touch(wa_user_id, inbound_key=inbound_key, source="user")

            # 4) --- Session fetch/create with canonical user_id in state ---
            session_id = self._get_cached_session_id(wa_user_id)
            if not session_id:
                # Canonical backend id for the agent state (not for WA transport)
                state_user_id = self._get_canonical_user_id_for_state(wa_user_id)
                external_user_id = self.get_external_user_id(wa_user_id)
                # Ensure user doc exists (bootstrap may have already created it)
                self.create_user_document(wa_user_id)
                customer_meta = await asyncio.to_thread(self._ensure_customer_metadata, wa_user_id)
                state = {
                    "user_id": state_user_id,  # what {user_id} in the prompt will see
                    "wa_user_id": wa_user_id,
                }
                if external_user_id:
                    state["external_user_id"] = external_user_id
                state.update({k: v for k, v in customer_meta.items() if v})
                new_session = await self.session_service.create_session(
                    app_name=self.APP_NAME,
                    user_id=wa_user_id,  # Vertex session owned by WA user id
                    state=state,
                )
                session_id = new_session.id
                self._update_cached_session_id(wa_user_id, session_id)
                logger.info(
                    "session.created",
                    user_id=wa_user_id,
                    state_user_id=state_user_id,
                    external_user_id=external_user_id,
                    session_id=session_id,
                )

            # 5) --- Goodbye / catalog logic ---
            is_goodbye = self.is_goodbye_message(message)

            # Auto-send catalog once per session before responding (skip farewell messages)
            if not is_goodbye:
                await self._send_catalog_if_new_session(wa_user_id, session_id)

            if is_goodbye:
                # Mark ended immediately and rotate
                self.session_helper.end_now(wa_user_id, reason="manual")
                new_session_id = await self.save_and_create_new_session(wa_user_id, session_id)
                logger.info("session.rotated", user_id=wa_user_id, new_session_id=new_session_id)
                response = "Goodbye! If you need assistance later, feel free to reach out."
                self._send_text_once(wa_user_id, response, reply_to_message_id=reply_to_message_id)
                return response

            # 6) --- Call agent ---
            agent_response = await self._call_agent_text_only(message, wa_user_id, session_id)
            if not agent_response:
                raise ValueError("Agent response is empty.")

            # NOTE: we used to pre-compute threshold_hit here. We don't anymore.
            # The VN/text send function will compute threshold AFTER counting the sent agent text.
            agent_response = await self._send_text_then_optional_vn_then_finalize(
                wa_user_id,
                agent_response,
                is_voice_input=is_voice_input,
                reply_to_message_id=reply_to_message_id,
                inbound_key=inbound_key,
                session_id_at_start=session_id,
                threshold_hit=False,  # ignored inside; kept for signature compatibility
                user_utterance_for_summary=message,
            )
            return agent_response

        except Exception as e:
            logger.error("handle_message_async.error", error=str(e), user_id=user_id)
            err = "I didn't catch that. Could you please repeat?"
            self._send_text_once(user_id, err, reply_to_message_id=reply_to_message_id)
            return err

    # ---------- VN (or fallback audio/text) ----------
    async def _send_text_then_optional_vn_then_finalize(
        self,
        user_id: str,
        agent_text: Any,
        *,
        is_voice_input: bool,
        reply_to_message_id: Optional[str],
        inbound_key: Optional[str],
        session_id_at_start: str,
        threshold_hit: bool,                # legacy param: will be recomputed AFTER send
        user_utterance_for_summary: str
    ):
        """
        Order of ops on rotation turn (every 10 *agent* texts):
        1) send agent TEXT
        2) increment agent text counter and recompute threshold (exactly on multiples of 10)
        3) if voice-input flow: send VN AFTER text (sync only if threshold turn)
        4) update summary (only on threshold turns)
        5) rotate Vertex session (only on threshold turns)
        6) reset the 10-text counter (only on threshold turns)
        """
        def _coerce_messages(raw):
            if isinstance(raw, str):
                if MULTI_MESSAGE_DELIMITER in raw:
                    return [p.strip() for p in raw.split(MULTI_MESSAGE_DELIMITER) if p and p.strip()]
                return [raw.strip()] if raw.strip() else []
            if isinstance(raw, (list, tuple)):
                return [str(p).strip() for p in raw if isinstance(p, str) and str(p).strip()]
            if isinstance(raw, dict):
                candidate = raw.get("messages") or raw.get("parts")
                if isinstance(candidate, (list, tuple)):
                    return [str(p).strip() for p in candidate if isinstance(p, str) and str(p).strip()]
                if isinstance(raw.get("text"), str) and raw.get("text").strip():
                    return [raw.get("text").strip()]
            if raw:
                return [str(raw).strip()]
            return []

        messages = _coerce_messages(agent_text)
        combined_text = "\n\n".join(messages).strip() if messages else (agent_text.strip() if isinstance(agent_text, str) else "")

        # 1) send agent TEXT (must be first)
        sent_count = 0
        for idx, msg in enumerate(messages or ([combined_text] if combined_text else [])):
            if not msg:
                continue
            sent = self._send_text_once(user_id, msg, reply_to_message_id=reply_to_message_id if idx == 0 else None)
            if sent:
                sent_count += 1
        if sent_count == 0:
            # do not count failed sends; do not VN/rotate
            return combined_text or agent_text

        # 2) Count **agent** text now that it actually sent, then recompute threshold here
        try:
            new_count = self.session_helper.inc_text_count(user_id, sent_count)
        except Exception:
            new_count = None
        # Recompute threshold: rotate exactly on 10, 20, 30, ...
        threshold_hit = (isinstance(new_count, int) and new_count % 10 == 0)

        # 3) if this turn originated from VOICE input, send VN AFTER text
        if is_voice_input:
            if threshold_hit:
                # synchronous VN to guarantee "text -> VN -> finalize"
                try:
                    if self._acquire_voice_lock(user_id):
                        await self._gen_vn_else_text(
                            user_id,
                            combined_text,
                            inbound_key=inbound_key,
                            reply_to_message_id=reply_to_message_id
                        )
                finally:
                    self._release_voice_lock(user_id)
            else:
                # non-threshold: fire-and-forget VN path
                self._spawn_vn_or_text(
                    user_id,
                    combined_text,
                    inbound_key=inbound_key,
                    reply_to_message_id=reply_to_message_id
                )

        # 4–6) finalize lifecycle if we just hit the 10th **agent** text
        if threshold_hit:
            # 4) summary (sync on this last turn)
            try:
                prior = self.session_helper.get_summary(user_id)
            except Exception:
                prior = ""
            try:
                updated = await self._summarize_turn(prior, user_utterance_for_summary, combined_text)
                if updated and updated != prior:
                    self.session_helper.set_summary(user_id, updated[:4000])
            except Exception:
                pass

            # 5) rotate session
            try:
                await self.save_and_create_new_session(user_id, session_id_at_start)
            except Exception:
                pass

            # 6) reset the 10-text window
            try:
                self.session_helper.reset_text_count(user_id)
            except Exception:
                pass

        # agent activity touch (doesn't extend inactivity)
        self.session_helper.touch(user_id, source="agent")
        return combined_text

    def _spawn_vn_or_text(self, user_id: str, text: str, *, inbound_key: Optional[str], reply_to_message_id: Optional[str]):
        def _runner():
            try:
                asyncio.run(self._gen_vn_else_text(user_id, text, inbound_key=inbound_key, reply_to_message_id=reply_to_message_id))
            except Exception as e:
                logger.error("vn.text.runner.crash", error=str(e), user_id=user_id)

        if not self._acquire_voice_lock(user_id):
            logger.info("voice.lock.busy.skip", user_id=user_id)
            return

        t = threading.Thread(target=_runner, daemon=True)
        t.start()

    async def _call_agent_text_only(self, query: str, user_id: str, session_id: str) -> str:
        """
        Invoke the agent with a compact prompt:
        [SESSION_SUMMARY] + current user query.
        After getting the final reply, update the running summary in the background.
        Also captures Gemini usage metadata for unified billing by route handlers.
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        # ---- fetch & inject running summary (best-effort; safe if unavailable) ----
        try:
            summary = self.session_helper.get_summary(user_id)  # may be ""
        except Exception:
            summary = ""

        merged_query = (
            f"[SESSION_SUMMARY]\n{summary}\n[/SESSION_SUMMARY]\n{query.strip()}"
            if summary else query.strip()
        )
        # Prepend customer/store context if available
        try:
            customer_ctx = self._build_customer_context_for_agent(user_id)
        except Exception:
            customer_ctx = ""
        if customer_ctx:
            merged_query = f"[CUSTOMER_CONTEXT]\n{customer_ctx}\n[/CUSTOMER_CONTEXT]\n{merged_query}"

        content = Content(role="user", parts=[Part(text=merged_query)])

        # ---- Agent execution ----
        session, runner = await self.setup_session_and_runner(user_id, session_id)
        if not session:
            raise ValueError("Session not found")

        events = runner.run_async(user_id=user_id, session_id=session.id, new_message=content)
        final_text = ""
        gemini_usage = None
        agent_start_time = time.perf_counter()

        try:
            async for event in events:
                try:
                    # Try generic detection for "final" event
                    is_final = False
                    attr = getattr(event, "is_final_response", None)
                    if callable(attr):
                        is_final = attr()
                    elif isinstance(attr, bool):
                        is_final = attr
                    else:
                        is_final = (getattr(event, "event_type", "") == "final_response")

                    if is_final:
                        content_obj = getattr(event, "content", None)
                        if content_obj and getattr(content_obj, "parts", None):
                            out = []
                            for p in content_obj.parts:
                                txt = getattr(p, "text", None)
                                if not txt and isinstance(p, dict):
                                    txt = p.get("text")
                                if txt:
                                    out.append(txt)
                            final_text = " ".join(out).strip() if out else ""

                        # ---- Extract Gemini usage metadata (best-effort) ----
                        try:
                            usage_meta = getattr(event, "usage_metadata", None)
                            if usage_meta is not None:
                                agent_latency_ms = int((time.perf_counter() - agent_start_time) * 1000)

                                prompt_tokens = getattr(usage_meta, "prompt_token_count", 0)
                                candidates_tokens = getattr(usage_meta, "candidates_token_count", 0)
                                total_tokens = getattr(usage_meta, "total_token_count", 0)
                                thoughts_tokens = getattr(usage_meta, "thoughts_token_count", 0)

                                # Current public Gemini 2.5 Flash pricing (per 1M tokens)
                                input_price_gemini = round(0.30 / 1_000_000, 9)
                                output_price_gemini = round(2.50 / 1_000_000, 9)

                                gemini_usage = {
                                    "enabled": True,
                                    "model": "gemini-2.5-flash",
                                    "request_id": getattr(event, "invocation_id", "") or "",
                                    "latency_ms": agent_latency_ms,
                                    "input_tokens": prompt_tokens,
                                    "output_tokens": candidates_tokens,
                                    "total_tokens": total_tokens,
                                    "thoughts_tokens": thoughts_tokens,
                                    "pricing": {
                                        "currency": "USD",
                                        "input_unit": "1M_tokens",
                                        "output_unit": "1M_tokens",
                                        "input_price_per_unit": input_price_gemini,
                                        "output_price_per_unit": output_price_gemini,
                                    },
                                    "cost": {
                                        "input_cost_usd": round(prompt_tokens * input_price_gemini, 6),
                                        "output_cost_usd": round(candidates_tokens * output_price_gemini, 6),
                                    },
                                }
                                gemini_usage["cost"]["total_cost_usd"] = round(
                                    gemini_usage["cost"]["input_cost_usd"] + gemini_usage["cost"]["output_cost_usd"],
                                    6,
                                )

                                # Expose latest usage for unified per-message billing (route handler)
                                try:
                                    self._last_gemini_usage[user_id] = gemini_usage
                                except Exception:
                                    pass

                                logger.info(
                                    "gemini.usage.captured",
                                    user_id=user_id,
                                    prompt_tokens=prompt_tokens,
                                    candidates_tokens=candidates_tokens,
                                    total_tokens=total_tokens,
                                    thoughts_tokens=thoughts_tokens,
                                    latency_ms=agent_latency_ms,
                                    total_cost_usd=gemini_usage["cost"]["total_cost_usd"],
                                )
                        except Exception as usage_err:
                            logger.warning("gemini.usage_extraction.error", error=str(usage_err))

                        break
                except Exception as per_event:
                    logger.warning("agent.event.process.error", error=str(per_event))
                    continue
        finally:
            aclose = getattr(events, "aclose", None)
            if callable(aclose):
                try:
                    await aclose()
                except Exception:
                    pass

        # If guardrails forced a reply, prefer it over the LLM text
        try:
            forced = adk_guardrails.pop_forced_reply(user_id=user_id)
            if not forced and hasattr(session, "state") and isinstance(getattr(session, "state"), dict):
                forced = (session.state.get("forced_reply") or session.state.get("Engro_response") or "").strip()
            if forced:
                final_text = forced
        except Exception:
            pass

        # ---- update the summary in the background (fire-and-forget) ----
        try:
            if final_text:
                prior_summary = summary
                user_text = query
                assistant_text = final_text

                async def _bg():
                    try:
                        new_summary = await self._summarize_turn(prior_summary, user_text, assistant_text)
                        if new_summary and new_summary != prior_summary:
                            try:
                                self.session_helper.set_summary(user_id, new_summary[:4000])
                            except Exception:
                                pass
                    except Exception:
                        pass

                asyncio.create_task(_bg())
        except Exception:
            pass

        return final_text or "I didn't catch that. Could you please say it again?"

    def get_last_gemini_usage(self, user_id: str) -> Optional[dict]:
        try:
            return self._last_gemini_usage.get(user_id)
        except Exception:
            return None

    def get_last_eleven_tts_usage(self, user_id: str) -> Optional[dict]:
        try:
            return self._last_eleven_tts_usage.get(user_id)
        except Exception:
            return None
