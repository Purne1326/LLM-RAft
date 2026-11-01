import time
import os
import uuid
from typing import Optional
from fastapi import APIRouter, Request, Header, HTTPException, Depends
from fastapi.responses import HTMLResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from app.models import ChatRequest, ChatResponse
from app.config import settings
from app.metrics.logger import get_logger, request_id_var
from app.metrics.prometheus import REQUEST_COUNT, REQUEST_LATENCY, ROUTING_DECISIONS
from app.resilience.circuit_breaker import CircuitBreakerOpenException
from app.resilience.retry import retry_with_backoff
from app.router.v3_cascade import CascadeOutputValidator

logger = get_logger("api_routes")
router = APIRouter()

@router.get("/", response_class=HTMLResponse)
async def serve_ui():
    ui_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static", "index.html")
    if os.path.exists(ui_path):
        with open(ui_path, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h1>Hybrid LLM Gateway API</h1><p>Visit <a href='/docs'>/docs</a></p>")

def get_api_key(
    x_api_key: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None)
) -> str:
    if x_api_key:
        return x_api_key
    if authorization and authorization.startswith("Bearer "):
        return authorization[7:].strip()
    return "default_client"

@router.get("/health")
async def health_check():
    return {"status": "ok", "app": settings.APP_NAME}

@router.get("/metrics")
async def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@router.post("/v1/chat", response_model=ChatResponse)
async def chat_completion(
    chat_req: ChatRequest,
    req: Request,
    api_key: str = Depends(get_api_key)
):
    req_id = str(uuid.uuid4())
    request_id_var.set(req_id)
    start_time = time.time()
    prompt = chat_req.get_prompt_text()

    if not prompt:
        raise HTTPException(status_code=400, detail="Empty prompt provided")

    state = req.app.state
    
    # 1. Rate Limiting via Token Bucket
    allowed, remaining = await state.rate_limiter.allow_request(api_key)
    if not allowed:
        REQUEST_COUNT.labels(endpoint="/v1/chat", status="429").inc()
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please slow down.",
            headers={"Retry-After": "1"}
        )

    # 2. Semantic Cache Lookup
    if not chat_req.no_cache:
        cached_result = await state.semantic_cache.get(client_id=api_key, prompt=prompt)
        if cached_result:
            duration = time.time() - start_time
            REQUEST_LATENCY.labels(route="cache", cached="true").observe(duration)
            REQUEST_COUNT.labels(endpoint="/v1/chat", status="200").inc()
            return ChatResponse(
                id=req_id,
                response=cached_result["response"],
                provider=cached_result["provider"],
                model=cached_result["model"],
                cached=True,
                latency_ms=round(duration * 1000, 2),
                route_decision="cache_hit"
            )

    # 3. Routing Decision
    router_instance = state.active_router
    decision = await router_instance.route(chat_req)
    backend_choice = decision.selected_backend
    reason = decision.reason
    ROUTING_DECISIONS.labels(backend=backend_choice, router_version=settings.ROUTER_TYPE, reason=reason).inc()

    # 4. Resilience, Backpressure, Execution & Fallback
    async def call_provider(target_backend: str):
        provider = state.providers[target_backend]
        backpressure = state.backpressure[target_backend]
        cb = state.circuit_breakers[target_backend]

        if not cb.allow_request():
            raise CircuitBreakerOpenException(f"Circuit breaker open for {target_backend}")

        await backpressure.acquire()
        try:
            async def _exec():
                return await provider.generate(
                    prompt,
                    max_tokens=chat_req.max_tokens,
                    temperature=chat_req.temperature
                )
            res = await retry_with_backoff(_exec, max_retries=2)
            cb.record_success()
            return res
        except Exception as e:
            cb.record_failure()
            raise e
        finally:
            backpressure.release()

    response_content = None
    final_provider_name = ""
    final_model_name = ""
    tokens = 0

    try:
        # Cascade Router logic (v3)
        if settings.ROUTER_TYPE == "cascade":
            try:
                res = await call_provider("local")
                is_ok, check_msg = CascadeOutputValidator.is_acceptable(prompt, res.content)
                if is_ok:
                    response_content = res.content
                    final_provider_name = res.provider
                    final_model_name = res.model
                    tokens = res.tokens_used
                    reason = f"cascade: local output accepted ({check_msg})"
                else:
                    logger.info(f"Cascade: local check failed ({check_msg}). Escalating to cloud...")
                    res = await call_provider("cloud")
                    response_content = res.content
                    final_provider_name = res.provider
                    final_model_name = res.model
                    tokens = res.tokens_used
                    reason = f"cascade: escalated to cloud ({check_msg})"
            except Exception as e:
                logger.warning(f"Local failed in cascade ({e}), falling back to cloud...")
                res = await call_provider("cloud")
                response_content = res.content
                final_provider_name = res.provider
                final_model_name = res.model
                tokens = res.tokens_used
        else:
            # v1 (rules) or v2 (learned)
            try:
                res = await call_provider(backend_choice)
                response_content = res.content
                final_provider_name = res.provider
                final_model_name = res.model
                tokens = res.tokens_used
            except Exception as e:
                # Primary failed, attempt fallback to the alternative backend
                fallback_backend = "cloud" if backend_choice == "local" else "local"
                logger.warning(f"Primary backend '{backend_choice}' failed ({e}). Attempting fallback to '{fallback_backend}'")
                res = await call_provider(fallback_backend)
                response_content = res.content
                final_provider_name = res.provider
                final_model_name = res.model
                tokens = res.tokens_used
                reason += f" (primary failed: {e}; fell back to {fallback_backend})"

    except Exception as exc:
        logger.error(f"Both primary and fallback backends failed: {exc}")
        REQUEST_COUNT.labels(endpoint="/v1/chat", status="502").inc()
        raise HTTPException(status_code=502, detail=f"All backends unavailable: {str(exc)}")

    # 5. Populate Semantic Cache
    if not chat_req.no_cache and response_content:
        await state.semantic_cache.set(
            client_id=api_key,
            prompt=prompt,
            response=response_content,
            provider=final_provider_name,
            model=final_model_name
        )

    duration = time.time() - start_time
    REQUEST_LATENCY.labels(route=backend_choice, cached="false").observe(duration)
    REQUEST_COUNT.labels(endpoint="/v1/chat", status="200").inc()

    return ChatResponse(
        id=req_id,
        response=response_content,
        provider=final_provider_name,
        model=final_model_name,
        cached=False,
        latency_ms=round(duration * 1000, 2),
        route_decision=backend_choice,
        route_reason=reason,
        tokens_used=tokens
    )

