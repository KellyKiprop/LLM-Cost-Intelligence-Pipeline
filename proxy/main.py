import time
import json
import os
import httpx

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="LLM Cost Proxy")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = os.getenv("KAFKA_TOPIC", "llm.inference.raw")

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

COST_PER_1K = {
    # OpenAI
    "gpt-4o":                    {"input": 0.00250, "output": 0.01000},
    "gpt-4o-mini":               {"input": 0.00015, "output": 0.00060},
    "gpt-4-turbo":               {"input": 0.01000, "output": 0.03000},
    "o1":                        {"input": 0.01500, "output": 0.06000},
    "o1-mini":                   {"input": 0.00300, "output": 0.01200},

    # ── Anthropic Claude ────────────────────────────────
    "claude-3-5-sonnet-20241022": {"input": 0.00300, "output": 0.01500},
    "claude-3-5-haiku-20241022":  {"input": 0.00080, "output": 0.00400},
    "claude-3-opus-20240229":     {"input": 0.01500, "output": 0.07500},
    "claude-3-haiku-20240307":    {"input": 0.00025, "output": 0.00125},

    # ── xAI Grok ────────────────────────────────────────
    "grok-2":                    {"input": 0.00200, "output": 0.01000},
    "grok-2-mini":               {"input": 0.00020, "output": 0.00040},
    "grok-beta":                 {"input": 0.00500, "output": 0.01500},

    # ── Groq / OSS (actual inference backend) ───────────
    "llama-3.3-70b-versatile":   {"input": 0.00059, "output": 0.00079},
    "llama-3.1-8b-instant":      {"input": 0.00005, "output": 0.00008},
    "mixtral-8x7b-32768":        {"input": 0.00024, "output": 0.00024},
    "gemma2-9b-it":              {"input": 0.00020, "output": 0.00020},
}

}

DEFAULT_MODEL = "llama-3.3-70b-versatile"

def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    rates = COST_PER_1K.get(model, {"input": 0.00059, "output": 0.00079})
    return round(
        (input_tokens  / 1000 * rates["input"]) +
        (output_tokens / 1000 * rates["output"]),
        8
    )

def get_provider(model: str) -> str:
    if model.startswith("gpt") or model.startswith("o1"):
        return "openai"
    if model.startswith("claude"):
        return "anthropic"
    if model.startswith("grok"):
        return "xai"
    if model.startswith("llama") or model.startswith("mixtral") or model.startswith("gemma"):
        return "groq"
    return "unknown"

@app.post("/v1/chat/completions")
async def proxy_completions(request: Request):
    body    = await request.json()
    headers = dict(request.headers)

    team             = headers.get("x-team",    "unknown")
    user_id          = headers.get("x-user-id", "unknown")
    feature          = headers.get("x-feature", "unknown")
    attributed_model = headers.get("x-model",   None)
    model            = body.get("model", DEFAULT_MODEL)

    if model not in COST_PER_1K:
        model = DEFAULT_MODEL
        body["model"] = model

    forward_headers = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {os.getenv('GROQ_API_KEY')}",
    }

    start = time.time()
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                GROQ_API_URL, json=body, headers=forward_headers
            )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.text
        )
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Upstream error: {str(e)}")

    latency_ms = round((time.time() - start) * 1000, 2)
    resp_data  = response.json()

    usage         = resp_data.get("usage", {})
    input_tokens  = usage.get("prompt_tokens",     0)
    output_tokens = usage.get("completion_tokens", 0)

    cost_model = attributed_model if attributed_model else model
    cost_usd   = calculate_cost(cost_model, input_tokens, output_tokens)

    event = {
        "event_ts":      int(time.time() * 1000),
        "team":          team,
        "user_id":       user_id,
        "feature":       feature,
        "model":         cost_model,
        "provider":      get_provider(cost_model),
        "input_tokens":  input_tokens,
        "output_tokens": output_tokens,
        "cost_usd":      cost_usd,
        "latency_ms":    latency_ms,
        "request_id":    resp_data.get("id", ""),
    }

    producer.send(TOPIC, event)
    producer.flush()

    print(f"[EVENT] team={team} model={cost_model} "
          f"tokens={input_tokens}+{output_tokens} "
          f"cost=${cost_usd} latency={latency_ms}ms")

    return JSONResponse(content=resp_data)

@app.get("/health")
async def health():
    return {
        "status":        "ok",
        "topic":         TOPIC,
        "default_model": DEFAULT_MODEL,
        "models":        list(COST_PER_1K.keys()),
    }

@app.get("/models")
async def list_models():
    return {
        "models": [
            {"id": model, "rates": rates}
            for model, rates in COST_PER_1K.items()
        ]
    }
