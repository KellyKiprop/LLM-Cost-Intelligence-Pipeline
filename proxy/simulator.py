import httpx
import asyncio
import random

PROXY_URL = "http://localhost:8000/v1/chat/completions"

TEAMS = ["engineering", "product", "data", "marketing", "support"]

USERS = {
    "engineering": ["kelly", "james", "aisha", "brian"],
    "product":     ["linda", "omar", "priya", "tom"],
    "data":        ["grace", "hassan", "nina", "joel"],
    "marketing":   ["sara", "mike", "fatma", "leon"],
    "support":     ["diana", "kevin", "amara", "chris"],
}

FEATURES = {
    "engineering": ["code-review", "bug-triage", "docs-generation", "test-writing"],
    "product":     ["roadmap-summary", "user-feedback", "spec-writing", "competitor-analysis"],
    "data":        ["query-generation", "report-summary", "anomaly-explain", "chart-caption"],
    "marketing":   ["copy-generation", "email-draft", "seo-summary", "campaign-ideas"],
    "support":     ["ticket-reply", "kb-search", "escalation-summary", "sentiment-check"],
}

TEAM_MODELS = {
    # Engineering — heavy Claude + GPT-4 users
    "engineering": [
        ("claude-3-5-sonnet-20241022",  0.40),
        ("gpt-4o",                      0.30),
        ("o1-mini",                     0.15),
        ("claude-3-opus-20240229",      0.10),
        ("gpt-4-turbo",                 0.05),
    ],
    # Product — balanced mid-tier models
    "product": [
        ("gpt-4o",                      0.35),
        ("claude-3-5-sonnet-20241022",  0.25),
        ("gpt-4o-mini",                 0.20),
        ("grok-2",                      0.20),
    ],
    # Data — cost-conscious, OSS + cheap models
    "data": [
        ("llama-3.3-70b-versatile",     0.35),
        ("gpt-4o-mini",                 0.25),
        ("claude-3-5-haiku-20241022",   0.25),
        ("grok-2-mini",                 0.15),
    ],
    # Marketing — cheap + mid tier for copy/content
    "marketing": [
        ("gpt-4o-mini",                 0.40),
        ("claude-3-haiku-20240307",     0.30),
        ("grok-2-mini",                 0.20),
        ("gemma2-9b-it",                0.10),
    ],
    # Support — cheapest models, high volume
    "support": [
        ("claude-3-haiku-20240307",     0.40),
        ("llama-3.1-8b-instant",        0.30),
        ("gpt-4o-mini",                 0.20),
        ("grok-2-mini",                 0.10),
    ],
}

PROMPTS = [
    "Summarise this in two sentences: data engineering is the practice of designing and building systems for collecting, storing, and analysing data at scale.",
    "What are three best practices for writing clean SQL queries?",
    "Explain the difference between a data lake and a data warehouse.",
    "Write a short professional email declining a meeting request.",
    "What is the CAP theorem and why does it matter for distributed systems?",
    "List five key metrics every SaaS business should track.",
    "Summarise the main differences between Kafka and RabbitMQ.",
    "What is idempotency and why is it important in data pipelines?",
    "Write three subject lines for a product launch email campaign.",
    "Explain what a Kafka consumer group is in simple terms.",
    "What are the main causes of data pipeline failures in production?",
    "Give me five ideas for reducing cloud infrastructure costs.",
    "What is the difference between batch and stream processing?",
    "Explain what a dbt model is and why it is useful.",
    "What are the pros and cons of using a star schema?",
]

async def fire_request(client: httpx.AsyncClient, team: str):
    user_id = random.choice(USERS[team])
    feature = random.choice(FEATURES[team])
    prompt  = random.choice(PROMPTS)

    models, weights = zip(*TEAM_MODELS[team])
    attributed_model = random.choices(models, weights=list(weights))[0]

    try:
        response = await client.post(
            PROXY_URL,
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": random.randint(50, 300),
            },
            headers={
                "Content-Type": "application/json",
                "x-team":       team,
                "x-user-id":    user_id,
                "x-feature":    feature,
                "x-model":      attributed_model,
            },
            timeout=30.0,
        )
        data       = response.json()
        usage      = data.get("usage", {})
        in_tokens  = usage.get("prompt_tokens", 0)
        out_tokens = usage.get("completion_tokens", 0)
        print(f"OK  | team={team:<12} user={user_id:<8} "
              f"model={attributed_model:<25} "
              f"tokens={in_tokens}+{out_tokens}")
    except Exception as e:
        print(f"ERR | team={team} — {e}")

async def run_simulation(total_requests: int = 200, concurrency: int = 5):
    print(f"Firing {total_requests} requests across {len(TEAMS)} teams...\n")
    semaphore = asyncio.Semaphore(concurrency)

    async def throttled(team):
        async with semaphore:
            await fire_request(client, team)
            await asyncio.sleep(random.uniform(2.0, 4.0))

    async with httpx.AsyncClient() as client:
        tasks = [
            throttled(random.choice(TEAMS))
            for _ in range(total_requests)
        ]
        await asyncio.gather(*tasks)

    print(f"\nDone. {total_requests} events fired.")
    print("Check Kafka UI and PostgreSQL for results.")

if __name__ == "__main__":
    asyncio.run(run_simulation(total_requests=200, concurrency=2))
