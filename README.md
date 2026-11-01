# Distributed Systems Portfolio: LLM Gateway & Raft KV Store

This repository contains the complete implementation of two distributed systems:

1. **Part 1: Hybrid LLM Gateway** (`llm-gateway/`) — Intelligent reverse proxy routing between local quantised models and cloud LLMs, equipped with an atomic Redis token-bucket rate limiter, semantic cache (FAISS + MiniLM), bounded queue backpressure, circuit breakers, and Prometheus/Grafana observability.
2. **Part 2: Distributed Raft Key-Value Store** (`raft-kv/`) — Complete Raft consensus algorithm built from scratch in Go, featuring leader election, log replication, persistent write-ahead logging (WAL) with `fsync`, snapshots/compaction, client request deduplication, and an in-memory fault-injection test harness.

---

## Architecture Diagrams

### 1. Hybrid LLM Gateway Architecture
```
[Client Request: POST /v1/chat]
       │
       ▼
[Redis Token Bucket (Atomic Lua)] ─── (Over Limit) ──► 429 Too Many Requests
       │ (Allowed)
       ▼
[Semantic Cache (MiniLM + FAISS)] ─── (Cosine Sim >= 0.90) ──► Cached Response
       │ (Cache Miss)
       ▼
[Intelligent Router]
  ├─ v1: Rule-Based (Length, keywords, latency budget)
  ├─ v2: Learned Classifier (Logistic regression on vector embeddings)
  └─ v3: Cascade (Speculative local execution -> output quality validation -> cloud escalation)
       │
       ▼
[Bounded Queue & Backpressure] ─── (Queue Full) ──► 503 Service Unavailable (Retry-After)
       │
       ▼
[Circuit Breakers & Retries with Jitter]
       │
   ┌───┴────────────────────────┐
   ▼                            ▼
[Local Model (Ollama/llama.cpp)]  [Cloud Provider (OpenAI / FakeProvider)]
       │                            │
       └──────────────┬─────────────┘
                      ▼
            [Populate Semantic Cache]
            [Log JSON + Prometheus Metrics]
                      ▼
               [HTTP 200 Response]
```

### 2. Raft Consensus KV Store Architecture
```
                      ┌──────────────────────┐
                      │    Client (Clerk)    │
                      └──────────┬───────────┘
                                 │ Put / Get / Delete
                                 ▼
                     ┌────────────────────────┐
        ┌────────────┤  Leader (Node 0)       ├────────────┐
        │            │  Term: T               │            │
        │            └──────────┬─────────────┘            │
 AppendEntries (RPC)            │              AppendEntries (RPC)
        │                       │                          │
        ▼                       ▼                          ▼
┌────────────────┐      ┌────────────────┐        ┌────────────────┐
│ Follower (N1)  │      │ WAL on Disk    │        │ Follower (N2)  │
│ (Term: T)      │      │ fsync(Term,Log)│        │ (Term: T)      │
└────────────────┘      └────────────────┘        └────────────────┘
        │                       │                          │
        └───────────────────────┴──────────────────────────┘
                                │
                    (Majority Commit: >= 2/3)
                                │
                                ▼
                   ┌──────────────────────────┐
                   │ Memory KV State Machine  │
                   │ Client Deduplication Tab │
                   │ Snapshot Compaction      │
                   └──────────────────────────┘
```

---

## Project 1: Hybrid LLM Gateway (`llm-gateway/`)

### Key Components
- **API Layer**: FastAPI with async non-blocking handlers, Pydantic request/response validation, and correlation `request_id` attached to every structured JSON log line.
- **Rate Limiting**: Token bucket per API key stored in Redis, executed atomically via `app/ratelimit/token_bucket.lua`. Supports burst traffic while strictly enforcing average throughput.
- **Semantic Cache**: Normalized 384-dimensional sentence embeddings indexed in a FAISS inner-product vector store (`app/cache/semantic_cache.py`). Includes client-level tenant namespace isolation and regex filtering for sensitive/time-sensitive prompts.
- **Routing Engine**:
  - `v1_rules.py`: Heuristics based on prompt word count, complex keywords (`code`, `math`, `translate`, `summarize`), token counts, and latency budgets.
  - `v2_learned.py`: Embeds prompt and runs inference through a trained Logistic Regression classifier.
  - `v3_cascade.py`: Speculatively routes to local model first, evaluates output against refusal patterns, repetition anomalies, and length constraints, escalating to cloud if checks fail.
- **Resilience & Chaos Engineering**:
  - `BoundedQueueBackpressure`: Enforces strict concurrency limits per backend; sheds load immediately with `503 Service Unavailable` and `Retry-After: 2` when saturated.
  - `CircuitBreaker`: Three-state machine (`Closed`, `Open`, `Half-Open`) with failure thresholds and recovery cooldown timers.
  - `FakeProvider`: Configurable latency and failure injection for zero-cost chaos testing.
- **Observability**: Prometheus metrics exported at `/metrics` (request count, latency histograms, cache hit/miss, routing decisions, queue depth, circuit breaker states).

### Quickstart (LLM Gateway)
```bash
# 1. Run with Docker Compose (Gateway, Redis, Prometheus, Grafana)
docker-compose up --build -d

# 2. Or run natively with Python:
cd llm-gateway
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000

# 3. Run full test suite:
pytest tests/ -v

# 4. Run load benchmark:
python benchmarks/load_test.py
```

---

## Project 2: Distributed Raft KV Store (`raft-kv/`)

### Key Components
- **Raft Consensus Protocol**:
  - **Leader Election**: Randomized election timeouts (150–300ms) with `RequestVote` RPC handlers enforcing election safety (candidate log must be at least as up to date as receiver).
  - **Log Replication**: `AppendEntries` RPC with log matching, conflict truncation, fast rollback optimization (`conflictIndex`, `conflictTerm`), and majority replica commit bookkeeping.
  - **Persistent Storage**: Write-Ahead Logging (`FilePersister`) with explicit `fsync` calls for `currentTerm`, `votedFor`, and log entries before replying to RPCs.
  - **Log Compaction & Snapshots**: Log compaction up to `lastIncludedIndex` and `InstallSnapshot` RPC to bring lagging followers up to date.
  - **Client Semantics**: Linearizable `Clerk` with unique client IDs and monotonic sequence numbers ensuring strict idempotency and deduplication.
- **Fault-Injection Test Harness (`SimulatedNetwork`)**:
  - Seeded, deterministic in-memory network simulator capable of dropping packets, delaying messages, disconnecting nodes, and executing arbitrary network partitions (`Partition` and `Heal`).

### Quickstart (Raft KV Store)
```bash
cd raft-kv

# Run all distributed tests:
go test -v ./tests/...

# Run individual fault tolerance tests:
go test -v -run TestLeaderFailover ./tests/...
go test -v -run TestNetworkPartitionAndHealing ./tests/...
go test -v -run TestCrashAndPersistenceRecovery ./tests/...
go test -v -run TestConcurrentLinearizability ./tests/...
```

---

## Benchmarks (measured)

### LLM Gateway
60 requests, 8 concurrent clients, in-process run without Redis.
- Throughput: 120.71 req/s
- Latency: p50 5.12 ms, p95 305.68 ms, p99 386.70 ms
- Cloud traffic: 1 of 60 requests (1.7%), routed by rule-based heuristics
- Cache hit rate in this benchmark: 0% (every prompt was unique)
- Cache hits under 2 ms were observed in manual tests with paraphrased prompts

### Raft KV Store
3-node and 5-node clusters, 6 of 6 tests passing in 6.41 s.
- Leader failover: about 0.30 s
- 5-node partition and heal: pass, 1.09 s
- Crash and WAL recovery: pass, 0.54 s
- Concurrent operations: 50 operations from 5 clients, all passed

### Limitations
- The gateway benchmark is small (60 requests) and does not measure answer quality, so the cloud-call reduction is not a quality-adjusted figure.
- The gateway benchmark did not include Redis or the rate limiter.
- The Raft tests run on a simulated in-memory network, not on separate machines.

---

## SDE Interview Preparation Guide

### 1. Why a token bucket, and how is the check made atomic?
A token bucket permits short bursts of traffic up to the bucket's capacity while strictly bounding the average throughput to the refill rate. In Redis, checking tokens and updating the timestamp in two separate commands creates a race condition (Time-Of-Check to Time-Of-Use). We guarantee atomicity by packaging the calculation inside a single Redis Lua script (`EVALSHA`), which runs in a single-threaded atomic execution context inside Redis.

### 2. How do you tune the semantic cache threshold, and what happens on false hits?
We measure cosine similarity of normalized MiniLM vectors using FAISS inner product. The 0.90 threshold was chosen manually and tested on a small set of example prompts. On a false hit, a user receives an unrelated answer. We mitigate this through:
1. Strict per-client tenant namespacing so one client never accesses another's cached data.
2. Regex exclusions for dynamic or sensitive inputs (passwords, tokens, stock prices, timestamps).
3. Client-controlled cache bypass headers (`no_cache=true`).

### 3. How does Raft guarantee committed entries are never lost during leader election?
Raft's election restriction states that a follower only votes for a candidate whose log is at least as up to date as its own. When comparing logs:
1. If the last entries have different terms, the candidate with the larger term is more up to date.
2. If the last entries have the same term, the candidate with the longer log (larger index) is more up to date.
Since committing an entry requires replication on a majority of nodes, and electing a leader also requires votes from a majority of nodes, the majority sets must intersect by at least one node. That intersecting node will refuse to vote for any candidate missing the committed entry, ensuring no candidate without all committed entries can ever win an election.
