#!/usr/bin/env python3
"""
Reddit comment collector with FastText language detection and
CardiffNLP sentiment analysis. Supports two modes:
  - single:  fetch one user every 30s
  - batch:   fetch BATCH_SIZE users in parallel, then repeat
"""

from __future__ import annotations
import os
import json
import time
import re
import argparse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional
from itertools import cycle

import fasttext
import praw
import prawcore
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers.pipelines import pipeline

# ---------- Configuration ----------
CREDENTIALS_FILE = "reddit_credentials.json"
SEEN_USERS_FILE = "seen_usernames.json"
DATA_DIR = "data"

BATCH_SIZE = 50  # used in batch mode
MAX_COMMENTS = 2000
FT_MODEL_PATH = "lid.176.ftz"
FT_THRESHOLD = 0.7
SLEEP_SECONDS = 30  # used in single mode
DEVICE = 0 if torch.cuda.is_available() else -1

os.makedirs(DATA_DIR, exist_ok=True)

# ---------- Regex helper ----------
_URL_RE = re.compile(r"https?://\S+")

# ---------- Load models once ----------
ft_model = fasttext.load_model(FT_MODEL_PATH)

tokenizer = AutoTokenizer.from_pretrained(
    "cardiffnlp/twitter-roberta-base-sentiment-latest", normalization=True
)
model = AutoModelForSequenceClassification.from_pretrained(
    "cardiffnlp/twitter-roberta-base-sentiment-latest", ignore_mismatched_sizes=True
)
sent_pipe = pipeline(
    "sentiment-analysis", model=model, tokenizer=tokenizer, device=DEVICE
)


# ---------- Utility functions ----------
def clean_text(text: str) -> str:
    # Removes URLs, collapse newlines, strip whitespace
    out = _URL_RE.sub("", text).replace("\n", " ").strip()
    return out


def detect_lang(text: str, threshold: float = FT_THRESHOLD) -> str:
    # Returns ISO code or 'unknown' if below threshold
    cleaned = clean_text(text).lower()
    if not cleaned:
        return "unknown"
    result = ft_model.predict(cleaned, k=1)
    if not isinstance(result, tuple) or len(result) != 2:
        return "unknown"
    labels, scores = result
    labels = list(labels)
    scores = list(scores)
    if not labels or not scores:
        return "unknown"
    iso = str(labels[0]).replace("__label__", "")
    return iso if float(scores[0]) >= threshold else "unknown"


def classify_sentiment(text: str) -> Dict[str, float]:
    # ask for all three labels and their scores
    result = sent_pipe(text[:512], return_all_scores=True)
    if not result:
        return {}
    # result[0] is a list of dicts: [{'label':..., 'score':...}, …]
    scores = result[0]
    out: Dict[str, float] = {}
    for entry in scores:
        label = entry["label"].lower()
        out[label] = float(entry["score"])
    return out


def save_parquet(data: List[Dict], prefix: str) -> None:
    # Writes list of dicts to a parquet file with timestamped prefix
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    filename = f"{prefix}_{ts}.parquet"
    path = os.path.join(DATA_DIR, filename)
    table = pa.Table.from_pylist(data)
    pq.write_table(table, path)
    print(f"✓ Saved {len(data)} rows to {path}")


# ---------- Reddit client setup ----------
with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
    creds_list: List[Dict] = json.load(f)

reddit_clients = [
    praw.Reddit(
        client_id=c["client_id"],
        client_secret=c["client_secret"],
        username=c.get("username", ""),
        password=c.get("password", ""),
        user_agent=c.get("user_agent", f"Collector{i}"),
        check_for_async=False,
    )
    for i, c in enumerate(creds_list)
]
reddit_cycle = cycle(enumerate(reddit_clients, start=1))

# ---------- Seen-users checkpoint ----------
if os.path.exists(SEEN_USERS_FILE):
    with open(SEEN_USERS_FILE, "r", encoding="utf-8") as f:
        seen_users = set(json.load(f))
else:
    seen_users = set()


def save_seen() -> None:
    # Persists seen usernames to JSON checkpoint
    with open(SEEN_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(sorted(seen_users), f)


# ---------- Core fetch-and-process ----------
def process_user_comments(username: str, raw_comments: List) -> List[Dict]:
    # Detects language and sentiment on a list of Comment objects
    out: List[Dict] = []
    for c in raw_comments:
        body = c.body or ""
        clean_body = clean_text(body)
        lang = detect_lang(clean_body)
        sentiment = (
            classify_sentiment(clean_body) if lang == "en" and clean_body else None
        )
        out.append(
            {
                "username": username,
                "comment_id": c.id,
                "subreddit": str(c.subreddit),
                "created_utc": int(c.created_utc),
                "score": c.score,
                "body_clean": clean_body,
                "language": lang,
                "sentiment": sentiment,
            }
        )
    return out


def fetch_user_job(job: Tuple[int, str]) -> Optional[Tuple[Dict, List[Dict]]]:
    idx, uname = job
    client = reddit_clients[idx - 1]
    try:
        u = client.redditor(uname)
        raw_comments = list(u.comments.new(limit=MAX_COMMENTS))
    except Exception as e:
        print(f"[{uname}] ✖ fetch failed: {e}")
        return None

    # Builds user metadata row
    user_row = {
        "username": uname,
        "created_utc": int(u.created_utc),
        "link_karma": u.link_karma,
        "comment_karma": u.comment_karma,
        "is_mod": bool(getattr(u, "is_mod", False)),
        "fetched_at": int(time.time()),
    }

    # Processes comments
    comment_rows = process_user_comments(uname, raw_comments)

    # Marks seen and persist JSON
    seen_users.add(uname)
    save_seen()

    return user_row, comment_rows


# ---------- Single-user mode ----------
def run_single():
    # Cycles through all available reddit_clients
    single_cycle = cycle(enumerate(reddit_clients, start=1))

    for c in reddit_clients[0].subreddit("all").stream.comments(pause_after=-1):
        if c is None:
            continue
        author = c.author
        if not author or author.name in seen_users:
            continue
        uname = author.name

        # Picks the next credential
        client_idx, client = next(single_cycle)

        # marks seen & persist
        seen_users.add(uname)
        save_seen()

        # Logs which credential we’re using
        print(f"[single][#{client_idx}] Processing u/{uname}")

        # Fetches & saves user metadata
        u = client.redditor(uname)
        user_row = {
            "username": uname,
            "created_utc": int(u.created_utc),
            "link_karma": u.link_karma,
            "comment_karma": u.comment_karma,
            "is_mod": bool(getattr(u, "is_mod", False)),
            "trophy_count": 0,
            "fetched_at": int(time.time()),
            "client_idx": client_idx,
        }
        save_parquet([user_row], "users")

        # Fetches, processes & saves comments
        raw = list(client.redditor(uname).comments.new(limit=MAX_COMMENTS))
        comment_rows = process_user_comments(uname, raw)
        save_parquet(comment_rows, uname)

        time.sleep(SLEEP_SECONDS)


# ---------- Batch mode ----------
def run_batch():
    from threading import Lock

    fresh: List[str] = []
    lock = Lock()
    print("Collecting new usernames…")
    while len(fresh) < BATCH_SIZE:
        idx, client = next(reddit_cycle)
        try:
            for c in client.subreddit("all").stream.comments(pause_after=-1):
                if c is None:
                    break
                if c.author and c.author.name not in seen_users:
                    with lock:
                        if c.author.name not in seen_users:
                            fresh.append(c.author.name)
                            if len(fresh) >= BATCH_SIZE:
                                break
        except prawcore.exceptions.ResponseException:
            time.sleep(1)

    total = len(fresh)
    print(f"Collected {total} users; fetching in parallel…")

    jobs = list(zip(cycle(range(1, len(reddit_clients) + 1)), fresh))
    all_users: List[Dict] = []
    all_comments: List[Dict] = []

    with ThreadPoolExecutor(max_workers=len(reddit_clients)) as exe:
        futures = {exe.submit(fetch_user_job, job): job for job in jobs}
        for i, fut in enumerate(as_completed(futures), start=1):
            idx, uname = futures[fut]
            res = fut.result()
            if res:
                user_row, comment_rows = res
                all_users.append(user_row)
                all_comments.extend(comment_rows)
                print(
                    f"[{i}/{total}] [#{idx}] ✓ u/{uname} – {len(comment_rows)} comments"
                )
            else:
                print(f"[{i}/{total}] [#{idx}] u/{uname} – failed")

    # Saves batch of user metadata and comments
    save_parquet(all_users, "users")
    save_parquet(all_comments, f"batch_{len(all_comments)}")


# ---------- Entry point ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["single", "batch"],
        default="single",
        help="single: one user every 30s; batch: BATCH_SIZE users at once",
    )
    args = parser.parse_args()
    if args.mode == "single":
        run_single()
    else:
        while True:
            run_batch()
            time.sleep(SLEEP_SECONDS / 10)


if __name__ == "__main__":
    main()
