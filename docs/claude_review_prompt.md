# Claude Review Prompt

Use local Claude CLI only on entries emitted by `scripts/audit_dataset.py`. Do not review the full dataset blindly.

## Input

You will receive one or more `review_queue.json` items plus optional external evidence snippets.

Your job:

1. Decide whether each listing itself looks valid.
2. Decide whether the aliases look plausible.
3. Identify which fields require human or external-source verification.
4. Return structured JSON that matches `docs/review_response.schema.json`.

## Review rules

- Treat exchange-specific evidence and ISIN-based evidence as stronger than fuzzy name matches.
- Prefer `needs_human` over guessing.
- Remove aliases that are obvious products, celebrities, wrapper terms, or generic/common words.
- Keep aliases that are strong lexical matches to the issuer/fund name or clear listing identifiers.
- If ticker existence cannot be validated from the provided evidence, set `ticker_exists` to `"unknown"`.
- If the entry looks like a wrapper, receipt, note, warrant, right, or preferred instrument that should not be in the stock universe, prefer `drop_entry`.

## Output expectations

- Keep explanations short and evidence-based.
- Use `alias_actions` only for aliases that need action.
- Use `metadata_actions` only for specific field changes.
- Prefer `stock_sector` for stock GICS-sector fixes and `etf_category` for ETF category fixes; `sector` is the legacy compatibility field.
- Do not invent ISINs, exchanges, or company names.
