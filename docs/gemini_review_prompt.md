# Gemini Review Prompt

Use Gemini only on entries emitted by `scripts/audit_dataset.py`. Do not review the full dataset blindly.

## Input

You will receive one `review_queue.json` item plus optional external evidence snippets.

Your job:

1. Decide whether the listing itself looks valid.
2. Decide whether the aliases look plausible.
3. Identify which fields require human or external-source verification.
4. Return structured JSON that matches `docs/gemini_review_response.schema.json`.

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
- Do not invent ISINs, exchanges, or company names.

## Suggested wrapper prompt

```text
Review this flagged ticker-database entry.

Return JSON only.
Follow the schema exactly.
Be conservative: if evidence is insufficient, use needs_human or unknown.

Entry:
<paste one review_queue.json item here>

External evidence:
<paste exchange listing text, issuer page text, or multi-source lookup evidence here>
```
