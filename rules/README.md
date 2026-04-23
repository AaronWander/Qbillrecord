# SMS bookkeeping rules (95588 / ICBC)

This directory contains parsing + classification rules that convert SMS messages into structured transactions, which can then be exported/pushed to bookkeeping systems such as Firefly III.

## Quickstart: create your own rules file

1) Copy the runnable example JSON:

```sh
cp rules/icbc_95588_rules.example.json rules/icbc_95588_rules.json
```

2) Edit `rules/icbc_95588_rules.json` to match your real message template(s).

3) Ensure your pipeline points at the rules file:

- `pipelines/qbillrecord_icbc95588_inc.yml` → `classifier.rules_path: rules/icbc_95588_rules.json`

Tip: `rules/icbc_95588_rules.example.jsonc` contains the same content with inline comments, but it is **not valid JSON** and cannot be loaded by the CLI directly.

## 1) Data source (macOS iMessage / `chat.db`)

Example query to view recent messages from sender `95588`:

```sh
sqlite3 ~/Library/Messages/chat.db "
SELECT
  datetime(message.date / 1000000000 + strftime('%s', '2001-01-01'), 'unixepoch', 'localtime') AS date,
  handle.id AS sender,
  message.text
FROM message
JOIN handle ON message.handle_id = handle.ROWID
WHERE handle.id LIKE '%95588%'
ORDER BY message.date DESC;"
```

## 2) Export to JSONL

Run the pipeline to export messages and generate artifacts under `exports/runs/<timestamp>/`:

```sh
cd <repo-root>
python3 -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

## 3) Export to Firefly III JSONL

The same pipeline produces a Firefly III JSONL file (per-run) that contains `POST /api/v1/transactions` payloads:

```sh
python3 -m qbillrecord run --pipeline pipelines/qbillrecord_icbc95588_inc.yml
```

## 4) Rule file

- Copy `rules/icbc_95588_rules.example.json` to `rules/icbc_95588_rules.json` and edit it for your bank/SMS template.
  - A commented variant is provided as `rules/icbc_95588_rules.example.jsonc` (for humans only).

- `rules/icbc_95588_rules.json`
  - `ignore_if_text_matches_any`: keywords for non-transaction messages to ignore (OTP/security/login alerts, etc.)
  - `transaction_patterns`: regex patterns for parsing transaction messages (expense/income, channel, merchant, amount, balance, card last4, timestamp)
  - `category_taxonomy`: category list/taxonomy
  - `category_rules`: merchant/biz-type based category suggestions; unresolved items remain “needs review”
  - `tags_rules`: tags derived from channel/provider (e.g. Tenpay/Alipay/Pinduoduo)

## 5) Pattern requirements (what the parser expects)

The parser uses `transaction_patterns[].regex` to extract a transaction via named capture groups.

Rules patterns should use **PCRE-style** named groups: `(?<name>...)` (the code rewrites them to Python `(?P<name>...)`).

Recommended groups (most templates should provide these):

- `card_last4`: last 4 digits (optional if not present)
- `month`, `day`, `time`: local timestamp pieces
- `direction`: raw direction label (e.g. `Expense|Income` or Chinese equivalents)
- `amount`: numeric amount (comma separators are allowed)
- `balance`: numeric balance (optional but useful)
- `biz_type`, `channel`, `merchant`: bracket details (optional; you can also provide a single `bracket` group and let the code heuristically split it)

If a message does not match any pattern, it will be recorded as "unparsed" in the run artifacts.

## 5) Covered message shapes (examples)

This ruleset is designed for the message formats commonly used by sender `95588` and covers patterns such as:

- Balance change (expense/income)
- Non-transaction notifications that should be ignored (OTP/security reminders, etc.)

Note: rule files should match your real message templates and language (Chinese, English, etc.).
