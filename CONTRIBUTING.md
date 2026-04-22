# Contributing

Thanks for considering a contribution.

## Scope

This project focuses on a pipeline that turns bank SMS into categorized transactions and pushes them to Firefly III.

Good contribution areas:
- Add/adjust bank SMS parsing patterns
- Improve classification rules/taxonomy
- Improve robustness and diagnostics
- Add tests for parsing/classification/export

## Development

### Requirements
- Python 3.12+

### Local config
- Copy `.env.example` → `.env`
- Never commit `.env` or any `exports/` artifacts (they may contain personal data)

## Pull requests

- Keep changes focused (one feature/fix per PR)
- Update docs when behavior changes
- Add tests when you change parsing/classification logic

