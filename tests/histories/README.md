Recorded Temporal workflow history JSON files in this directory are replayed in CI.
Keep at least one fixture here so replay coverage cannot silently disappear.

Add exported production histories here when workflow command shapes change.

Supported fixture formats:

- Raw history JSON exported from Temporal tools/UI
- Wrapper JSON containing:
  - `workflow_id`
  - `history`

Run replay verification with:

```bash
pytest -q tests/test_replay.py
```
