Place exported Temporal workflow history JSON files in this directory to enable replay testing.

Supported fixture formats:

- Raw history JSON exported from Temporal tools/UI
- Wrapper JSON containing:
  - `workflow_id`
  - `history`

Run replay verification with:

```bash
pytest -q tests/test_replay.py
```
