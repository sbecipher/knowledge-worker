import asyncio
import json
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from workflows import MarketDataWorkflow


def test_replay_recorded_histories() -> None:
    histories_dir = Path(__file__).parent / "histories"
    history_files = sorted(histories_dir.glob("*.json"))
    assert history_files, "No recorded workflow histories present for replay"

    async def _replay() -> None:
        replayer = Replayer(workflows=[MarketDataWorkflow])
        for history_file in history_files:
            payload = json.loads(history_file.read_text(encoding="utf-8"))
            workflow_id = payload.get("workflow_id") or history_file.stem
            history = payload.get("history", payload)
            await replayer.replay_workflow(
                WorkflowHistory.from_json(
                    workflow_id=workflow_id,
                    history=history,
                )
            )

    asyncio.run(_replay())
