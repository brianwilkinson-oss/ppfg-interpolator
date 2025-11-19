import json
from pathlib import Path

from scripts import update_datasets


def test_merge_records():
    base = {"name": "foo", "indexes": []}
    detail = {"indexes": [{"name": "time", "type": "time"}]}
    merged = update_datasets.merge_records(base, detail)
    assert merged["indexes"][0]["name"] == "time"
    assert merged["name"] == "foo"


def test_fetch_detail(monkeypatch):
    calls = []

    class DummyClient:
        async def get(self, url, timeout):
            class Resp:
                status_code = 200

                def raise_for_status(self):
                    pass

                def json(self):
                    return {"name": "foo", "indexes": [1]}

            calls.append(url)
            return Resp()

    client = DummyClient()
    detail = update_datasets.asyncio.run(update_datasets.fetch_detail(client, "foo"))
    assert detail["indexes"] == [1]
    assert calls


def test_dataset_path_conversion():
    assert update_datasets._dataset_path("corva#activities") == "corva/activities"
    assert update_datasets._dataset_path("plain") == "plain"
