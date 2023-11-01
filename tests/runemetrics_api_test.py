import os
import sys

# Get the path to the parent directory (one folder up from the test file)
parent_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(parent_directory)

# Append the "src" folder to the Python path
src_folder = os.path.join(parent_directory, "src")
print(src_folder)

sys.path.append(src_folder)

import pytest
from aiohttp import ClientSession

from modules.api.runemetrics_api import InvalidResponse, RuneMetricsApi
from modules.validation.player import Player


@pytest.mark.asyncio
async def test_lookup_existing_player():
    api = RuneMetricsApi()
    player = Player(
        id=1,
        name="extreme4all",
        created_at="2021-09-04T23:42:37",
        updated_at="2023-07-13T01:28:37",
        possible_ban=0,
        confirmed_ban=0,
        confirmed_player=1,
        label_id=0,
        label_jagex=0,
    )
    session = ClientSession()

    data = await api.lookup_runemetrics(player=player.copy(), session=session)
    print(data)

    assert isinstance(data, Player)

    # Optionally, you can perform other assertions on the Player object if needed
    assert data.name == "extreme4all"
    assert data.label_jagex == 0

    await session.close()


@pytest.mark.asyncio
async def test_lookup_not_existing_player():
    api = RuneMetricsApi()
    player = Player(
        id=1,
        name="z12rpksw9f02q",
        created_at="2021-09-04T23:42:37",
        updated_at="2023-07-13T01:28:37",
        possible_ban=0,
        confirmed_ban=0,
        confirmed_player=1,
        label_id=0,
        label_jagex=0,
    )
    session = ClientSession()

    data = await api.lookup_runemetrics(player=player.copy(), session=session)
    print(data)

    assert isinstance(data, Player)

    # Optionally, you can perform other assertions on the Player object if needed
    assert data.name == "z12rpksw9f02q"
    assert data.label_jagex == 1
    assert (
        data.updated_at != player.updated_at
    ), f"player must be updated, existing: {player.updated_at} received: {data.updated_at}"

    await session.close()
