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
import asyncio
from aiohttp import ClientSession
from modules.validation.player import Player, PlayerDoesNotExistException
from modules.api.highscore_api import HighscoreApi
from modules.api.runemetrics_api import RuneMetricsApi
from modules.scraper import Scraper
import time

# Test lookup_hiscores with an existing player
@pytest.mark.asyncio
async def test_lookup_hiscores_existing_player():
    scraper = Scraper()
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
    
    player_data, highscore_data = await scraper.lookup_hiscores(player, session)

    # Assert that the returned data is of the correct types
    assert isinstance(player_data, Player)
    assert isinstance(highscore_data, dict)

    # Optionally, you can perform other assertions on the Player and highscore data
    assert player_data.name == "extreme4all"
    assert player_data.label_jagex == 0
    assert highscore_data["Player_id"] == 1
    assert player_data.confirmed_player == 1

# Test lookup_hiscores with a non-existing player
@pytest.mark.asyncio
async def test_lookup_hiscores_non_existing_player():
    scraper = Scraper()
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
    
    player_data, highscore_data = await scraper.lookup_hiscores(player, session)

    # Assert that the returned data is of the correct types
    assert isinstance(player_data, Player)
    assert highscore_data is None

    # Check that the player_data reflects the non-existing player scenario
    assert player_data.possible_ban == 1
    assert player_data.confirmed_player == 0

    # Optionally, you can perform other assertions on the Player and highscore data
    assert player_data.name == "z12rpksw9f02q"

@pytest.mark.asyncio
async def test_rate_limit():
    scraper = Scraper()

    # Save the current time before calling rate_limit
    start_time = time.time()
    
    for i in range(scraper.history.maxlen):
        if i % 10 == 0:
            print(i)
        await scraper.rate_limit()

    end_time = time.time()

    assert end_time - start_time >= 60