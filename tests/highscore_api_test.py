import pytest
from aiohttp import ClientSession

from src.modules.api.highscore_api import (
    HighscoreApi,
    PlayerDoesNotExistException,
)
from src.modules.validation.player import Player


@pytest.mark.asyncio
async def test_lookup_existing_player():
    api = HighscoreApi()
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
    async with ClientSession() as session:
        data = await api.lookup_hiscores(player=player, session=session)
    assert isinstance(data, dict), f"data must be of type dict, received {type(data)}"
    assert data["Player_id"] == player.id, "player id must be equal"
    print(data)


@pytest.mark.asyncio
async def test_lookup_not_existing_player():
    api = HighscoreApi()
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

    # Use pytest.raises to check if the expected exception is raised
    with pytest.raises(PlayerDoesNotExistException) as exc_info:
        async with ClientSession() as session:
            data = await api.lookup_hiscores(player=player, session=session)

    # Now, you can assert other properties of the exception if needed
    assert "does not exist" in str(exc_info.value)


@pytest.mark.asyncio
async def test_parse_hiscore_name():
    # Test the _parse_hiscore_name function.

    # Create a HighscoreApi instance.
    api = HighscoreApi()

    # Test various name replacements.
    assert api._parse_hiscore_name("Attack") == "attack"
    assert api._parse_hiscore_name("Clue Scrolls (all)") == "cs_all"
    assert api._parse_hiscore_name("Fishing") == "fishing"


@pytest.mark.asyncio
async def test_parse_hiscore_stat():
    # Test the _parse_hiscore_stat function.

    # Create a HighscoreApi instance.
    api = HighscoreApi()

    # Test valid stat values.
    assert api._parse_hiscore_stat(100) == 100
    assert api._parse_hiscore_stat(-1) == 0
