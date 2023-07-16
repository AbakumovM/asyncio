import asyncio
import json
from datetime import datetime

import aiohttp
from more_itertools import chunked

from models import Base, Session, SwapiPeople, engine

MAX_CHUNK_SIZE = 10


async def get_field_data(url_list: list, title: str) -> str:
    if url_list:
        session = aiohttp.ClientSession()
        it_list = []
        for url in url_list:
            url = await session.get(url)
            json_data = await url.json()
            it_list.append(json_data[title])
        await session.close()
        return ",".join(it_list)
    return "No data available"


async def get_items(people_data: json) -> str:
    people_data["films"] = await get_field_data(people_data["films"], "title")
    people_data["species"] = await get_field_data(people_data["species"], "name")
    people_data["starships"] = await get_field_data(people_data["starships"], "name")
    people_data["vehicles"] = await get_field_data(people_data["vehicles"], "name")
    return people_data


async def get_people(id_people: str | int) -> str:
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{id_people}/")
    json_data = await response.json()
    await session.close()
    return json_data


async def insert_to_db(people_json_list: list):
    async with Session() as session:
        swapi_people_list = [
            SwapiPeople(
                **{
                    "birth_year": json_data["birth_year"],
                    "eye_color": json_data["eye_color"],
                    "films": json_data["films"],
                    "gender": json_data["gender"],
                    "hair_color": json_data["hair_color"],
                    "height": json_data["height"],
                    "homeworld": json_data["homeworld"],
                    "mass": json_data["mass"],
                    "name": json_data["name"],
                    "skin_color": json_data["skin_color"],
                    "species": json_data["species"],
                    "starships": json_data["starships"],
                    "vehicles": json_data["vehicles"],
                }
            )
            for json_data in people_json_list
        ]
        session.add_all(swapi_people_list)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    for ids_chunk in chunked(range(1, 83), MAX_CHUNK_SIZE):
        people_list = [get_people(j) for j in ids_chunk]
        people_json_list = await asyncio.gather(*people_list)
        people_correct_list = [
            get_items(people)
            for people in people_json_list
            if "detail" not in people.keys()
        ]
        people_list_to_db = await asyncio.gather(*people_correct_list)
        asyncio.create_task(insert_to_db(people_list_to_db))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)
    await engine.dispose()


if __name__ == "__main__":
    start = datetime.now()
    asyncio.run(main())
    print(datetime.now() - start)
