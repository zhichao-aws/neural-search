import os
import asyncio
import aiohttp
from aiohttp import ClientTimeout


async def do_search(session, endpoint, query_body, post_process=None):
    url = endpoint
    body = query_body

    async with session.get(url, json=body, verify_ssl=False) as resp:
        response = await resp.json()
        if "error" in response:
            raise Exception(response["error"])

    hits = response["hits"]["hits"]
    if post_process is not None:
        hits = post_process(hits)
    return hits


async def batch_search(
    queries,
    index_name,
    get_query_lambda,
    post_process=None,
    interval=0.01,
):
    endpoints = os.getenv("HOSTS").split(",")
    endpoints_num = len(endpoints)

    timeout = ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        i = 0
        for query in queries:
            tasks.append(
                asyncio.create_task(
                    do_search(
                        session,
                        f"""{endpoints[i%endpoints_num]}/{index_name}/_search""",
                        get_query_lambda(query),
                        post_process=post_process,
                    )
                )
            )
            i += 1
            await asyncio.sleep(interval)

        try:
            result = await asyncio.gather(*tasks)
        except Exception as e:
            print("error happens when querying index ", index_name)
            print(e)
            return {"error": str(e)}
    return result


def search(queries: dict, index_name: str, batch_size: int = 50, query_lambda=None):
    # queries: List[[id, sparse_vector]]
    run_res = dict()
    for start_idx in range(0, len(queries), batch_size):
        end_idx = min(start_idx + batch_size, len(queries))
        data = queries[start_idx:end_idx]
        ids = [x[0] for x in data]
        queries_encoded = [x[1] for x in data]

        search_results = asyncio.run(
            batch_search(
                queries=queries_encoded,
                index_name=index_name,
                get_query_lambda=query_lambda,
                interval=0.001,
            )
        )

        for _id, res in zip(ids, search_results):
            run_res[_id] = {hit["_source"]["id"]: hit["_score"] for hit in res}

    for query_id, doc_dict in run_res.items():
        if query_id in doc_dict:
            doc_dict.pop(query_id)

    return run_res
