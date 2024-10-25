import os
import json
import subprocess

from opensearchpy import OpenSearch

INGEST_WORKLOAD_FILE = "ingest_workload.json"
RUN_BENCHMARK_COMMAND = f"""
opensearch-benchmark execute-test --target-host $HOSTS \
        --pipeline benchmark-only \
        --workload-path ./{INGEST_WORKLOAD_FILE}
"""
get_client_options = (
    lambda auth: ""
    if auth is None
    else f""" --client-options=basic_auth_user:'{auth[0]}',basic_auth_password:'{auth[1]}',use_ssl:true,verify_certs:false" """
)


def drop_lines_then_count(fp, target=1e8):
    target = int(target)

    line_num = 0
    command = ["wc", "-l", fp]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode == 0:
        line_num = int(result.stdout.split(" ")[0])
    else:
        raise Exception(result.stderr)

    print(line_num, target)
    if line_num < target:
        return line_num

    command = f"head -n {target} {fp} > temp.json && mv temp.json {fp}"
    print(command)
    subprocess.run(command, shell=True)
    return int(target)


def prepare_workload_json(documents_fp, index_name, limit=1e8):
    workload_template = {
        "version": 2,
        "description": "ingest documents",
        "indices": [{"name": "test"}],
        "corpora": [
            {
                "name": "test",
                "documents": [
                    {
                        "source-file": "documents.json",
                        "document-count": 1942934,
                        "uncompressed-bytes": 16520902783,
                    }
                ],
            }
        ],
        "schedule": [
            {
                "operation": {
                    "operation-type": "cluster-health",
                    "request-params": {"wait_for_status": "green"},
                    "retry-until-success": True,
                }
            },
            {"operation": {"operation-type": "bulk", "bulk-size": 1000}, "clients": 16},
        ],
    }
    lines_num = drop_lines_then_count(documents_fp, target=limit)
    workload_template["indices"][0]["name"] = index_name
    workload_template["corpora"][0]["documents"][0]["source-file"] = documents_fp
    workload_template["corpora"][0]["documents"][0]["document-count"] = lines_num
    workload_template["corpora"][0]["documents"][0][
        "uncompressed-bytes"
    ] = os.path.getsize(documents_fp)
    with open(INGEST_WORKLOAD_FILE, "w") as f:
        json.dump(workload_template, f, indent=4)


def ingest(index_name, file_path, auth=None):
    prepare_workload_json(file_path, index_name)

    command = f"""
    {RUN_BENCHMARK_COMMAND}{get_client_options(auth)}
    """

    print(command)
    subprocess.run(command, shell=True)

    client = OpenSearch(
        hosts=os.getenv("HOSTS").split(","),
        http_auth=None if os.getenv("AUTH") is None else os.getenv("AUTH").split(","),
        verify_certs=False,
        ssl_show_warn=False,
    )
    if client is not None:
        try:
            client.indices.refresh(index=index_name, request_timeout=600)
            client.indices.forcemerge(
                index=index_name, params={"max_num_segments": 1}, request_timeout=600
            )
        except:
            print("failed to refresh/forcemerge")
