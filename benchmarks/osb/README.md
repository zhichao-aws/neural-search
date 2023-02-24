# OpenSearch Benchmarks for Neural Search

## Overview

This directory contains code and configurations to run Neural-Search benchmarking 
workloads using OpenSearch Benchmarks.

The [extensions](extensions) directory contains common code shared between 
procedures. The [procedures](procedures) directory contains the individual 
test procedures for this workload.

## Getting Started

### OpenSearch Benchmarks Background

OpenSearch Benchmark is a framework for performance benchmarking an OpenSearch 
cluster. For more details, checkout their 
[repo](https://github.com/opensearch-project/opensearch-benchmark/). 

Before getting into the benchmarks, it is helpful to know a few terms:
1. Workload - Top level description of a benchmark suite. A workload will have a `workload.json` file that defines different components of the tests 
2. Test Procedures - A workload can have a schedule of operations that run the test. However, a workload can also have several test procedures that define their own schedule of operations. This is helpful for sharing code between tests
3. Operation - An action against the OpenSearch cluster
4. Parameter source - Producers of parameters for OpenSearch operations
5. Runners - Code that actually will execute the OpenSearch operations

### Setup

OpenSearch Benchmarks requires Python 3.8 or greater to be installed. One of 
the easier ways to do this is through Conda, a package and environment 
management system for Python.

First, follow the 
[installation instructions](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html) 
to install Conda on your system.

Next, create a Python 3.8 environment:
```
conda create -n neural-osb python=3.8
```

After the environment is created, activate it:
```
conda activate neural-osb
```

Lastly, clone the Neural Search repo and install all required python packages:
```
git clone https://github.com/opensearch-project/neural-search.git
cd neural-search/benchmarks/osb
pip install -r requirements.txt
```

After all of this completes, you should be ready to run your first benchmark!

### Running a benchmark

Before running a benchmark, make sure you have the endpoint of your cluster and
  the machine you are running the benchmarks from can access it. 
 Additionally, ensure that all data has been pulled to the client.


Once the parameters are set, set the URL and PORT of your cluster and run the 
command to run the test procedure. 

```
export URL=
export PORT=
export PARAMS_FILE=
export PROCEDURE={ml-predict | trec-covid | trec-covid-query}
export WORKLOAD_JSON={"./workload.json" | "./ml_predict_workload.json" | "./query_workload.json"}

opensearch-benchmark execute_test \ 
    --target-hosts $URL:$PORT \ 
    --workload-path $WORKLOAD_JSON \ 
    --workload-params ${PARAMS_FILE} \
    --test-procedure=${PROCEDURE} \
    --pipeline benchmark-only --kill-running-processes
```

## Current Procedures

1. ML Commons Predict API Testing : This uses an already uploaded model on the cluster to run the Predict API call of ML Commons plugin.
2. Trec-Covid: This is a complete testing of the Neural Search Plugin via Trec-Covid Data set. It indexes the data first and then do the query.
3. Trec-Covid-Query: This is only a ```neural``` query clause testing procedure which assumes that you have already indexed the data in the cluster and is doing the Query Benchmarking.

## Setup
The complete setup for all files and data sets is yet to be added. But for testing I have added some sample files that act as a reference for creating larger data sets.

More details coming soon.
