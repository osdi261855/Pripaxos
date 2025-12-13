from copy import deepcopy
import json
import csv
import os
from itertools import islice

def read_json(path, roles):
    objs = []

    f = open(path, "r")

    js = json.load(f)["nodes"]
    for j in js:
        role = j["roles"]
        if role in roles:
            objs.append(j)

    f.close()
    return objs

def read_conf(path, key):
    f = open(path, "r")
    val = json.load(f)[key]
    f.close()
    return val

def aggregate_alias(path, variables, func):
    f = open(path, "r")
    reader = csv.DictReader(f)
    num_rows = sum(1 for _ in reader)

    f.seek(0)
    for row in islice(reader, 1, None, None):
        row["num_of_rows"] = num_rows
        variables = func(row, variables)
    f.close()
    return variables

def aggregate_protocol(path, variables, f):
    aliases = [x for x in os.listdir(path) if os.path.isdir(f"{path}/{x}")]
    for alias in aliases:
        variables = aggregate_alias(f"{path}/{alias}/transactions.csv", variables, f)
    return variables

def sorted_access(path, variables, f):
    alias_files = [open(f"{path}/{x}/transactions.csv") for x in os.listdir(path) if os.path.isdir(f"{path}/{x}")]
    alias_readers = [csv.DictReader(x) for x in alias_files]
    n = len(alias_readers)

    if n == 0:
        return variables

    #Get total number of records
    total_records = sum(1 for _ in alias_readers[0])
    iterators = []
    for i in range(1, n):
        alias_files[i - 1].seek(0)
        iterators.append(islice(alias_readers[i - 1], 1, None, None))
        total_records += sum(1 for _ in alias_readers[i])
    alias_files[n - 1].seek(0)
    iterators.append(islice(alias_readers[n - 1], 1, None, None))

    counts = [0 for _ in alias_readers]
    rows = [next(iter, None) for iter in iterators]
    latencies = [float(row["latency"]) if row is not None else float("inf") for row in rows]
    idx = latencies.index(min(latencies))
    counts[idx] += 1
    while sum(counts)<=total_records:
        row = rows[idx]
        row["num_of_rows"] = total_records
        variables = f(row, variables)

        rows = [next(iter, None) if i == idx else rows[i] for i, iter in enumerate(iterators)]
        latencies = [float(row["latency"]) if row is not None else float("inf") for row in rows]
        idx = latencies.index(min(latencies))
        counts[idx] += 1

    for f in alias_files:
        f.close()
    return variables

#Testing
if __name__ == "__main__":
    print(aggregate_alias("out/exp1/swiftpaxos/client1/transactions.csv", [0], lambda row, variables: [variables[0] + 1]))