import os
import csv

import sys
sys.path.insert(0, "./scripts")
from utils import aggregate_protocol

# Outputs a csv file 

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"

#Compute average per protocol e.g. Per aws region
def compute_protocol_avgs(exp, protocol):
    folder_path = f"{ROOT_PATH}/exp{exp}/{protocol}"
    alias_folder = [x for x in os.listdir(folder_path)]

    def iterative_avg(row, variables):
        avg = variables[0]
        count = variables[1]
        return [(avg*count + float(row["latency"]))/(count + 1), count + 1]

    avgs = {}
    for i in range(len(alias_folder)):
        avg, count = aggregate_protocol(f"{ROOT_PATH}/exp{exp}/{protocol}", [0,0], iterative_avg)
        avgs[alias_folder[i]] = {"avg": avg, "count": count}
    return avgs

experiment_number = 3
folder_path = f"{ROOT_PATH}/exp{experiment_number}"
protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]

field_names = ["protocol", "alias", "req_count", "latency"]
f = open(f"{folder_path}/agg.csv", "w")
writer = csv.DictWriter(f, fieldnames=field_names)
writer.writeheader()
for protocol in protocols:
    avgs = compute_protocol_avgs(experiment_number, protocol)
    for alias in avgs:
        alias_avg = avgs[alias]
        writer.writerow({"protocol": protocol, "alias": alias, "req_count": alias_avg["count"], "latency": alias_avg["avg"]})
f.close() 
