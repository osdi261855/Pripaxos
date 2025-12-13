import os
import csv

import sys
sys.path.insert(0, "./scripts")
from utils import aggregate_protocol, sorted_access


#Please procduce files with transactions for each alias (sorted) before using this
#Produce a csv file for a protocol in an experiment.
#csv: protocol, latency, percentage

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"

experiment_number = 2
folder_path = f"{ROOT_PATH}/exp{experiment_number}"
protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]

field_names = ["protocol", "latency", "percentage"]
protocol_f = open(f"{folder_path}/cdf.csv", "w")
writer = csv.DictWriter(protocol_f, fieldnames=field_names)
writer.writeheader()
for protocol in protocols:
    folder_path = f"{ROOT_PATH}/exp{experiment_number}/{protocol}"

    alias_folder = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]
    files = [f"{folder_path}/{alias}/transactions.csv" for alias in alias_folder]
    for i in range(10):
        percentage = (i+1) * 10

        def agg(row, variables):
            return [row["latency"] if (variables[1] + 1)/row["num_of_rows"] * 100 <= percentage else variables[0], variables[1] + 1]
        latency, count = sorted_access(folder_path, [-1, 0], agg)
        writer.writerow({"protocol": protocol, "latency": latency, "percentage": percentage})
protocol_f.close()

