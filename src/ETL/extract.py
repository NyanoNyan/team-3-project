import csv
import json
from os import remove

pathway = "../../data/2021-02-23-isle-of-wight.csv"
fieldnames = ["timestamp","store_name","customer_name","basket", "payment_type", "total_price","card_number"]


def extract(pathway):
    with open(pathway, mode='r') as csv_file:
        reader = csv.DictReader(csv_file, fieldnames=fieldnames)
        
        
        # for row in reader:
        #     print(json.dumps(row, indent=4, sort_keys=True))
        
        return list(reader)

print(extract(pathway))


