import json
from pprint import pprint

# Task 1 : Read in and store the json data for the wal and disk_pages snapshot

wal = []
with open("wal.jsonl", "r") as file :
    for line in file:
        wal.append(json.loads(line))
#pprint(wal)

dsnap = {}
with open("disk_pages.json", "r") as file :
    dsnap = json.load(file)
#pprint(dsnap)

# Task 2 : Preform the analysis, detmine winner/losers construct DPT and TT
# 1st Implementation will be without the logic for checkpoints

