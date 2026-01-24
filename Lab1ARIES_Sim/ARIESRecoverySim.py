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

TT = {}
DPT = {}

for entry in wal :
    lastLSN = entry["LSN"]
    eType = entry["type"]

    if eType == "BEGIN" :
        tID = entry["tx"]
        TT[tID] = {
            "status": "active",
            "lastLSN": lastLSN
        }

    elif eType == "UPDATE" :
        tID = entry["tx"]
        TT[tID]["lastLSN"] = lastLSN
        # Build DPT 
        # Get the page and it's associated lsn then add only if first occurance
        page = entry["page"]
        if page not in DPT :
            DPT[page] = lastLSN

    elif eType == "COMMIT" :
        tID = entry["tx"]
        TT[tID]["status"] = "committed"
        TT[tID]["lastLSN"] = lastLSN

    elif eType == "END" :
        tID = entry["tx"]
        del TT[tID]

    #if tID == "CHECKPOINT" ADD CHECKPOINT IMPLEMENATION LATER TODO

# Task 3 : Preform the Redo; only when pageLsn < LSN of the update log record
redoneLSNS = ""
if DPT:
    rStart = min(DPT.values())

    for entry in wal :
        logLSN = entry["LSN"]

        if logLSN < rStart :
            continue

        if entry["type"] == "UPDATE" :
            page = entry["page"]

            if dsnap[page]["pageLSN"] < logLSN:
                dsnap[page]["pageLSN"] = logLSN
                dsnap[page]["value"] = entry["after"]
                redoneLSNS += ("\t" + "REDO:" + str(logLSN) + "\n")

# Task 4: Preform UNDO & use/find losers trans
losers = {tx for tx, entry in TT.items() if entry["status"] == "active"}
winners = {tx for tx, entry in TT.items() if entry["status"] == "committed"}
undoneLSN = ""
for entry in reversed(wal) :
    if entry["type"] == "UPDATE" and entry["tx"] in losers:
        dsnap[entry["page"]]["value"] = entry["before"]
        dsnap[entry["page"]]["pageLSN"] = entry["LSN"]  #Might Be Wrong
        undoneLSN += ("\t" + "UNDO:" + str(entry["LSN"]) + "\n")

with open("disk_pages_after.json", "w") as file :
    json.dump(dsnap, file, indent=2)

## Show desired output
if winners : 
    print("Winners: ", winners, "\n")
else:
    print("No Winning Transactions\n")

if losers : 
    print("Losers : ", losers, "\n")
else:
    print("No Losing Transactions\n")

if redoneLSNS :
    print("REDONE LSN Records:\n" + redoneLSNS)
else :
    print("No updates were redone")

if undoneLSN :
    print("UNDONE LSN Records:\n" + undoneLSN)
else :
    print("No updates were undone")








