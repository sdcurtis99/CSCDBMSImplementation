import json

# Task 1 : Read WAL and disk snapshot

wal = []
with open("wal.jsonl", "r") as file:
    for line in file:
        wal.append(json.loads(line))

with open("disk_pages.json", "r") as file:
    dsnap = json.load(file)

# Task 2 : ANALYSIS PHASE

TT = {}
DPT = {}

last_checkpoint = None
for entry in reversed(wal):
    if entry["type"] == "CHECKPOINT":
        last_checkpoint = entry
        break

start_index = 0

if last_checkpoint:
    DPT = dict(last_checkpoint["DPT"])
    for tx, info in last_checkpoint["TT"].items():
        TT[tx] = {
            "status": "active",  
            "lastLSN": info["lastLSN"]
        }
    start_index = wal.index(last_checkpoint) + 1

for entry in wal[start_index:]:
    lsn = entry["LSN"]
    etype = entry["type"]

    if etype == "BEGIN":
        TT[entry["tx"]] = {"status": "active", "lastLSN": lsn}

    elif etype == "UPDATE":
        tx = entry["tx"]
        page = entry["page"]
        TT[tx]["lastLSN"] = lsn
        if page not in DPT:
            DPT[page] = lsn

    elif etype == "COMMIT":
        tx = entry["tx"]
        TT[tx]["status"] = "committed"
        TT[tx]["lastLSN"] = lsn

    elif etype == "END":
        del TT[entry["tx"]]

# Task 3 : REDO PHASE

redoneLSNS = ""

if DPT:
    redo_start = min(DPT.values())

    for entry in wal:
        lsn = entry["LSN"]

        if lsn < redo_start:
            continue

        if entry["type"] == "UPDATE":
            page = entry["page"]
            if page in DPT and dsnap[page]["pageLSN"] < lsn:
                dsnap[page]["value"] = entry["after"]
                dsnap[page]["pageLSN"] = lsn
                redoneLSNS += f"\tREDO:{lsn}\n"

# Task 4 : UNDO PHASE 

losers = {tx for tx, entry in TT.items() if entry["status"] == "active"}
winners = {tx for tx, entry in TT.items() if entry["status"] == "committed"}

undoneLSN = ""
clrLSNs = ""

nextCLRLSN = max(entry["LSN"] for entry in wal) + 1

for entry in reversed(wal):
    if entry["type"] == "UPDATE" and entry["tx"] in losers:
        page = entry["page"]

        dsnap[page]["value"] = entry["before"]
        dsnap[page]["pageLSN"] = nextCLRLSN  

        undoneLSN += f"\tUNDO:{entry['LSN']}\n"
        clrLSNs += (
            f"\tCLR: tx={entry['tx']} "
            f"page={page} "
            f"undoLSN={entry['LSN']} "
            f"CLR_LSN={nextCLRLSN}\n"
        )

        nextCLRLSN += 1


with open("disk_pages_after.json", "w") as file:
    json.dump(dsnap, file, indent=2)


print("\nWinners:", winners if winners else "No Winning Transactions")
print("Losers:", losers if losers else "No Losing Transactions\n")

if TT:
    print("Transaction Table:")
    print(TT, "\n")

if DPT:
    print("Dirty Page Table:")
    print(DPT, "\n")

if redoneLSNS:
    print("REDONE LSN Records:")
    print(redoneLSNS, end="")
else:
    print("No updates were redone\n")

if undoneLSN:
    print("UNDONE LSN Records:")
    print(undoneLSN, end="")
else:
    print("No updates were undone\n")

if clrLSNs:
    print("CLR Records:")
    print(clrLSNs)
else:
    print("No CLR records generated")
