import argparse
import json
import os
from pathlib import Path
from collections import deque
from copy import deepcopy

# INITIAL DATABASE STATE
INITIAL_DB_STATE = {
    "A": 100,
    "B": 100,
    "X": 0,
    "Y": 0
}

# UTILITIES
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cc", required=True, choices=["2pl", "mvcc"])
    parser.add_argument("--schedule", required=True)
    parser.add_argument("--out", required=True)
    return parser.parse_args()

def read_schedule(path):
    events = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events

def ensure_output_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def write_trace(trace, out_dir):
    with open(os.path.join(out_dir, "trace.jsonl"), "w") as f:
        for entry in trace:
            f.write(json.dumps(entry) + "\n")

def write_final_state(state, out_dir):
    with open(os.path.join(out_dir, "final_state.json"), "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")

def validate_event(e):
    if "t" not in e or "op" not in e:
        raise ValueError("Invalid event format")
    if e["op"] not in ["BEGIN", "R", "W", "COMMIT", "ABORT"]:
        raise ValueError("Invalid op")
    if e["op"] in ["R", "W"] and "item" not in e:
        raise ValueError("Missing item")
    if e["op"] == "W" and "value" not in e:
        raise ValueError("Missing value")

# STRICT 2PL ENGINE
class Strict2PL:
    def __init__(self):
        self.db = deepcopy(INITIAL_DB_STATE)
        self.lock_table = {}
        self.txn = {}
        self.wait_for = {}
        self.trace = []
        self.step = 0

    def log(self, entry):
        self.step += 1
        entry["step"] = self.step
        self.trace.append(entry)

    def ensure_lock(self, item):
        if item not in self.lock_table:
            self.lock_table[item] = {
                "granted": [],
                "queue": deque()
            }

    def compatible(self, item, mode, txn_id):
        for holder, hmode in self.lock_table[item]["granted"]:
            if holder == txn_id:
                continue
            if mode == "X" or hmode == "X":
                return False
        return True

    def add_wait_edges(self, txn_id, item):
        holders = [t for t,_ in self.lock_table[item]["granted"] if t != txn_id]
        self.wait_for.setdefault(txn_id,set()).update(holders)

    def request_lock(self, txn_id, item, mode, op_type):
        self.ensure_lock(item)
        entry = self.lock_table[item]
        held_mode = self.txn[txn_id]["locks"].get(item)

        # -------- UPGRADE --------
        if held_mode == "S" and mode == "X":
            others = [(t,m) for t,m in entry["granted"] if t != txn_id]
            if not others:
                entry["granted"] = [(t,m) for t,m in entry["granted"] if t != txn_id]
                entry["granted"].append((txn_id,"X"))
                self.txn[txn_id]["locks"][item] = "X"
                self.log({"event":"LOCK","item":item,"grant":"X","to":txn_id})
                return True
            else:
                entry["queue"].append((txn_id, mode, op_type))
                self.txn[txn_id]["status"] = "BLOCKED"
                self.add_wait_edges(txn_id,item)
                self.log({
                    "event":"OP",
                    "t":txn_id,
                    "op":op_type,
                    "item":item,
                    "result":"BLOCKED",
                    "why":f"waiting for X({item})"
                })
                self.detect_deadlock()
                return False

        # -------- NORMAL --------
        if self.compatible(item, mode, txn_id):
            entry["granted"].append((txn_id, mode))
            self.txn[txn_id]["locks"][item] = mode
            self.log({"event":"LOCK","item":item,"grant":mode,"to":txn_id})
            return True

        entry["queue"].append((txn_id, mode, op_type))
        self.txn[txn_id]["status"] = "BLOCKED"
        self.add_wait_edges(txn_id,item)

        self.log({
            "event":"OP",
            "t":txn_id,
            "op":op_type,
            "item":item,
            "result":"BLOCKED",
            "why":f"waiting for {mode}({item})"
        })
        self.detect_deadlock()
        return False

    def process_queue(self, item):
        entry = self.lock_table[item]
        q = entry["queue"]

        while q:
            txn_id, mode, op_type = q[0]

            if self.txn.get(txn_id,{}).get("status") != "BLOCKED":
                q.popleft()
                continue

            if self.compatible(item, mode, txn_id):
                q.popleft()

                if self.txn[txn_id]["locks"].get(item) == "S" and mode == "X":
                    entry["granted"] = [(t,m) for t,m in entry["granted"] if t != txn_id]

                entry["granted"].append((txn_id, mode))
                self.txn[txn_id]["locks"][item] = mode
                self.txn[txn_id]["status"] = "ACTIVE"

                self.wait_for.pop(txn_id, None)
                for t in self.wait_for:
                    self.wait_for[t].discard(txn_id)

                self.log({
                    "event":"UNBLOCK",
                    "t":txn_id,
                    "op":op_type,
                    "item":item
                })
            else:
                break

    def release_locks(self, txn_id):
        for item in list(self.lock_table.keys()):
            entry = self.lock_table[item]
            entry["granted"] = [(t,m) for t,m in entry["granted"] if t != txn_id]
            self.process_queue(item)

        self.wait_for.pop(txn_id,None)
        for t in self.wait_for:
            self.wait_for[t].discard(txn_id)

    def detect_deadlock(self):
        visited = set()
        stack = []

        def dfs(node):
            visited.add(node)
            stack.append(node)
            for n in self.wait_for.get(node,[]):
                if n not in visited:
                    cycle = dfs(n)
                    if cycle:
                        return cycle
                elif n in stack:
                    idx = stack.index(n)
                    return stack[idx:] + [n]
            stack.pop()
            return None

        for node in list(self.wait_for.keys()):
            if node not in visited:
                cycle = dfs(node)
                if cycle:
                    victim = max(cycle[:-1])
                    self.log({"event":"DEADLOCK","cycle":cycle,"victim":victim})
                    self.abort(victim)
                    return

    def abort(self, txn_id):
        self.txn[txn_id]["status"] = "ABORTED"
        self.txn[txn_id]["write_buffer"].clear()
        self.log({"event":"ABORT","t":txn_id})
        self.release_locks(txn_id)

    def execute(self,event):
        t = event["t"]
        op = event["op"]

        if t in self.txn and self.txn[t]["status"] == "ABORTED":
            return "aborted"

        if op == "BEGIN":
            self.txn[t] = {
                "status":"ACTIVE",
                "locks":{},
                "write_buffer":{}
            }
            self.log({"event":"OP","t":t,"op":"BEGIN","result":"OK"})
            return "ok"

        if self.txn[t]["status"] == "BLOCKED":
            return "blocked"

        if op == "R":
            item = event["item"]
            if not self.request_lock(t,item,"S","R"):
                return "blocked"
            val = self.txn[t]["write_buffer"].get(item,self.db.get(item,0))
            self.log({"event":"OP","t":t,"op":"R","item":item,"result":"OK","value_read":val})
            return "ok"

        if op == "W":
            item = event["item"]
            value = event["value"]
            if not self.request_lock(t,item,"X","W"):
                return "blocked"
            self.txn[t]["write_buffer"][item] = value
            self.log({"event":"OP","t":t,"op":"W","item":item,"value":value,"result":"OK"})
            return "ok"

        if op == "COMMIT":
            for item,value in self.txn[t]["write_buffer"].items():
                self.db[item] = value
            self.txn[t]["status"] = "COMMITTED"
            self.log({"event":"OP","t":t,"op":"COMMIT","result":"OK"})
            self.release_locks(t)
            return "ok"

        if op == "ABORT":
            self.abort(t)
            return "ok"

# MVCC SNAPSHOT ISOLATION
class MVCC_SI:
    def __init__(self):
        self.timestamp = 0
        self.version_store = {}
        for item,value in INITIAL_DB_STATE.items():
            self.version_store[item] = [{
                "value":value,
                "begin_ts":0,
                "end_ts":float("inf")
            }]
        self.txn = {}
        self.trace = []
        self.step = 0

    def log(self,entry):
        self.step += 1
        entry["step"] = self.step
        self.trace.append(entry)

    def next_ts(self):
        self.timestamp += 1
        return self.timestamp

    def visible(self,item,start_ts):
        for v in self.version_store[item]:
            if v["begin_ts"] <= start_ts < v["end_ts"]:
                return v
        return None

    def execute(self,event):
        t = event["t"]
        op = event["op"]

        if t in self.txn and self.txn[t]["status"] == "ABORTED":
            return

        if op == "BEGIN":
            self.txn[t] = {
                "status":"ACTIVE",
                "start_ts":self.next_ts(),
                "write_buffer":{}
            }
            self.log({"event":"OP","t":t,"op":"BEGIN","result":"OK"})
            return

        if op == "R":
            item = event["item"]
            if item in self.txn[t]["write_buffer"]:
                val = self.txn[t]["write_buffer"][item]
            else:
                v = self.visible(item,self.txn[t]["start_ts"])
                val = v["value"]
            self.log({"event":"OP","t":t,"op":"R","item":item,"result":"OK","value_read":val})
            return

        if op == "W":
            self.txn[t]["write_buffer"][event["item"]] = event["value"]
            self.log({"event":"OP","t":t,"op":"W","item":event["item"],"value":event["value"],"result":"OK"})
            return

        if op == "COMMIT":
            start_ts = self.txn[t]["start_ts"]
            for item in self.txn[t]["write_buffer"]:
                latest = self.version_store[item][-1]
                if latest["begin_ts"] > start_ts:
                    self.txn[t]["status"]="ABORTED"
                    self.log({"event":"ABORT","t":t,"reason":"write-write conflict"})
                    return
            commit_ts = self.next_ts()
            for item,value in self.txn[t]["write_buffer"].items():
                latest = self.version_store[item][-1]
                latest["end_ts"] = commit_ts
                self.version_store[item].append({
                    "value":value,
                    "begin_ts":commit_ts,
                    "end_ts":float("inf")
                })
            self.txn[t]["status"]="COMMITTED"
            self.log({"event":"OP","t":t,"op":"COMMIT","result":"OK"})
            return

# RUNNER
def run_2pl(events):
    engine = Strict2PL()

    blocked = {}      # txn_id -> blocked event
    deferred = {}     # txn_id -> list of deferred future events

    idx = 0
    while idx < len(events):
        e = events[idx]
        validate_event(e)
        t = e["t"]

        # If transaction aborted, ignore future ops
        if t in engine.txn and engine.txn[t]["status"] == "ABORTED":
            idx += 1
            continue

        # If transaction currently blocked, defer this event
        if t in blocked:
            deferred.setdefault(t, []).append(e)
            idx += 1
            continue

        result = engine.execute(e)

        if result == "blocked":
            blocked[t] = e

        idx += 1

        # Retry loop
        made_progress = True
        while made_progress:
            made_progress = False

            for txn_id in list(blocked.keys()):
                if engine.txn.get(txn_id, {}).get("status") == "ACTIVE":

                    # Retry original blocked operation
                    retry_event = blocked[txn_id]
                    result = engine.execute(retry_event)

                    if result == "ok":
                        blocked.pop(txn_id)

                        # Now execute deferred operations in order
                        for future_event in deferred.get(txn_id, []):
                            res2 = engine.execute(future_event)
                            if res2 == "blocked":
                                blocked[txn_id] = future_event
                                # remove executed portion
                                deferred[txn_id] = deferred[txn_id][
                                    deferred[txn_id].index(future_event)+1:
                                ]
                                break

                        else:
                            # all deferred executed successfully
                            deferred.pop(txn_id, None)

                        made_progress = True

                    else:
                        blocked[txn_id] = retry_event

    return engine.trace, engine.db

def run_mvcc(events):
    engine = MVCC_SI()
    for e in events:
        validate_event(e)
        engine.execute(e)
    final = {k:v[-1]["value"] for k,v in engine.version_store.items()}
    return engine.trace, final

def main():
    args = parse_arguments()
    events = read_schedule(args.schedule)
    ensure_output_dir(args.out)

    if args.cc == "2pl":
        trace, state = run_2pl(events)
    else:
        trace, state = run_mvcc(events)

    write_trace(trace,args.out)
    write_final_state(state,args.out)

if __name__ == "__main__":
    main()