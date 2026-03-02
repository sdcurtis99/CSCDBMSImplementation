"""
CSC 468 - Simulated Cost-Based Query Optimizer
"""

import json
import sys
import math

SEP = "=" * 60


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_json(path):
    with open(path) as f:
        return json.load(f)

def load_relations(path):
    raw = load_json(path)
    page_size = raw["page_size"]
    relations = {}
    for name, body in raw["relations"].items():
        schema = body["schema"]
        pages  = [[dict(zip(schema, tup)) for tup in page] for page in body["pages"]]
        relations[name] = {"schema": schema, "pages": pages}
    return page_size, relations


# ---------------------------------------------------------------------------
# Hash Index
# ---------------------------------------------------------------------------

HASH_LOOKUP_COST = 1

INDEX_TARGETS = [
    ("Student", "major"),
    ("Student", "sid"),
    ("Enroll",  "sid"),
]

def build_indexes(relations):
    indexes = {}
    for rel_name, attr in INDEX_TARGETS:
        bucket = {}
        for page_idx, page in enumerate(relations[rel_name]["pages"]):
            for tuple_idx, tup in enumerate(page):
                bucket.setdefault(tup[attr], []).append((page_idx, tuple_idx))
        indexes.setdefault(rel_name, {})[attr] = bucket
    return indexes

def index_lookup(relations, indexes, rel_name, attr, value):
    locations  = indexes[rel_name][attr].get(value, [])
    pages      = relations[rel_name]["pages"]
    seen_pages = set()
    result     = []
    for page_idx, tuple_idx in locations:
        seen_pages.add(page_idx)
        result.append(pages[page_idx][tuple_idx])
    return result, len(seen_pages)


# ---------------------------------------------------------------------------
# Query Tree Utilities
# ---------------------------------------------------------------------------

def get_children(node):
    if "child" in node:  return [node["child"]]
    if "left"  in node:  return [node["left"], node["right"]]
    return []

def get_relations(node):
    if node["op"] == "Scan":
        return {node["relation"]}
    return set().union(*[get_relations(c) for c in get_children(node)])

def fmt_node(node):
    op = node["op"]
    if op == "Scan":    return f"Scan({node['relation']})"
    if op == "Project": return f"Project({', '.join(node['attrs'])})"
    if op == "Join":    return f"Join({node['condition'][0]} = {node['condition'][2]})"
    if op == "Select":
        parts = [f"{node['predicate'][i]} = {node['predicate'][i+2]!r}"
                 for i in range(0, len(node["predicate"]), 4)]
        return f"Select({' AND '.join(parts)})"
    return op

def print_tree(node, depth=0):
    print("  " * depth + fmt_node(node))
    for child in get_children(node):
        print_tree(child, depth + 1)


# ---------------------------------------------------------------------------
# Logical Rewriting
# ---------------------------------------------------------------------------

def rewrite(node):

    if "child" in node:
        node = {**node, "child": rewrite(node["child"])}
    elif "left" in node:
        node = {**node, "left": rewrite(node["left"]), "right": rewrite(node["right"])}

    if node["op"] == "Select" and node["child"]["op"] == "Select":
        merged = node["predicate"] + ["AND"] + node["child"]["predicate"]
        return rewrite({"op": "Select", "predicate": merged, "child": node["child"]["child"]})

    if node["op"] == "Select" and node["child"]["op"] == "Project":
        return {
            "op": "Project",
            "attrs": node["child"]["attrs"],
            "child": rewrite({
                "op": "Select",
                "predicate": node["predicate"],
                "child": node["child"]["child"]
            })
        }

    if node["op"] == "Project" and node["child"]["op"] == "Project":
        return rewrite({**node, "child": node["child"]["child"]})

    if node["op"] == "Select" and node["child"]["op"] == "Join":
        join = node["child"]
        rel  = node["predicate"][0].split(".")[0]
        if rel in get_relations(join["left"]):
            return {**join, "left":  {"op": "Select", "predicate": node["predicate"], "child": join["left"]}}
        if rel in get_relations(join["right"]):
            return {**join, "right": {"op": "Select", "predicate": node["predicate"], "child": join["right"]}}

    if node["op"] == "Project" and node["child"]["op"] == "Join":
        join   = node["child"]
        jl, jr = join["condition"][0], join["condition"][2]
        l_rels = get_relations(join["left"])
        r_rels = get_relations(join["right"])
        l_attrs = [a for a in node["attrs"] if a.split(".")[0] in l_rels]
        r_attrs = [a for a in node["attrs"] if a.split(".")[0] in r_rels]
        if jl not in l_attrs: l_attrs.append(jl)
        if jr not in r_attrs: r_attrs.append(jr)
        return {
            **join,
            "left": rewrite({"op": "Project", "attrs": l_attrs, "child": join["left"]}),
            "right": rewrite({"op": "Project", "attrs": r_attrs, "child": join["right"]})
        }

    return node


# ---------------------------------------------------------------------------
# Cardinality Estimation
# ---------------------------------------------------------------------------

def estimate(node, stats, page_size):
    op = node["op"]
    if op == "Scan":
        return stats[node["relation"]]["T"], stats[node["relation"]]["B"]
    if op == "Select":
        T, _ = estimate(node["child"], stats, page_size)
        for i in range(0, len(node["predicate"]), 4):
            rel, col = node["predicate"][i].split(".")
            T = T / stats[rel]["V"][col]
        return T, math.ceil(T / page_size)
    if op == "Project":
        T, _ = estimate(node["child"], stats, page_size)
        return T, math.ceil(T / page_size)
    if op == "Join":
        lT, _ = estimate(node["left"],  stats, page_size)
        rT, _ = estimate(node["right"], stats, page_size)
        l_rel, l_col = node["condition"][0].split(".")
        r_rel, r_col = node["condition"][2].split(".")
        T = lT * rT / max(stats[l_rel]["V"][l_col], stats[r_rel]["V"][r_col])
        return T, math.ceil(T / page_size)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def base_relation(plan):
    while plan["op"] == "Project":
        plan = plan["child"]
    return plan["relation"]


# ---------------------------------------------------------------------------
# Physical Plan Enumeration
# ---------------------------------------------------------------------------

def leaf_plans(node, stats, page_size, indexes):
    rel  = node["child"]["relation"] if node["op"] == "Select" else node["relation"]
    pred = node["predicate"]         if node["op"] == "Select" else None
    est_T, est_B = estimate(node, stats, page_size)

    plans = [{
        "op": "SeqScan",
        "relation": rel,
        "predicate": pred,
        "cost": stats[rel]["B"],
        "est_T": est_T,
        "est_B": est_B
    }]

    if pred:
        rel_name, col = pred[0].split(".")
        if rel_name in indexes and col in indexes[rel_name]:
            idx_cost = HASH_LOOKUP_COST + math.ceil(
                stats[rel_name]["T"] / stats[rel_name]["V"][col] / page_size)
            plans.append({
                "op": "IndexScan",
                "relation": rel,
                "predicate": pred,
                "cost": idx_cost,
                "est_T": est_T,
                "est_B": est_B
            })

    return plans


def enumerate_plans(node, stats, page_size, indexes):

    if node["op"] == "Project":
        child_plans = enumerate_plans(node["child"], stats, page_size, indexes)
        return [{
            "op": "Project",
            "attrs": node["attrs"],
            "child": p,
            "cost": p["cost"],
            "est_T": p["est_T"],
            "est_B": p["est_B"]
        } for p in child_plans]

    if node["op"] == "Join":
        lps = enumerate_plans(node["left"],  stats, page_size, indexes)
        rps = enumerate_plans(node["right"], stats, page_size, indexes)
        cond, plans = node["condition"], []

        join_T, join_B = estimate(node, stats, page_size)

        for lp in lps:
            for rp in rps:
                plans.append({
                    "op": "HashJoin",
                    "condition": cond,
                    "left": lp,
                    "right": rp,
                    "cost": lp["cost"] + rp["cost"],
                    "est_T": join_T,
                    "est_B": join_B
                })

                plans.append({
                    "op": "NestedLoopJoin",
                    "condition": cond,
                    "outer": lp,
                    "inner": rp,
                    "cost": lp["cost"] + lp["est_T"] * stats[base_relation(rp)]["B"],
                    "est_T": join_T,
                    "est_B": join_B
                })

                plans.append({
                    "op": "NestedLoopJoin",
                    "condition": cond,
                    "outer": rp,
                    "inner": lp,
                    "cost": rp["cost"] + rp["est_T"] * stats[base_relation(lp)]["B"],
                    "est_T": join_T,
                    "est_B": join_B
                })

        return plans

    return leaf_plans(node, stats, page_size, indexes)


# ---------------------------------------------------------------------------
# Plan Formatting Helpers
# ---------------------------------------------------------------------------

def fmt_access(plan):
    pred_str = ""
    if plan.get("predicate"):
        parts = [
            f"{plan['predicate'][i]}={plan['predicate'][i+2]!r}"
            for i in range(0, len(plan["predicate"]), 4)
        ]
        pred_str = ", pred=" + " AND ".join(parts)

    relation = plan.get("relation", "")
    return f"{plan['op']}({relation}{pred_str}, cost={plan['cost']:.1f}, est_T={plan['est_T']:.1f})"


def fmt_plan(plan, indent="     "):
    if plan["op"] == "Project":
        return (
            f"Project({', '.join(plan['attrs'])})\n"
            f"{indent}-> {fmt_plan(plan['child'], indent + '   ')}"
        )

    if plan["op"] == "HashJoin":
        return (
            f"HashJoin\n"
            f"{indent}left  = {fmt_plan(plan['left'], indent + '        ')}\n"
            f"{indent}right = {fmt_plan(plan['right'], indent + '        ')}"
        )

    if plan["op"] == "NestedLoopJoin":
        return (
            f"NestedLoopJoin\n"
            f"{indent}outer = {fmt_plan(plan['outer'], indent + '        ')}\n"
            f"{indent}inner = {fmt_plan(plan['inner'], indent + '        ')}"
        )

    return fmt_access(plan)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def apply_predicate(tup, predicate):
    return all(tup.get(predicate[i].split(".")[1]) == predicate[i + 2]
               for i in range(0, len(predicate), 4))

def execute(plan, relations, indexes):
    op = plan["op"]

    if op == "Project":
        child_tuples, child_io = execute(plan["child"], relations, indexes)
        attrs = [a.split(".")[1] for a in plan["attrs"]]
        result = [{k: t[k] for k in attrs if k in t} for t in child_tuples]
        return result, child_io

    if op == "SeqScan":
        pages = relations[plan["relation"]]["pages"]
        result = [tup for page in pages for tup in page
                  if plan["predicate"] is None or apply_predicate(tup, plan["predicate"])]
        return result, len(pages)

    if op == "IndexScan":
        rel_name, col = plan["predicate"][0].split(".")
        tuples, pages_read = index_lookup(relations, indexes, rel_name, col, plan["predicate"][2])
        return [t for t in tuples if apply_predicate(t, plan["predicate"])], HASH_LOOKUP_COST + pages_read

    if op == "HashJoin":
        left_tuples,  left_io  = execute(plan["left"],  relations, indexes)
        right_tuples, right_io = execute(plan["right"], relations, indexes)
        l_col = plan["condition"][0].split(".")[1]
        r_col = plan["condition"][2].split(".")[1]
        table = {}
        for tup in left_tuples:
            table.setdefault(tup[l_col], []).append(tup)
        result = [{**m, **tup} for tup in right_tuples for m in table.get(tup[r_col], [])]
        return result, left_io + right_io

    if op == "NestedLoopJoin":
        outer_tuples, outer_io    = execute(plan["outer"], relations, indexes)
        inner_tuples, one_scan_io = execute(plan["inner"], relations, indexes)
        outer_rel                 = base_relation(plan["outer"])
        cl_rel, cl_col = plan["condition"][0].split(".")
        cr_rel, cr_col = plan["condition"][2].split(".")
        o_col = cl_col if cl_rel == outer_rel else cr_col
        i_col = cr_col if cl_rel == outer_rel else cl_col
        result = [{**ot, **it} for ot in outer_tuples for it in inner_tuples if ot[o_col] == it[i_col]]
        return result, outer_io + len(outer_tuples) * one_scan_io


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 4:
        print("Usage: python optimizer.py relations.json statistics.json query.json")
        sys.exit(1)

    page_size, relations = load_relations(sys.argv[1])
    stats   = load_json(sys.argv[2])
    query   = load_json(sys.argv[3])
    indexes = build_indexes(relations)

    print(SEP); print("1. ORIGINAL LOGICAL PLAN");  print(SEP)
    print_tree(query)

    rewritten = rewrite(query)
    print(); print(SEP); print("2. REWRITTEN LOGICAL PLAN"); print(SEP)
    print_tree(rewritten)

    print(); print(SEP); print("3. ESTIMATED CARDINALITIES"); print(SEP)
    def print_estimates(node, depth=0):
        T, B = estimate(node, stats, page_size)
        print("  " * depth + f"{fmt_node(node)}  [est T={T:.1f}, B={B}]")
        for child in get_children(node):
            print_estimates(child, depth + 1)
    print_estimates(rewritten)

    print()
    plans = enumerate_plans(rewritten, stats, page_size, indexes)
    best  = min(plans, key=lambda p: p["cost"])

    print(SEP); print("4. CANDIDATE PHYSICAL PLANS"); print(SEP)
    for i, plan in enumerate(plans, 1):
        print(f"  Plan {i}:")
        print(f"{fmt_plan(plan)}")
        print(f"     total cost = {plan['cost']:.1f}\n")

    print(SEP); print("5. CHOSEN PLAN"); print(SEP)
    print(f"{fmt_plan(best)}")
    print(f"  Estimated cost = {best['cost']:.1f}")

    print(SEP); print("6. EXECUTION"); print(SEP)
    result, actual_io = execute(best, relations, indexes)
    print(f"  Estimated cost : {best['cost']:.1f} I/Os")
    print(f"  Actual I/O     : {actual_io} pages")

    print(SEP); print(f"7. RESULT TUPLES ({len(result)} rows)"); print(SEP)
    for tup in result:
        print(f"  {tup}")

if __name__ == "__main__":
    main()