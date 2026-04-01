import pandas as pd
import sys

# Load outputs
txn    = pd.read_csv("transactions.csv")
settle = pd.read_csv("settlements.csv")

txn["amount"]           = pd.to_numeric(txn["amount"])
txn["txn_date"]         = pd.to_datetime(txn["timestamp"]).dt.date.astype(str)
settle["settled_amount"]  = pd.to_numeric(settle["settled_amount"])
settle["settlement_date"] = pd.to_datetime(settle["settlement_date"]).dt.date.astype(str)

passed = 0
failed = 0

def check(name, condition):
    global passed, failed
    if condition:
        print(f"  ✅ PASS — {name}")
        passed += 1
    else:
        print(f"  ❌ FAIL — {name}")
        failed += 1

print("\n=== RUNNING TEST CASES ===\n")

# TEST 1 — Cross-month settlements exist
late = settle[settle["settlement_date"] > "2026-03-31"]
check("GAP 1: At least one cross-month settlement exists", len(late) >= 1)
check("GAP 1: TXN0091 settled in April", 
      "TXN0091" in late["transaction_id"].values)
check("GAP 1: TXN0092 settled in April", 
      "TXN0092" in late["transaction_id"].values)

# TEST 2 — Rounding mismatch exists
merged = txn[txn["status"]=="SUCCESS"].merge(settle, on="transaction_id")
merged["diff"] = (merged["amount"] - merged["settled_amount"]).abs().round(4)
check("GAP 2: TXN0093 has amount mismatch", 
      merged[merged["transaction_id"]=="TXN0093"]["diff"].values[0] > 0)
check("GAP 2: Aggregate books do not balance", 
      abs(txn[txn["status"]=="SUCCESS"]["amount"].sum() - 
          settle["settled_amount"].sum()) > 0.01)

# TEST 3 — Duplicate exists
dups = txn[txn.duplicated(subset=["transaction_id"], keep=False)]
check("GAP 3: Duplicate transaction detected", len(dups) > 0)
check("GAP 3: TXN0094 is the duplicate", 
      "TXN0094" in dups["transaction_id"].values)
check("GAP 3: TXN0094 appears exactly 2 times", 
      len(txn[txn["transaction_id"]=="TXN0094"]) == 2)

# TEST 4 — Orphan refund exists
refunds   = txn[txn["status"]=="REFUND"]
valid_ids = set(txn[txn["status"]=="SUCCESS"]["transaction_id"])
orphans   = refunds[~refunds["original_txn_id"].isin(valid_ids)]
check("GAP 4: Orphan refund detected", len(orphans) > 0)
check("GAP 4: TXN0095 is the orphan refund", 
      "TXN0095" in orphans["transaction_id"].values)
check("GAP 4: Orphan references TXN9999", 
      "TXN9999" in orphans["original_txn_id"].values)

# TEST 5 — Legitimate refunds still pass
legit = refunds[refunds["original_txn_id"].isin(valid_ids)]
check("SANITY: Legitimate refunds are not flagged as orphans", 
      len(legit) >= 1)

# TEST 6 — Normal transactions have settlements
normal = txn[(txn["status"]=="SUCCESS") & 
             (~txn["transaction_id"].isin(["TXN0091","TXN0092","TXN0094"]))]\
             .drop_duplicates(subset=["transaction_id"])
unsettled = normal[~normal["transaction_id"].isin(settle["transaction_id"])]
check("SANITY: All normal March transactions are settled", 
      len(unsettled) == 0)

print(f"\n=== RESULTS: {passed} passed | {failed} failed ===\n")
if failed > 0:
    sys.exit(1)