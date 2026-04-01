"""
=============================================================
  GAP REPORTER — Plain English Output
  Every gap explained in one clear sentence
  Assessment: Payments Reconciliation | March 2026 | INR
=============================================================
"""

import pandas as pd
import warnings
warnings.filterwarnings("ignore")

MONTH_END        = "2026-03-31"
AMOUNT_TOLERANCE = 0.01
DIVIDER          = "=" * 65
SUBDIV           = "-" * 65

# ──────────────────────────────────────────────
# LOAD
# ──────────────────────────────────────────────
txn    = pd.read_csv("transactions.csv")
settle = pd.read_csv("settlements.csv")

txn["timestamp"]        = pd.to_datetime(txn["timestamp"])
txn["txn_date"]         = txn["timestamp"].dt.date.astype(str)
txn["amount"]           = pd.to_numeric(txn["amount"])
settle["settlement_date"] = pd.to_datetime(settle["settlement_date"]).dt.date.astype(str)
settle["settled_amount"]  = pd.to_numeric(settle["settled_amount"])

txn = txn[txn["status"] != "FAILED"].copy()

all_gaps = []   # collect every gap as a dict for final CSV

# ──────────────────────────────────────────────
# HELPER — register + print a gap
# ──────────────────────────────────────────────
def flag(gap_id, gap_type, txn_id, sentence, amount=None, extra=None):
    print(f"  [{gap_id}] {sentence}")
    row = {"gap_id": gap_id, "gap_type": gap_type,
           "transaction_id": txn_id, "explanation": sentence}
    if amount  is not None: row["amount_inr"]   = amount
    if extra   is not None: row.update(extra)
    all_gaps.append(row)


# ══════════════════════════════════════════════
# GAP 1 ── CROSS-MONTH SETTLEMENTS
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  GAP 1 — CROSS-MONTH SETTLEMENTS")
print(SUBDIV)
print("  Criteria: Transaction date ≤ Mar 31 but settled in April+")
print(SUBDIV)

march_ids   = txn[txn["txn_date"] <= MONTH_END]["transaction_id"]
late        = settle[
    settle["transaction_id"].isin(march_ids) &
    (settle["settlement_date"] > MONTH_END)
].merge(txn[["transaction_id","txn_date","amount"]], on="transaction_id")

if late.empty:
    print("  ✅  No cross-month settlements found.")
else:
    for _, r in late.iterrows():
        sentence = (
            f"Transaction {r.transaction_id} (₹{r.amount:,.2f}) was made on "
            f"{r.txn_date} but settled on {r.settlement_date} — "
            f"this falls AFTER month-end and will appear in April's books."
        )
        flag("G1", "Cross-Month Settlement", r.transaction_id, sentence,
             amount=r.amount,
             extra={"txn_date": r.txn_date, "settlement_date": r.settlement_date})


# ══════════════════════════════════════════════
# GAP 2 ── ROUNDING / AMOUNT MISMATCHES
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  GAP 2 — ROUNDING & AMOUNT MISMATCHES")
print(SUBDIV)
print(f"  Criteria: |txn amount − settled amount| > ₹{AMOUNT_TOLERANCE}")
print(SUBDIV)

success = txn[txn["status"] == "SUCCESS"].copy()
merged  = success.merge(settle, on="transaction_id", how="inner")
merged["diff"]     = (merged["amount"] - merged["settled_amount"]).round(4)
merged["abs_diff"] = merged["diff"].abs()

row_mismatches = merged[merged["abs_diff"] > AMOUNT_TOLERANCE]

if row_mismatches.empty:
    print("  ✅  No per-row amount mismatches found.")
else:
    for _, r in row_mismatches.iterrows():
        direction = "short" if r["diff"] > 0 else "excess"
        sentence = (
            f"Transaction {r.transaction_id}: platform recorded ₹{r.amount:.3f} "
            f"but bank settled ₹{r.settled_amount:.2f} — "
            f"rounding mismatch of ₹{abs(r['diff']):.3f} ({direction})."
        )
        flag("G2", "Amount Mismatch", r.transaction_id, sentence,
             amount=r.amount,
             extra={"settled_amount": r.settled_amount, "diff_inr": r["diff"]})

# ── Aggregate sum check (catches cross-month leakage at book level) ──
total_txn    = success["amount"].sum()
settled_ids  = settle["transaction_id"].isin(success["transaction_id"])
total_settle = settle[settled_ids]["settled_amount"].sum()
net_diff     = round(total_txn - total_settle, 4)

print(f"\n  {SUBDIV}")
print(f"  AGGREGATE BOOK-BALANCE CHECK")
print(f"  {SUBDIV}")
print(f"  Total of all platform transactions : ₹{total_txn:>12,.4f}")
print(f"  Total of all bank settlements      : ₹{total_settle:>12,.4f}")
print(f"  Net difference                     : ₹{net_diff:>12,.4f}")

if abs(net_diff) > AMOUNT_TOLERANCE:
    agg_sentence = (
        f"Books do NOT balance: platform recorded ₹{total_txn:,.4f} in total "
        f"but bank settled only ₹{total_settle:,.4f} — "
        f"net shortfall of ₹{net_diff:,.4f} across all March transactions."
    )
    print(f"\n  ⚠️  {agg_sentence}")
    flag("G2-AGG", "Aggregate Imbalance", "ALL", agg_sentence,
         extra={"total_txn": total_txn, "total_settled": total_settle, "net_diff": net_diff})
else:
    print("\n  ✅  Books balance at aggregate level.")


# ══════════════════════════════════════════════
# GAP 3 ── DUPLICATE TRANSACTIONS
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  GAP 3 — DUPLICATE TRANSACTIONS")
print(SUBDIV)
print("  Criteria: Same transaction_id appears more than once")
print(SUBDIV)

dup_mask = txn.duplicated(subset=["transaction_id"], keep=False)
dups     = txn[dup_mask].copy()
dup_groups = dups.groupby("transaction_id")

if dups.empty:
    print("  ✅  No duplicate transaction IDs found.")
else:
    for txn_id, grp in dup_groups:
        count     = len(grp)
        amt       = grp["amount"].iloc[0]
        ts        = grp["timestamp"].iloc[0].strftime("%Y-%m-%d %H:%M")
        overpay   = amt * (count - 1)
        sentence  = (
            f"Duplicate detected: Transaction {txn_id} (₹{amt:,.2f} on {ts}) "
            f"appears {count} times in the platform records — "
            f"if settled {count}×, this inflates revenue by ₹{overpay:,.2f}."
        )
        flag("G3", "Duplicate Transaction", txn_id, sentence,
             amount=amt, extra={"occurrences": count, "risk_amount": overpay})


# ══════════════════════════════════════════════
# GAP 4 ── ORPHAN REFUNDS
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  GAP 4 — ORPHAN REFUNDS")
print(SUBDIV)
print("  Criteria: REFUND row whose original_txn_id has no SUCCESS match")
print(SUBDIV)

refunds     = txn[txn["status"] == "REFUND"].copy()
valid_ids   = set(txn[txn["status"] == "SUCCESS"]["transaction_id"])
orphans     = refunds[~refunds["original_txn_id"].isin(valid_ids)]

if orphans.empty:
    print("  ✅  No orphan refunds found.")
else:
    for _, r in orphans.iterrows():
        sentence = (
            f"Orphan refund detected: Transaction {r.transaction_id} attempts to refund "
            f"₹{abs(r.amount):,.2f} back to original transaction {r.original_txn_id} — "
            f"but {r.original_txn_id} does NOT exist in platform records. "
            f"This refund has no parent and cannot be reconciled."
        )
        flag("G4", "Orphan Refund", r.transaction_id, sentence,
             amount=r.amount,
             extra={"original_txn_id": r.original_txn_id})


# ══════════════════════════════════════════════
# GAP 5 ── MISSING SETTLEMENTS
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  GAP 5 — MISSING SETTLEMENTS")
print(SUBDIV)
print("  Criteria: SUCCESS transaction with zero matching settlement rows")
print(SUBDIV)

deduped_success = success.drop_duplicates(subset=["transaction_id"])
settled_ids     = set(settle["transaction_id"])
missing         = deduped_success[~deduped_success["transaction_id"].isin(settled_ids)]

if missing.empty:
    print("  ✅  Every transaction has at least one settlement entry.")
else:
    for _, r in missing.iterrows():
        sentence = (
            f"Transaction {r.transaction_id} (₹{r.amount:,.2f} on {r.txn_date}) "
            f"has NO corresponding settlement — money collected but never confirmed received by bank."
        )
        flag("G5", "Missing Settlement", r.transaction_id, sentence,
             amount=r.amount, extra={"txn_date": r.txn_date})


# ══════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════
print(f"\n{DIVIDER}")
print("  RECONCILIATION SUMMARY — MARCH 2026")
print(DIVIDER)

gap_df = pd.DataFrame(all_gaps)

if gap_df.empty:
    print("  ✅  BOOKS BALANCE — No gaps detected.")
else:
    summary = gap_df.groupby("gap_type")["gap_id"].count().reset_index()
    summary.columns = ["Gap Type", "Count"]
    print(summary.to_string(index=False))
    print(f"\n  TOTAL GAPS  : {len(gap_df)}")
    print(f"  GAP TYPES   : {gap_df['gap_type'].nunique()}")

    # Save human-readable report
    gap_df.to_csv("gap_report.csv", index=False)
    print(f"\n  ✅  Full gap report saved → gap_report.csv")

print(DIVIDER)
