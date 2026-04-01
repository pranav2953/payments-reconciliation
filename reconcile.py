"""
=============================================================
  PAYMENTS RECONCILIATION ENGINE
  Author  : AI-Native Engineer Assessment
  Dataset : March 2026 | Currency: INR | Timezone: IST
=============================================================

ASSUMPTIONS:
  - Month-end cutoff    = 2026-03-31 (last day of reconciliation window)
  - Settlement delay    = 1–2 business days (bank side)
  - Currency            = INR only (no FX conversion)
  - Matching primary    = transaction_id (exact)
  - Matching fallback   = amount + date (fuzzy, within ±₹1 tolerance)
  - Rounding tolerance  = ₹0.01 per transaction, but summed diff flagged
  - Duplicate           = same transaction_id appearing > 1 time in txn file
  - Orphan refund       = REFUND row whose original_txn_id has no match
  - FAILED transactions = excluded from reconciliation entirely
  - A settlement after  2026-03-31 for a March txn = cross-month gap
"""

import pandas as pd
import warnings
warnings.filterwarnings("ignore")

MONTH_END         = "2026-03-31"
AMOUNT_TOLERANCE  = 0.01          # per-row rounding tolerance (INR)
REPORT_DIVIDER    = "=" * 65

# ─────────────────────────────────────────────
# 1. LOAD DATA
# ─────────────────────────────────────────────
def load_data(txn_path: str, settle_path: str):
    txn = pd.read_csv(txn_path)
    settle = pd.read_csv(settle_path)

    txn["timestamp"]       = pd.to_datetime(txn["timestamp"])
    txn["txn_date"]        = txn["timestamp"].dt.date.astype(str)
    txn["amount"]          = pd.to_numeric(txn["amount"])

    settle["settlement_date"] = pd.to_datetime(settle["settlement_date"]).dt.date.astype(str)
    settle["settled_amount"]  = pd.to_numeric(settle["settled_amount"])

    # Exclude FAILED transactions
    txn = txn[txn["status"] != "FAILED"].copy()

    print(f"  Transactions loaded  : {len(txn)} rows (after excluding FAILED)")
    print(f"  Settlements  loaded  : {len(settle)} rows")
    return txn, settle


# ─────────────────────────────────────────────
# 2. GAP 1 — CROSS-MONTH SETTLEMENTS
#    Transaction in March → settled in April+
# ─────────────────────────────────────────────
def detect_cross_month(txn: pd.DataFrame, settle: pd.DataFrame) -> pd.DataFrame:
    march_txns   = txn[txn["txn_date"] <= MONTH_END]["transaction_id"].tolist()
    late_settle  = settle[
        (settle["transaction_id"].isin(march_txns)) &
        (settle["settlement_date"] > MONTH_END)
    ].copy()

    if late_settle.empty:
        return late_settle

    # Merge to get transaction timestamp
    merged = late_settle.merge(
        txn[["transaction_id", "txn_date", "amount"]],
        on="transaction_id", how="left"
    )
    merged["gap_type"]    = "Cross-Month Settlement"
    merged["description"] = (
        "Transaction on " + merged["txn_date"] +
        " settled on "    + merged["settlement_date"] +
        " (after month-end)"
    )
    return merged[["gap_type", "transaction_id", "txn_date",
                   "settlement_date", "amount", "settled_amount", "description"]]


# ─────────────────────────────────────────────
# 3. GAP 2 — AMOUNT MISMATCHES (incl. rounding)
#    Per-row diff AND summed diff
# ─────────────────────────────────────────────
def detect_amount_mismatch(txn: pd.DataFrame, settle: pd.DataFrame) -> pd.DataFrame:
    success_txn = txn[txn["status"] == "SUCCESS"].copy()
    merged = success_txn.merge(settle, on="transaction_id", how="inner")

    merged["amount_diff"]    = (merged["amount"] - merged["settled_amount"]).round(4)
    merged["abs_diff"]       = merged["amount_diff"].abs()

    # Per-row mismatches beyond tolerance
    mismatches = merged[merged["abs_diff"] > AMOUNT_TOLERANCE].copy()
    mismatches["gap_type"]    = "Amount Mismatch"
    mismatches["description"] = (
        "Txn=₹" + mismatches["amount"].astype(str) +
        " vs Settled=₹" + mismatches["settled_amount"].astype(str) +
        " | Diff=₹" + mismatches["amount_diff"].astype(str)
    )

    # Summed rounding check (catches distributed sub-tolerance rounding)
    total_txn    = success_txn["amount"].sum()
    total_settle = settle[settle["transaction_id"].isin(success_txn["transaction_id"])]["settled_amount"].sum()
    sum_diff     = round(total_txn - total_settle, 4)

    print(f"\n  [Rounding Check] Total Txn=₹{total_txn:.4f} | "
          f"Total Settled=₹{total_settle:.4f} | Net Diff=₹{sum_diff:.4f}")

    # Add a summary rounding row if summed diff exists but per-row is clean
    if abs(sum_diff) > AMOUNT_TOLERANCE and mismatches.empty:
        rounding_row = pd.DataFrame([{
            "gap_type"      : "Amount Mismatch",
            "transaction_id": "AGGREGATE",
            "amount"        : round(total_txn, 4),
            "settled_amount": round(total_settle, 4),
            "amount_diff"   : sum_diff,
            "description"   : (
                f"Books don't balance at aggregate level: "
                f"Sum of txns=₹{total_txn:.4f} vs Sum of settlements=₹{total_settle:.4f} "
                f"| Net diff=₹{sum_diff:.4f} (likely caused by cross-month or missing settlements)"
            )
        }])
        mismatches = pd.concat([mismatches, rounding_row], ignore_index=True)

    return mismatches[["gap_type", "transaction_id", "amount",
                        "settled_amount", "amount_diff", "description"]]


# ─────────────────────────────────────────────
# 4. GAP 3 — DUPLICATE TRANSACTIONS
#    Same transaction_id > 1 row in txn file
# ─────────────────────────────────────────────
def detect_duplicates(txn: pd.DataFrame) -> pd.DataFrame:
    dup_mask  = txn.duplicated(subset=["transaction_id"], keep=False)
    dups      = txn[dup_mask].copy()

    if dups.empty:
        return dups

    dups["occurrence"]   = dups.groupby("transaction_id").cumcount() + 1
    dups["gap_type"]     = "Duplicate Transaction"
    dups["description"]  = (
        "transaction_id " + dups["transaction_id"] +
        " appears " +
        dups.groupby("transaction_id")["transaction_id"]
            .transform("count").astype(str) +
        " times in transactions file"
    )
    return dups[["gap_type", "transaction_id", "amount",
                 "timestamp", "occurrence", "description"]]


# ─────────────────────────────────────────────
# 5. GAP 4 — ORPHAN REFUNDS
#    REFUND row whose original_txn_id doesn't exist
# ─────────────────────────────────────────────
def detect_orphan_refunds(txn: pd.DataFrame) -> pd.DataFrame:
    refunds        = txn[txn["status"] == "REFUND"].copy()
    valid_txn_ids  = set(txn[txn["status"] == "SUCCESS"]["transaction_id"])

    orphans = refunds[
        ~refunds["original_txn_id"].isin(valid_txn_ids)
    ].copy()

    orphans["gap_type"]    = "Orphan Refund"
    orphans["description"] = (
        "Refund " + orphans["transaction_id"] +
        " references original_txn_id=" + orphans["original_txn_id"].astype(str) +
        " which does NOT exist in transactions"
    )
    return orphans[["gap_type", "transaction_id", "amount",
                    "original_txn_id", "description"]]


# ─────────────────────────────────────────────
# 6. MISSING SETTLEMENTS
#    SUCCESS txns with no entry in settlements
# ─────────────────────────────────────────────
def detect_missing_settlements(txn: pd.DataFrame, settle: pd.DataFrame) -> pd.DataFrame:
    success_txn    = txn[txn["status"] == "SUCCESS"].drop_duplicates(subset=["transaction_id"])
    settled_ids    = set(settle["transaction_id"])
    missing        = success_txn[~success_txn["transaction_id"].isin(settled_ids)].copy()

    missing["gap_type"]    = "Missing Settlement"
    missing["description"] = (
        "Transaction " + missing["transaction_id"] +
        " (₹" + missing["amount"].astype(str) + ") has NO matching settlement"
    )
    return missing[["gap_type", "transaction_id", "txn_date",
                    "amount", "description"]]


# ─────────────────────────────────────────────
# 7. FALLBACK FUZZY MATCH
#    Match by amount + date when ID match fails
# ─────────────────────────────────────────────
def fuzzy_match(txn: pd.DataFrame, settle: pd.DataFrame) -> pd.DataFrame:
    """Finds transactions that match settlement by amount+date but NOT by ID."""
    unmatched_txn    = txn[~txn["transaction_id"].isin(settle["transaction_id"])].copy()
    unmatched_settle = settle[~settle["transaction_id"].isin(txn["transaction_id"])].copy()

    if unmatched_txn.empty or unmatched_settle.empty:
        return pd.DataFrame()

    unmatched_settle["settle_date_str"] = unmatched_settle["settlement_date"]
    matches = []

    for _, t_row in unmatched_txn.iterrows():
        candidates = unmatched_settle[
            (unmatched_settle["settle_date_str"] >= t_row["txn_date"]) &
            (unmatched_settle["settle_date_str"] <= MONTH_END) &
            (abs(unmatched_settle["settled_amount"] - t_row["amount"]) <= AMOUNT_TOLERANCE)
        ]
        if not candidates.empty:
            best = candidates.iloc[0]
            matches.append({
                "gap_type"      : "Fuzzy Match (ID mismatch, Amount+Date matched)",
                "transaction_id": t_row["transaction_id"],
                "settlement_id" : best["settlement_id"],
                "txn_amount"    : t_row["amount"],
                "settled_amount": best["settled_amount"],
                "description"   : f"ID mismatch but amount ₹{t_row['amount']} matched on date"
            })
    return pd.DataFrame(matches)


# ─────────────────────────────────────────────
# 8. PRINT REPORT
# ─────────────────────────────────────────────
def print_report(results: dict):
    print(f"\n{REPORT_DIVIDER}")
    print("        RECONCILIATION REPORT — MARCH 2026")
    print(REPORT_DIVIDER)

    total_gaps = 0
    for gap_name, df in results.items():
        count = len(df)
        total_gaps += count
        status = "✅ NONE" if count == 0 else f"⚠️  {count} FOUND"
        print(f"\n{'─'*65}")
        print(f"  {gap_name}")
        print(f"  Status: {status}")
        print(f"{'─'*65}")
        if count > 0:
            print(df.to_string(index=False))

    print(f"\n{REPORT_DIVIDER}")
    print(f"  TOTAL GAPS DETECTED: {total_gaps}")
    print(REPORT_DIVIDER)


# ─────────────────────────────────────────────
# 9. SAVE REPORT TO CSV
# ─────────────────────────────────────────────
def save_report(results: dict, out_path: str):
    all_rows = []
    for gap_name, df in results.items():
        if not df.empty:
            df = df.copy()
            df["gap_category"] = gap_name
            all_rows.append(df)

    if all_rows:
        final = pd.concat(all_rows, ignore_index=True)
        # Move gap columns to front
        cols = ["gap_category", "gap_type", "transaction_id", "description"]
        other_cols = [c for c in final.columns if c not in cols]
        final = final[cols + other_cols]
        final.to_csv(out_path, index=False)
        print(f"\n  ✅ Report saved → {out_path}")
    else:
        print("\n  ✅ No gaps found — books balance!")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print(REPORT_DIVIDER)
    print("  LOADING DATA...")
    print(REPORT_DIVIDER)

    txn, settle = load_data(
        "transactions.csv",
        "settlements.csv"
    )

    print(f"\n{REPORT_DIVIDER}")
    print("  RUNNING GAP DETECTORS...")
    print(REPORT_DIVIDER)

    results = {
        "GAP 1 — Cross-Month Settlements"  : detect_cross_month(txn, settle),
        "GAP 2 — Amount Mismatches"        : detect_amount_mismatch(txn, settle),
        "GAP 3 — Duplicate Transactions"   : detect_duplicates(txn),
        "GAP 4 — Orphan Refunds"           : detect_orphan_refunds(txn),
        "GAP 5 — Missing Settlements"      : detect_missing_settlements(txn, settle),
        "GAP 6 — Fuzzy Matches (ID≠, Amt≈)": fuzzy_match(txn, settle),
    }

    print_report(results)
    save_report(results, "reconciliation_report.csv")
