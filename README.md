# Payments Reconciliation Engine
**Assessment:** Onelab Ventures — AI-Native Engineer Intern  
**Dataset:** March 2026 | Currency: INR | Timezone: IST

---

## Assumptions
- Settlement delay = 1–2 business days
- Currency = INR only
- Month-end cutoff = 2026-03-31 23:59:59 IST
- Transactions recorded instantly; settlements batched by bank
- FAILED transactions excluded from reconciliation
- Duplicate = same transaction_id appearing more than once
- Orphan refund = REFUND with no matching SUCCESS original

---

## Files
| File | Purpose |
|---|---|
| `transactions.csv` | 100 platform transaction records |
| `settlements.csv` | 98 bank settlement records |
| `reconcile.py` | Main reconciliation engine |
| `gap_reporter.py` | Plain-English gap explanations |
| `test_reconciliation.py` | 11 test cases verifying all 4 gaps |

---

## How to Run
```bash
pip install pandas
python reconcile.py
python gap_reporter.py
python test_reconciliation.py
```

---

## Gaps Detected
| Gap | Transaction | Explanation |
|---|---|---|
| Cross-Month | TXN0091, TXN0092 | March 31 txns settled April 1 |
| Rounding | TXN0093 | ₹999.999 settled as ₹1000.00 |
| Duplicate | TXN0094 | Appears twice in platform records |
| Orphan Refund | TXN0095 | Refunds TXN9999 which doesn't exist |

---

## What It Would Get Wrong in Production
1. Batch settlements where one transfer covers multiple transactions 
   cannot be split without a line-item mapping file from the bank.
2. UTC vs IST timezone mismatch causes false cross-month flags for 
   late-night IST transactions.
3. The duplicate check loads all IDs into memory — at 10M+ transactions 
   this needs a SQL GROUP BY query instead.
```

Click **Commit changes** ✅

---

## Step 5 — Get Your Shareable Link

Your repo URL will be:
```
https://github.com/YOUR_USERNAME/payments-reconciliation
```

This is your **Deliverable 5A (deployed link)** — paste it wherever the submission form asks for a link.

---

## Step 6 — Download as ZIP (Deliverable 5B)

On your repo page:
1. Click green **Code** button
2. Click **Download ZIP**
3. That's your `submission.zip` ✅

---

## What Your GitHub Repo Will Look Like
```
payments-reconciliation/
├── README.md                  ← assumptions + how to run + gap table
├── transactions.csv           ← input data
├── settlements.csv            ← input data  
├── reconcile.py               ← engine
├── gap_reporter.py            ← plain English output
└── test_reconciliation.py     ← test cases
```

---

## Why GitHub is Better Than Replit Here

| | GitHub | Replit |
|---|---|---|
| Evaluator can read code | ✅ directly in browser | ❌ need to open editor |
| Looks professional | ✅ industry standard | 🟡 looks like a demo |
| README renders nicely | ✅ automatic | ❌ manual |
| Free forever | ✅ | 🟡 limited |
| Download ZIP built-in | ✅ one click | ❌ manual |

---

## Your Final Submission Links

Once done, you'll have two links to submit:
```
GitHub repo  → https://github.com/YOUR_USERNAME/payments-reconciliation
Demo video   → https://loom.com/share/XXXXXXX   (record using Loom)
