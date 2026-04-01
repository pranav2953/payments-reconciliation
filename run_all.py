import subprocess
print("Running reconciliation engine...")
subprocess.run(["python", "reconcile.py"])
print("\nRunning gap reporter...")
subprocess.run(["python", "gap_reporter.py"])
print("\nDone. Check gap_report.csv and reconciliation_report.csv")