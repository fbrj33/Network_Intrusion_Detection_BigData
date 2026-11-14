import glob
import os
import re
import sys

import numpy as np
import pandas as pd

SRC = os.path.join("data", "cicids2017")
OUT = os.path.join("data", "prepared")
SEED = 42
SHARES = {"logs": 0.60, "mysql": 0.10}  # le reste (30 %) va au stream
RENAMES = {
    "Total Lengthof Bwd Packets": "Total Length of Bwd Packets",
    "SubflowBwd Bytes": "Subflow Bwd Bytes",
    "ActiveStd": "Active Std",
}


def snake(name):
    name = RENAMES.get(name, name)
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


os.makedirs(OUT, exist_ok=True)
for name in ("logs", "mysql", "stream"):
    p = os.path.join(OUT, "part_%s.csv" % name)
    if os.path.exists(p):
        os.remove(p)

files = sorted(glob.glob(os.path.join(SRC, "*.csv")))
if not files:
    sys.exit("Aucun CSV trouve dans " + SRC)

rng = np.random.default_rng(SEED)
next_id = 1
counts = []

for path in files:
    day = os.path.basename(path).split(".")[0]
    df = pd.read_csv(path, encoding="latin1", low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    if "Fwd Header Length.1" in df.columns:
        if df["Fwd Header Length"].equals(df["Fwd Header Length.1"]):
            df = df.drop(columns=["Fwd Header Length.1"])
    df.columns = [snake(c) for c in df.columns]
    if "label" not in df.columns:
        sys.exit("Colonne Label introuvable dans " + path)
    n_raw = len(df)

    df["label"] = (df["label"].astype(str).str.strip()
                   .str.replace(r"[^\x00-\x7F]+", "-", regex=True))
    num_cols = [c for c in df.columns if c != "label"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=num_cols)
    n_nan = n_raw - len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    n_dup = n_raw - n_nan - len(df)

    if next_id == 1:
        print("Colonnes (%d) : %s" % (len(df.columns), list(df.columns)))

    assign = np.empty(len(df), dtype=object)
    for label, idx in df.groupby("label").indices.items():
        idx = rng.permutation(idx)
        n1 = int(round(len(idx) * SHARES["logs"]))
        n2 = int(round(len(idx) * SHARES["mysql"]))
        assign[idx[:n1]] = "logs"
        assign[idx[n1:n1 + n2]] = "mysql"
        assign[idx[n1 + n2:]] = "stream"
    df["split"] = assign

    df.insert(0, "source_day", day)
    df.insert(0, "flow_id", range(next_id, next_id + len(df)))
    next_id += len(df)

    for name in ("logs", "mysql", "stream"):
        part = df[df["split"] == name].drop(columns=["split"])
        p = os.path.join(OUT, "part_%s.csv" % name)
        part.to_csv(p, mode="a", header=not os.path.exists(p),
                    index=False, float_format="%.10g")

    counts.append(df.groupby(["label", "split"]).size().rename("n").reset_index())
    print("%s : %d lues, %d NaN/Inf, %d doublons, %d gardees"
          % (day, n_raw, n_nan, n_dup, len(df)))

summary = pd.concat(counts).groupby(["label", "split"])["n"].sum().unstack(fill_value=0)
summary["total"] = summary.sum(axis=1)
print(summary.to_string())
print(summary.sum().to_string())
summary.to_csv(os.path.join(OUT, "summary_label_split.csv"))
