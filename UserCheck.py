import os
import re
import json
import csv
from collections import defaultdict
from tqdm import tqdm

import pandas as pd

RESULT_JSONL = "result.jsonl"             
INPUT_CSV_DIR = "./sms"                   
OUTPUT_CSV_DIR = "./deid_csv_outputs"    
os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)

CID_RE = re.compile(r"^(?P<base>[^|]+)\|si:(?P<idx>[^|]+)\|sf:(?P<src>.+)$")

by_source = defaultdict(dict)

total = 0
ok = 0

print(f"[Load] {RESULT_JSONL}")
with open(RESULT_JSONL, "r", encoding="utf-8") as fin:
    for line in tqdm(fin, desc="[Parsing result.jsonl]"):
        total += 1
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue

        if rec.get("response", {}).get("status_code") != 200:
            continue

        choice = (rec["response"]["body"]["choices"][0]
                  if rec["response"]["body"].get("choices") else None)
        if not choice:
            continue

        content = choice["message"]["content"] 
        custom_id = rec.get("custom_id", "")
        m = CID_RE.match(custom_id)
        if not m:
            continue

        src_file = m.group("src")
        src_idx = m.group("idx")

        try:
            src_idx = int(src_idx)
        except:
            pass

        by_source[src_file][src_idx] = content
        ok += 1

print(f"[Done] 응답 {ok}/{total} 건 매핑 수집")


def save_deid_csv_for_source(source_file: str, mapping: dict):

    in_csv_path = os.path.join(INPUT_CSV_DIR, source_file)
    if not os.path.exists(in_csv_path):
        print(f"[WARN] 원본 CSV 없음: {in_csv_path}")
        return

    print(f"[Open] {in_csv_path}")
    df = pd.read_csv(in_csv_path)

    index_col_name = None
    if "original_index" in df.columns:
        index_col_name = "original_index"
    else:
        df = df.reset_index()   
        index_col_name = "index"

    if "text" not in df.columns:
        print(f"[WARN] 'text' 컬럼이 없어 교체 불가: {source_file}")
        return

    for i in tqdm(range(len(df)), desc=f"[Patch text] {source_file}"):
        src_idx = df.loc[i, index_col_name]

        if isinstance(src_idx, float) and src_idx.is_integer():
            src_idx = int(src_idx)

        if src_idx in mapping:
            df.loc[i, "text"] = mapping[src_idx]

    out_name = os.path.splitext(os.path.basename(source_file))[0] + "_deid.csv"
    out_path = os.path.join(OUTPUT_CSV_DIR, out_name)

    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[Saved] {out_path} (rows: {len(df):,})")


for src_file, mapping in by_source.items():
    save_deid_csv_for_source(src_file, mapping)
