
import os
import json
import re
from collections import defaultdict
from typing import Tuple, Optional, Dict, List

import pandas as pd
from tqdm import tqdm


INPUT_CSV_DIR = "./sms"
OUTPUT_CSV_DIR = "./deid_csv_outputs"
DEID_ONLY_FILES = [
    "outputs/non_vishing_deid_only.jsonl",
    "outputs/vp_stt_deid_only.jsonl",
]
os.makedirs(OUTPUT_CSV_DIR, exist_ok=True)


ALIASES = {

    "vp_positive_whisper_large.csv": "vp_stt_labeled.csv",
    "vp_negative_whisper_large.csv": "vp_stt_labeled.csv",
}

def normalize_source_file(sf: Optional[str]) -> Optional[str]:
    if not sf:
        return None
    sf = os.path.basename(sf)
    if sf in ALIASES:
        return ALIASES[sf]
    if "vp_stt" in sf or sf.startswith("vp_stt_"):
        return "vp_stt_labeled.csv"
    return sf


LEGACY_RE = re.compile(r"^(?P<base>[^|]+)-(?P<chunk>\d+)-(?P<offset>\d+)$")

def parse_custom_id(cid: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:

    if not cid:
        return None, None, None

    m = LEGACY_RE.match(cid)
    if m:
        return m.group("base"), None, None

    parts = cid.split("|")
    base = parts[0] if parts else None
    kv = {}
    for p in parts[1:]:
        if ":" in p:
            k, v = p.split(":", 1)
            kv[k.strip()] = v.strip()

    src_idx = None
    raw = kv.get("si") or kv.get("source_index") or kv.get("id")
    if raw is not None:
        try:
            src_idx = int(float(raw))
        except Exception:
            src_idx = None

    sf = kv.get("sf") or kv.get("source_file")
    if sf:
        sf = os.path.basename(sf)

    return base, src_idx, sf

def guess_source_from_base(base: Optional[str]) -> Optional[str]:
    if not base:
        return None
    b = base.lower()
    if "non_vishing" in b or "nonvishing" in b:
        return "non_vishing_concat.csv"
    if "vp_stt" in b:
        return "vp_stt_labeled.csv"
    return None


by_source: Dict[str, Dict[int, str]] = defaultdict(dict)

legacy_results: Dict[str, List[Tuple[int, int, str]]] = defaultdict(list)

by_pair: Dict[Tuple[str, int], str] = {}

total_lines = 0
used_lines = 0


for path in DEID_ONLY_FILES:
    if not os.path.exists(path):
        print(f"[WARN] deid_only 파일이 없음: {path}")
        continue

    print(f"[Load] {path}")
    with open(path, "r", encoding="utf-8") as fin:
        for line in tqdm(fin, desc=f"[Parsing {os.path.basename(path)}]"):
            total_lines += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            cid = rec.get("custom_id", "")
            content = rec.get("content")
            if content is None:
                continue

            base, src_idx, src_file = parse_custom_id(cid)

            m = LEGACY_RE.match(cid)
            if m:
                chunk = int(m.group("chunk"))
                offset = int(m.group("offset"))
                legacy_results[base].append((chunk, offset, content))
                continue


            if not src_file:
                src_file = guess_source_from_base(base)

            if src_file is not None and src_idx is not None:

                norm_sf = normalize_source_file(src_file)
                by_source[norm_sf][int(src_idx)] = content
                used_lines += 1

                base_sf = os.path.basename(src_file)
                by_pair[(base_sf, int(src_idx))] = content

print(f"[Stage1] 신형 매핑: 사용 {used_lines}/{total_lines} 라인")


def attach_legacy_group(base: str, triplets: List[Tuple[int, int, str]]):

    tail_path = os.path.join(INPUT_CSV_DIR, f"{base}.jsonl")
    if not os.path.exists(tail_path):
        alt_path = f"./{base}.jsonl"
        if os.path.exists(alt_path):
            tail_path = alt_path
        else:
            print(f"[WARN] tail 입력 JSONL 없음: {tail_path}")
            return

    inputs: List[Tuple[str, str, int]] = []  
    with open(tail_path, "r", encoding="utf-8") as fin:
        for line in fin:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            sf_raw = rec.get("source_file")
            if not sf_raw:
                sf_raw = guess_source_from_base(base)
            if not sf_raw:
                sf_raw = "vp_stt_labeled.csv"

            norm_sf = normalize_source_file(sf_raw)
            base_sf = os.path.basename(sf_raw)

            idx = rec.get("source_index")
            if idx is None:
                idx = rec.get("id")
            if idx is None:
                continue

            try:
                idx = int(float(idx))
            except Exception:
                continue

            inputs.append((norm_sf, base_sf, idx))

    if not inputs:
        print(f"[WARN] tail 입력에서 (source_file, source_index) 추출 실패: {tail_path}")
        return

    triplets_sorted = sorted(triplets, key=lambda t: (t[0], t[1]))
    n = min(len(triplets_sorted), len(inputs))

    patched = 0
    for k in range(n):
        norm_sf, base_sf, idx = inputs[k]
        _, _, content = triplets_sorted[k]
        # 단일키(보조)
        by_source[norm_sf][idx] = content
        # ★ 복합키(주키)
        by_pair[(base_sf, idx)] = content
        patched += 1

    print(f"[Stage2] 레거시 매칭 완료: base={base}, patched={patched}")


for base, trips in legacy_results.items():
    attach_legacy_group(base, trips)


def save_deid_csv_for_source(source_file: str, mapping: dict):
    in_csv_path = os.path.join(INPUT_CSV_DIR, os.path.basename(source_file))
    aliased = ALIASES.get(os.path.basename(source_file))
    if not os.path.exists(in_csv_path) and aliased:
        in_csv_path = os.path.join(INPUT_CSV_DIR, aliased)

    if not os.path.exists(in_csv_path):
        print(f"[WARN] 원본 CSV 없음: {in_csv_path}")
        return

    print(f"[Open] {in_csv_path}")
    df = pd.read_csv(in_csv_path)

    if "text" not in df.columns:
        print(f"[WARN] 'text' 컬럼 없음 → 교체 불가: {source_file}")
        return


    is_vpstt = (os.path.basename(in_csv_path) == "vp_stt_labeled.csv")
    use_pair = ("original_file" in df.columns)

    if "original_index" in df.columns:
        index_mode = "column"
        index_col = "original_index"
    else:
        index_mode = "rowpos"

    patched_pair = 0
    patched_single = 0
    skipped = 0
    missed = 0

    if is_vpstt and use_pair and index_mode == "column":
        for i in tqdm(range(len(df)), desc=f"[Patch text] {os.path.basename(source_file)}"):
            key_val = df.loc[i, index_col]
            try:
                idx_key = int(key_val) if not (isinstance(key_val, float) and key_val.is_integer()) else int(key_val)
            except Exception:
                skipped += 1
                continue

            file_val = df.loc[i, "original_file"]
            if pd.isna(file_val):
                missed += 1
                continue

            base_sf = os.path.basename(str(file_val))
            pair_key = (base_sf, idx_key)

            if pair_key in by_pair:
                df.loc[i, "text"] = by_pair[pair_key]
                patched_pair += 1
            else:

                missed += 1

    else:

        if index_mode == "column":
            for i in tqdm(range(len(df)), desc=f"[Patch text] {os.path.basename(source_file)}"):
                key_val = df.loc[i, index_col]
                try:
                    key = int(key_val) if not (isinstance(key_val, float) and key_val.is_integer()) else int(key_val)
                except Exception:
                    skipped += 1
                    continue
                if key in mapping:
                    df.loc[i, "text"] = mapping[key]
                    patched_single += 1
                else:
                    missed += 1
        else:
            for i in tqdm(range(len(df)), desc=f"[Patch text] {os.path.basename(source_file)}"):
                if i in mapping:
                    df.loc[i, "text"] = mapping[i]
                    patched_single += 1
                else:
                    missed += 1

    if "index" in df.columns and "original_index" not in df.columns:
        df = df.drop(columns=["index"])

    out_name = os.path.splitext(os.path.basename(in_csv_path))[0] + "_deid.csv"
    out_path = os.path.join(OUTPUT_CSV_DIR, out_name)
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(
        f"[Saved] {out_path} (rows:{len(df):,}, "
    )


if not by_source and not by_pair:
    print("[ERROR] 적용할 매핑이 없습니다. deid_only 파일/입력 tail JSONL/ALIASES를 확인하세요.")
else:

    if "non_vishing_concat.csv" in by_source:
        save_deid_csv_for_source("non_vishing_concat.csv", by_source["non_vishing_concat.csv"])

    if "vp_stt_labeled.csv" in by_source:
        save_deid_csv_for_source("vp_stt_labeled.csv", by_source["vp_stt_labeled.csv"])
