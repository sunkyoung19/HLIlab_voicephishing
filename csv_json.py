import pandas as pd
import json
import os

input_csvs = [
    "./sms/vp_stt_labeled.csv",
    "./sms/non_vishing_concat.csv",  
]

output_dir = "./sms"
os.makedirs(output_dir, exist_ok=True)

for csv_path in input_csvs:
    df = pd.read_csv(csv_path)

    # human_label == "HALLUCINATION" 인 행만 제외
    if "human_label" in df.columns:
        df_filtered = df[df["human_label"] != "HALLUCINATION"].copy()
    else:
        df_filtered = df.copy()

    if "text" not in df_filtered.columns:
        raise ValueError(f"{csv_path} 입력 CSV에 'text' 컬럼이 없습니다. 컬럼명을 확인하세요.")

    out_name = os.path.splitext(os.path.basename(csv_path))[0] + ".jsonl"
    jsonl_path = os.path.join(output_dir, out_name)

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for i, row in df_filtered.iterrows():
            record = {
                "id": int(i),
                "text": row["text"],
                "source_index": int(row["original_index"]) if "original_index" in df_filtered.columns and pd.notna(row["original_index"]) else int(i),
                "source_file": row["original_file"] if "original_file" in df_filtered.columns and pd.notna(row["original_file"]) else os.path.basename(csv_path),
            }
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"변환 완료: {jsonl_path} | 총 {len(df):,} → {len(df_filtered):,}")
