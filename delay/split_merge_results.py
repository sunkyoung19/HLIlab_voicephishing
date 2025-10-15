import os, json
from dotenv import load_dotenv
from openai import OpenAI

BATCH_GROUPS = {

    "non_vishing": [
        "batch_68dead6ba0848190966ab16414f677a6",  
    ],

    "vp_stt": [
        "batch_68dead6f8bfc819096d501225b40c654",  # vp-stt (cancelled, partial) -> output_file_id 있음
        "batch_68dfeebad7f48190b979d6c71ee9a43b",  # vp-stt tail A17 (completed)
        "batch_68dfefe7c3948190b4d3c095b5832faa",  # vp-stt tail B17 (completed)
    ],
}

def fetch_output_if_needed(client: OpenAI, bid: str, out_dir: str) -> str | None:
   
    os.makedirs(out_dir, exist_ok=True)
    target = os.path.join(out_dir, f"{bid}_output.jsonl")

    b = client.batches.retrieve(bid)
    if not b.output_file_id:
        print(f"  - [skip] {bid}: output_file_id 없음 (status={b.status})")
        return None

    if os.path.exists(target) and os.path.getsize(target) > 0:
        print(f"  - [keep] {target} (기존 파일 사용)")
        return target

    print(f"  - [save] {target}")
    stream = client.files.content(b.output_file_id)
    with open(target, "wb") as f:
        f.write(stream.read())
    return target

def merge_and_dedupe_to_outputs(input_paths: list[str],
                                merged_path: str,
                                deid_only_path: str,
                                csv_path: str):
   
    seen = set()
    kept = 0

    os.makedirs(os.path.dirname(merged_path) or ".", exist_ok=True)

    with open(merged_path, "w", encoding="utf-8") as fout_all, \
         open(deid_only_path, "w", encoding="utf-8") as fout_deid, \
         open(csv_path, "w", encoding="utf-8", newline="") as fcsv:

        fcsv.write("custom_id,deidentified_text\n")

        for path in input_paths:
            if not path or not os.path.exists(path):
                print(f"  - [skip] {path} (없음)")
                continue

            print(f"  - [merge] {os.path.basename(path)}")
            with open(path, "r", encoding="utf-8") as fin:
                for line in fin:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    cid = rec.get("custom_id", "")
                    if cid in seen:
                        continue
                    seen.add(cid)

                    fout_all.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    kept += 1

                    resp = rec.get("response", {})
                    if resp.get("status_code") == 200:
                        choices = (resp.get("body", {}) or {}).get("choices") or []
                        if choices:
                            content = choices[0]["message"]["content"]
                            fout_deid.write(json.dumps({"custom_id": cid, "content": content}, ensure_ascii=False) + "\n")
                            safe = content.replace('"', '""').replace("\n", "\\n")
                            fcsv.write(f"\"{cid}\",\"{safe}\"\n")

    print(f"  => merged: {merged_path}")
    print(f"  => deid_only: {deid_only_path}")
    print(f"  => csv: {csv_path}")
    print(f"  (kept {kept} unique custom_id)")

def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY 환경변수를 설정하세요 (.env 또는 쉘).")
    client = OpenAI(api_key=api_key)

    print("== Fetch outputs to batch_results/ ==")
    saved_map: dict[str, list[str]] = {}
    for group, bids in BATCH_GROUPS.items():
        print(f"\n[Group: {group}]")
        saved_paths = []
        for bid in bids:
            path = fetch_output_if_needed(client, bid, out_dir="batch_results")
            if path:
                saved_paths.append(path)
        saved_map[group] = saved_paths


    print("\n== Merge & Dedupe by group ==")

    nv_inputs = saved_map.get("non_vishing", [])
    merge_and_dedupe_to_outputs(
        nv_inputs,
        merged_path="outputs/non_vishing_merged.jsonl",
        deid_only_path="outputs/non_vishing_deid_only.jsonl",
        csv_path="outputs/non_vishing_deid_outputs.csv"
    )

    vp_inputs = saved_map.get("vp_stt", [])
    merge_and_dedupe_to_outputs(
        vp_inputs,
        merged_path="outputs/vp_stt_merged.jsonl",
        deid_only_path="outputs/vp_stt_deid_only.jsonl",
        csv_path="outputs/vp_stt_deid_outputs.csv"
    )

if __name__ == "__main__":
    main()
