print("run tail")
import os, sys, json
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

MODEL = "gpt-4o-mini"
MAX_BYTES = 90 * 1024 * 1024   
BATCH_FOLDER = "./batch_inputs"
PROMPT_FILE = "sms_prompt.txt"
BATCH_LIST_OUT = "./tail_batches.txt"

def main():
    if len(sys.argv) < 2:
        print("usage: python run_tail_generic.py <input_jsonl>")
        sys.exit(1)
    in_jsonl = sys.argv[1]
    if not os.path.isfile(in_jsonl):
        raise FileNotFoundError(in_jsonl)

    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY 환경변수가 없습니다. .env 또는 쉘에서 설정하세요.")
    client = OpenAI(api_key=api_key)

    os.makedirs(BATCH_FOLDER, exist_ok=True)

    with open(PROMPT_FILE, "r", encoding="utf-8") as f:
        prompt_template = f.read().strip()

    base = os.path.splitext(os.path.basename(in_jsonl))[0]


    with open(in_jsonl, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f)

    chunk_idx = 0
    fout = None
    running = 0
    made_chunks = []

    with open(in_jsonl, "r", encoding="utf-8") as fin:
        for line in tqdm(fin, total=total_lines, desc=f"Chunking {base}"):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                print("[WARN] JSON parse 실패, 라인 건너뜀")
                continue

            text = record.get("text", record.get("message", ""))
            if not isinstance(text, str):
                text = "" if text is None else str(text)

            rid = record.get("id")
            source_index = record.get("source_index", rid)
            source_file = record.get("source_file", os.path.basename(in_jsonl))

            custom_id = f"{base}|r:{rid}|si:{source_index}|sf:{source_file}"
            prompt = f"{prompt_template}\n\n### Now de-identify the following message:\n{text}"

            batch_request = {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a privacy-preserving preprocessing model."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0
                }
            }
            json_line = (json.dumps(batch_request, ensure_ascii=False) + "\n").encode("utf-8")

            if (fout is None) or (running + len(json_line) > MAX_BYTES):
                if fout:
                    fout.close()
                chunk_idx += 1
                running = 0
                chunk_name = f"{base}_{MODEL}_batch_{chunk_idx:02d}.jsonl"
                chunk_path = os.path.join(BATCH_FOLDER, chunk_name)
                fout = open(chunk_path, "wb")
                made_chunks.append(chunk_path)
                print(f"[New chunk] {chunk_path}")

            fout.write(json_line)
            running += len(json_line)

    if fout:
        fout.close()

    # 업로드 & 배치 생성
    made_batches = []
    for chunk_path in made_chunks:
        print(f"[Uploading] {chunk_path}")
        with open(chunk_path, "rb") as bf:
            file_resp = client.files.create(file=bf, purpose="batch")
        batch = client.batches.create(
            input_file_id=file_resp.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": f"de-id {base} chunk {os.path.basename(chunk_path)}"}
        )
        made_batches.append(batch.id)
        print(f"[Batch Created] {os.path.basename(chunk_path)} -> {batch.id} ({batch.status})")

    if made_batches:
        with open(BATCH_LIST_OUT, "a", encoding="utf-8") as f:
            for bid in made_batches:
                f.write(bid + "\n")
        print(f"[Saved batch ids] {BATCH_LIST_OUT}")

if __name__ == "__main__":
    main()
