
import os, json
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

model = "gpt-4o-mini"
MAX_BYTES = 90 * 1024 * 1024
batch_folder = "./batch_inputs"
os.makedirs(batch_folder, exist_ok=True)

TAIL = "./sms/vp_stt_labeled_tail.jsonl"

with open("sms_prompt.txt", "r", encoding="utf-8") as f:
    prompt_template = f.read().strip()

base = os.path.splitext(os.path.basename(TAIL))[0]
with open(TAIL, "r", encoding="utf-8") as f:
    total_lines = sum(1 for _ in f)

chunk_idx = 0
fout = None
running = 0

with open(TAIL, "r", encoding="utf-8") as fin:
    for line in tqdm(fin, total=total_lines, desc=f"Chunking {base}"):
        record = json.loads(line)
        text = record.get("text", record.get("message", ""))
        if not isinstance(text, str):
            text = "" if text is None else str(text)

        prompt = f"{prompt_template}\n\n### Now de-identify the following message:\n{text}"
        batch_request = {
            "custom_id": f"{base}-{chunk_idx}-{running}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
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
            chunk_name = f"{base}_{model}_batch_{chunk_idx:02d}.jsonl"
            fout = open(os.path.join(batch_folder, chunk_name), "wb")
            print(f"[New chunk] {chunk_name}")

        fout.write(json_line)
        running += len(json_line)

if fout:
    fout.close()

# 업로드 & 배치 생성
for chunk_file in sorted(os.listdir(batch_folder)):
    if not chunk_file.startswith(base) or not chunk_file.endswith(".jsonl"):
        continue
    path = os.path.join(batch_folder, chunk_file)
    print(f"[Uploading] {path}")
    with open(path, "rb") as bf:
        file_resp = client.files.create(file=bf, purpose="batch")
    batch = client.batches.create(
        input_file_id=file_resp.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": f"de-id {base} chunk {chunk_file}"}
    )
    print(f"[Batch Created] {chunk_file} -> {batch.id} ({batch.status})")
