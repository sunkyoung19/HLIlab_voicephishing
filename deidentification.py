import os
import json
from tqdm import tqdm

from openai import OpenAI
from dotenv import load_dotenv


load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다. .env 또는 쉘에서 설정하세요.")

client = OpenAI(api_key=api_key)

with open("sms_prompt.txt", "r", encoding="utf-8") as f:
    prompt_template = f.read().strip()


input_folder = "./sms"
batch_folder = "./batch_inputs"
output_folder = "./batch_outputs"
os.makedirs(batch_folder, exist_ok=True)
os.makedirs(output_folder, exist_ok=True)

model = "gpt-4o-mini"
MAX_BYTES = 90 * 1024 * 1024    

for filename in os.listdir(input_folder):
    if not filename.endswith(".jsonl"):
        continue

    base = os.path.splitext(filename)[0]
    in_path = os.path.join(input_folder, filename)

    with open(in_path, "r", encoding="utf-8") as f:
        total_lines = sum(1 for _ in f)

    chunk_idx = 0
    fout = None

    with open(in_path, "r", encoding="utf-8") as fin:
        for line in tqdm(fin, total=total_lines, desc=f"Chunking {filename}"):
            if fout is None or fout.tell() > MAX_BYTES:     
                if fout:
                    fout.close()
                chunk_idx += 1
                chunk_name = f"{base}_{model}_batch_{chunk_idx:02d}.jsonl"
                batchinput_path = os.path.join(batch_folder, chunk_name)
                fout = open(batchinput_path, "w", encoding="utf-8")
                print(f"[New chunk] {batchinput_path}")

            try:
                record = json.loads(line)
            #오류처리
            except json.JSONDecodeError:
                print(f"[WARN] JSON parse 실패: {line[:80]}...")
                continue
            
            text = record.get("text", record.get("message", ""))
            if not isinstance(text, str):
                text = str(text) if text is not None else ""

            # 원본 매핑 정보(전처리 jsonl에 넣어둔 값 활용)
            source_index = record.get("source_index", record.get("id", ""))
            source_file = record.get("source_file", filename)

            # custom_id: 결과 ↔ 원본 CSV 매칭용
            custom_id = f"{base}|si:{source_index}|sf:{source_file}"

            prompt = prompt_template + "\n\n### Now de-identify the following message:\n" + text


            batch_request = {
                "custom_id": f"{base}-{chunk_idx}-{fout.tell()}",
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
            fout.write(json.dumps(batch_request, ensure_ascii=False) + "\n")

    if fout:
        fout.close()

    chunk_files = sorted([f for f in os.listdir(batch_folder)
                          if f.startswith(base) and f.endswith(".jsonl")])
   
    for chunk_file in tqdm(chunk_files, desc=f"[Uploading] {base}"):
        batchinput_path = os.path.join(batch_folder, chunk_file)

        with open(batchinput_path, "rb") as bf:
            file_resp = client.files.create(file=bf, purpose="batch")

        batch = client.batches.create(
            input_file_id=file_resp.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
            metadata={"description": f"de-id {filename} chunk {chunk_file}"}
        )
        print(f"[Batch Created] {chunk_file} -> {batch.id} ({batch.status})")


