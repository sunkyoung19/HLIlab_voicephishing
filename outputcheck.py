import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()  
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise RuntimeError("OPENAI_API_KEY 환경변수가 설정되지 않았습니다.")

client = OpenAI(api_key=api_key)

output_file_id = "file-Xff4MwszL5bnRyGReyLT2r"  # 배치의 output_file_id


stream = client.files.content(output_file_id)   
with open("result.jsonl", "wb") as f:
    f.write(stream.read())

print("saved: result.jsonl")


batch_id = "batch_68dbf898b7448190a02c5f57effae3e9"
batch = client.batches.retrieve(batch_id)
if batch.error_file_id:
    err_stream = client.files.content(batch.error_file_id)
    with open("errors.jsonl", "wb") as f:
        f.write(err_stream.read())
    print("saved: errors.jsonl (failed requests)")
