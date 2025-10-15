
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

out_id = "file-N6VQwPMJG58jhFCNzTjzKi"  
err_id = "file-Cq3ptRfnGd8eTRUaiVo6bG"   

s = client.files.content(out_id)
with open("vpstt_partial_result.jsonl", "wb") as f:
    f.write(s.read())

s2 = client.files.content(err_id)
with open("vpstt_partial_errors.jsonl", "wb") as f:
    f.write(s2.read())

print("saved partial outputs.")
