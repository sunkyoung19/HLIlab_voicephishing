import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

STUCK = "batch_68dfab78330c81908bd59e9909634193"  
b = client.batches.cancel(STUCK)
print("cancel sent:", b.id, b.status)