import os
import time
from typing import Tuple, Dict

from dotenv import load_dotenv
from openai import OpenAI
from tqdm import tqdm

TERMINAL_STATUSES = {"completed", "failed", "cancelled", "expired"}


def get_counts(b) -> Tuple[int, int, int]:
    
    rc = getattr(b, "request_counts", None)
    total = getattr(rc, "total", 0) or 0
    done = getattr(rc, "completed", 0) or 0
    fail = getattr(rc, "failed", 0) or 0
    return total, done, fail


def watch_batches(batch_ids, poll_secs: int = 5):
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY 환경변수를 설정하세요 (.env 또는 쉘).")
    client = OpenAI(api_key=api_key)

    bars: Dict[str, tqdm] = {}

    try:
        while True:
            all_terminal = True
            for pos, bid in enumerate(batch_ids):
                try:
                    b = client.batches.retrieve(bid)
                except Exception as e:
                    # 네트워크 오류 등 — 진행바 설명에 표시
                    if bid not in bars:
                        bars[bid] = tqdm(total=1, position=pos, leave=True, desc=f"{bid} [error]")
                    bars[bid].set_description(f"{bid} [error]")
                    bars[bid].set_postfix_str(str(type(e).__name__))
                    continue

                status = b.status
                total, done, fail = get_counts(b)
                progressed = done + fail
                pct = (progressed / total * 100) if total else 0.0


                if bid not in bars:
                    bars[bid] = tqdm(total=total or 1, position=pos, leave=True, desc=f"{bid} [{status}]")
                else:

                    if total and bars[bid].total != total:
                        bars[bid].total = total


                bars[bid].n = min(progressed, bars[bid].total or 1)
                bars[bid].set_description(f"{bid} [{status}]")
                bars[bid].set_postfix_str(f"{pct:5.2f}% | done:{done} fail:{fail} total:{total}")
                bars[bid].refresh()

                if status not in TERMINAL_STATUSES:
                    all_terminal = False

            if all_terminal:
                break
            time.sleep(poll_secs)

    finally:

        for bid in batch_ids:
            try:
                b = client.batches.retrieve(bid)
                total, done, fail = get_counts(b)
                print(f"[{bid}] status={b.status}  done={done}  fail={fail}  total={total}")
            except Exception:
                pass
        for bar in bars.values():
            bar.close()


if __name__ == "__main__":

    batch_ids = [
        #"batch_68dead6ba0848190966ab16414f677a6",  nonvishing
        #"batch_68dead6f8bfc819096d501225b40c654",  vp-stt
        #"batch_68dfab78330c81908bd59e9909634193"   34개 파싱
        "batch_68dfeebad7f48190b979d6c71ee9a43b", #17개 청킹 A
        "batch_68dfefe7c3948190b4d3c095b5832faa" #17개 청킹 B
    ]
    watch_batches(batch_ids, poll_secs=10)  # 10초 간격 폴링
