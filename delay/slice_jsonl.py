import sys
from itertools import islice


def main():
    if len(sys.argv) < 4:
        print("usage: python slice_jsonl.py <in> <out> <start> [<end>]")
        return
    in_path, out_path = sys.argv[1], sys.argv[2]
    start = int(sys.argv[3])
    end = int(sys.argv[4]) if len(sys.argv) > 4 else None
    with open(in_path, "r", encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout:
        for line in islice(fin, start, end):
            fout.write(line)
    print("[saved]", out_path)

if __name__ == "__main__":
    main()
