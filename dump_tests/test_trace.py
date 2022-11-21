import json
import os

PER_DIR = 10000
PER_FILE = 100
FILE_PER_DIR = PER_DIR // PER_FILE
DIR = "/home/yangdong/chainnode-arbitrum/dump_tests/geth_traces"
files = os.listdir(DIR)
for fc, fname in enumerate(files):
    print(f"Testing {fname}...")
    output = []
    with open(f"{DIR}/{fname}") as f:
        rec = json.load(f)
        def dfs(ct, path):
            output.append({
                "type": "call" if ct["type"] != "CREATE" else "create",
                "action": {
                    "callType": ct["type"].lower(),
                    "from": ct["from"],
                    "to": ct["to"],
                    "gas": ct["gas"],
                    "input": ct["input"],
                    "value": "0x" if ("value" not in ct or int(ct["value"], 16) == 0) else ct["value"],
                },
                "result": {
                    "gasUsed": ct["gasUsed"],
                    "output": ct["output"],
                },
                "subtraces": len(ct["calls"]) if "calls" in ct else 0,
                "traceAddress": path,
                "blockHash": None,
                "blockNumber": None,
                "transactionHash": None,
                "transactionPosition": None,
                "transactionTraceID": 0,
                "transactionLastTrace": 0
            })
            if "calls" in ct:
                for i, c in enumerate(ct["calls"]):
                    dfs(c, path + [i])
        dfs(rec, [])
        for i, d in enumerate(output):
            d["transactionTraceID"] = i
        output[-1]["transactionLastTrace"] = 1

    blockno, txhash = fname[:-5].split("_")
    blockno = int(blockno)

    with open(f"/data_ssd/yangdong/dump/traces/{blockno//PER_DIR}/{blockno//PER_FILE}.log") as f:
        traces = [json.loads(line) for line in f]
        txtraces = list(filter(lambda x: x["transactionHash"] == txhash, traces))

        with open("expect.json", "w") as g:
            for rec in output:
                g.write(json.dumps(rec) + "\n")            

        with open("output.json", "w") as g:
            for rec in txtraces:
                g.write(json.dumps(rec) + "\n")            

        assert len(txtraces) == len(output), f"Length differ: expect {len(output)}, get {len(txtraces)}"
        print(f"Number of traces: {len(output)}")
        for dtrace, ctrace in zip(txtraces, output):
            ctrace["blockHash"] = dtrace["blockHash"]
            ctrace["blockNumber"] = dtrace["blockNumber"]
            ctrace["transactionHash"] = dtrace["transactionHash"]
            ctrace["transactionPosition"] = dtrace["transactionPosition"]
            assert ctrace == dtrace, f"Record differ\n{ctrace}\n{dtrace}"
    print(f"ok {fc+1}/{len(files)}")
print("OK!")
