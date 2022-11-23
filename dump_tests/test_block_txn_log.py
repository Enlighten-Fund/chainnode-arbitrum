from pathlib import Path
import json
import requests
from tqdm import tqdm
import base64
from collections import Counter

PER_DIR = 10000
PER_FILE = 100
FILE_PER_DIR = PER_DIR // PER_FILE

DATA_DIR = Path("/data_ssd/yangdong/dump")
LOG_RANGE = list(range(404593, 404634))
URL = "http://localhost:8547"

total_txn, total_log = 0, 0
tot_txn_typ = []
for logno in LOG_RANGE:
    unseen_txns = {}
    with open(DATA_DIR / "blocks" / f"{logno // FILE_PER_DIR}" / f"{logno}.log") as f:
        data = {}
        for line in f:
            rec = json.loads(line)
            data[rec["blockNumber"]] = rec

        for i in tqdm(range(PER_FILE)):
            blockno = logno * PER_FILE + i
            payload = {
                "jsonrpc": "2.0",
                "method":"eth_getBlockByNumber",
                "params": [hex(blockno), True],
                "id": blockno,
            }
            response = requests.post(URL, json=payload).json()
            assert response["jsonrpc"] == "2.0", f"{response}"
            assert int(response["id"]) == blockno, f"{response}"
            result = response["result"]

            if blockno not in data:
                print(f"Warning: block #{blockno} is missing")
                assert len(result["transactions"]) == 0, result
                continue
            rec = data[blockno]
            # check all 10 dumped fields
            assert result["hash"] == rec["blockHash"]
            assert int(result["number"], 16) == blockno and blockno == rec["blockNumber"]
            assert int(result["difficulty"], 16) == rec["difficulty"]
            assert int(result["gasLimit"], 16) == rec["gasLimit"]
            assert int(result["gasUsed"], 16) == rec["gasUsed"]
            assert result["miner"] == rec["miner"]
            assert int(result["nonce"], 16) == rec["nonce"]
            assert result["parentHash"] == rec["parentHash"]
            assert int(result["size"], 16) == rec["size"]
            assert int(result["timestamp"], 16) == rec["timestamp"]
            assert int(result["l1BlockNumber"], 16) == rec["l1BlockNumber"]
            assert int(result["baseFeePerGas"], 16) == rec["baseFeePerGas"]
            
            for txn in result["transactions"]:
                unseen_txns[txn["hash"]] = txn
        print(f"ok. Verified {PER_FILE} BLOCK info in {logno}.log")
    txn_num = len(unseen_txns)

    dumped_logs = {}
    with open(DATA_DIR / "receipts" / f"{logno // 100}" / f"{logno}.log") as f:
        for line in f:
            rec = json.loads(line)
            key = (rec["blockNumber"], rec["logIndex"])
            assert key not in dumped_logs, f"Duplicate log: key={key}, log={rec}"
            dumped_logs[key] = rec

    with open(DATA_DIR / "transactions" / f"{logno // 100}" / f"{logno}.log") as f:
        ct, cl = 0, 0
        typ = []
        for line in tqdm(f):
            ct += 1
            rec = json.loads(line)
            txn = rec["transactionHash"]
            assert txn in unseen_txns, f"ERROR: txn is not dumped: {txn}"
            result = unseen_txns.pop(txn)
            assert rec["accessList"] is None or len(rec["accessList"]) == 0, rec
            assert rec["blockHash"] == result["blockHash"]
            assert rec["blockNumber"] == int(result["blockNumber"], 16)
            assert (result["input"] == "0x" and rec["data"] is None) or ("0x" + base64.b64decode(rec["data"]).hex() == result["input"])
            assert rec["from"] == result["from"]
            assert rec["gas"] == int(result["gas"], 16)
            assert rec["nonce"] == int(result["nonce"], 16)
            assert rec["to"] == result["to"]
            assert rec["transactionHash"] == result["hash"]
            assert rec["transactionIndex"] == int(result["transactionIndex"], 16)
            assert rec["type"] == int(result["type"], 16)
            assert rec["value"] == int(result["value"], 16)
            # gasTipCap is meaningless for arbitrum Nitro.
            if rec["type"] in {0, 1, 100, 106}:
                # legacy.
                assert rec["gasFeeCap"] == rec["gasPrice"] == int(result["gasPrice"], 16), f"DUMPED:\n{rec}\nEXPECT:\n{result}"
            elif rec["type"] in {2, 104, 105}:
                # EIP-1559
                assert rec["gasPrice"] == rec["gasFeeCap"] == int(result["maxFeePerGas"], 16)
            else:
                assert False, f"Unseen transaction type {rec['type']}\nDUMPED\n{rec}\nExpect\n{result}"
            optional = ["requestId","ticketId","maxRefund","submissionFeeRefund","refundTo","l1BaseFee","retryTo","retryValue","retryData","beneficiary","maxSubmissionFee"]
            for field in optional:
                assert (field in rec) == (field in result)
                if field in rec:
                    assert rec[field] == result[field]
            typ.append(rec["type"])
            tot_txn_typ.append(rec["type"])

            payload = {
                "jsonrpc": "2.0",
                "method":"eth_getTransactionReceipt",
                "params": [txn],
                "id": "1",
            }
            response = requests.post(URL, json=payload).json()
            assert response["jsonrpc"] == "2.0", f"{response}"
            assert int(response["id"]) == 1, f"{response}"
            result = response["result"]
            assert rec["effectiveGasPrice"] == result["effectiveGasPrice"],  f"effectiveGasPrice differs\nDUMPED\n{rec}\nExpect\n{result}"
            assert rec["gasUsed"] == int(result["gasUsed"], 16)
            assert rec["gasUsedForL1"] == int(result["gasUsedForL1"], 16)
            assert rec["l1BlockNumber"] == int(result["l1BlockNumber"], 16)
            assert rec["status"] == int(result["status"], 16)
            cl += len(result["logs"])
            for log in result["logs"]:
                nkey = (log["blockNumber"], log["logIndex"])
                assert nkey in dumped_logs, f"Log not found: key={nkey}, log={log}"
                dlog = dumped_logs.pop(nkey)
                assert dlog["address"] == log["address"]
                assert dlog["topics"] == log["topics"]
                assert dlog["data"] == log["data"]
                assert dlog["blockNumber"] == log["blockNumber"]
                assert dlog["transactionHash"] == log["transactionHash"]
                assert dlog["transactionIndex"] == log["transactionIndex"]
                assert dlog["blockHash"] == log["blockHash"], f"LOG:\n{log}\nDLOG:\n{dlog}\n"
                assert dlog["logIndex"] == log["logIndex"]
                assert dlog["removed"] == log["removed"]
        print(f"ok. Verified {ct} TRANSACTION info in {logno}.log. Txn types cnt: {Counter(typ)}")
        print(f"ok. Verified {cl} LOGS        info in {logno}.log.")
    assert len(unseen_txns) == 0, f"ERROR: not all dumped txns are valid: {unseen_txns.keys()}"
    assert len(dumped_logs) == 0, f"ERROR: not all dumped logs are valid: {dumped_logs.keys()}"
    total_txn += txn_num
    total_log += cl
print(f"OK! Verified {len(LOG_RANGE)*PER_FILE} blocks, {total_txn} txns, and {total_log} logs in total!")
print(f"Txn type stat: {Counter(tot_txn_typ)}")
