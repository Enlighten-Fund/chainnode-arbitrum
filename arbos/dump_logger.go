package arbos

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path"
	"strconv"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/ethereum/go-ethereum/common"
)

func getFile(taskName string, blockNumber uint64, perFolder, perFile uint64) (*os.File, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("get current work dir failed: %w", err)
	}
	cwd = path.Join(cwd, "dump")
	logPath := path.Join(cwd, taskName, strconv.FormatUint(blockNumber/perFolder, 10), strconv.FormatUint(blockNumber/perFile, 10)+".log")
	fmt.Printf("log path: %v, block: %v\n", logPath, blockNumber)
	if err := os.MkdirAll(path.Dir(logPath), 0755); err != nil {
		return nil, fmt.Errorf("mkdir for all parents [%v] failed: %w", path.Dir(logPath), err)
	}

	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return nil, fmt.Errorf("create file %s failed: %w", logPath, err)
	}
	return file, nil
}

type ParityTraceItemAction struct {
	CallType string         `json:"callType"`
	From     common.Address `json:"from"`
	To       common.Address `json:"to"`
	Gas      hexutil.Uint64 `json:"gas"`
	Input    hexutil.Bytes  `json:"input"`
	Value    hexutil.Bytes  `json:"value"`
}

type ParityTraceItemResult struct {
	GasUsed hexutil.Uint64 `json:"gasUsed"`
	Output  hexutil.Bytes  `json:"output"`
}

type ParityTraceItem struct {
	Type                 string                `json:"type"`
	Action               ParityTraceItemAction `json:"action"`
	Result               ParityTraceItemResult `json:"result"`
	Subtraces            int                   `json:"subtraces"`
	TraceAddress         []int                 `json:"traceAddress"`
	Error                string                `json:"error,omitempty"`
	BlockHash            common.Hash           `json:"blockHash"`
	BlockNumber          uint64                `json:"blockNumber"`
	TransactionHash      common.Hash           `json:"transactionHash"`
	TransactionPosition  int                   `json:"transactionPosition"`
	TransactionTraceID   int                   `json:"transactionTraceID"`
	TransactionLastTrace int                   `json:"transactionLastTrace"`
}

type ParityLogContext struct {
	BlockHash   common.Hash
	BlockNumber uint64
	TxPos       int
	TxHash      common.Hash
}

type ParityLogger struct {
	context           *ParityLogContext
	encoder           *json.Encoder
	activePrecompiles []common.Address
	file              *os.File
	stack             []*ParityTraceItem
	items             []*ParityTraceItem
}

func ReceiptDumpLogger(blockHash common.Hash, blockNumber uint64, perFolder, perFile uint64, receipts types.Receipts) error {
	file, err := getFile("receipts", blockNumber, perFolder, perFile)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			oldHash := log.BlockHash
			log.BlockHash = blockHash
			err := encoder.Encode(log)
			log.BlockHash = oldHash
			if err != nil {
				return fmt.Errorf("encode log failed: %w", err)
			}
		}
	}
	return nil
}

type TxLogger struct {
	blockNumber uint64
	blockHash   common.Hash
	file        *os.File
	encoder     *json.Encoder
	signer      types.Signer
	isLondon    bool
	baseFee     *big.Int
}

func NewTxLogger(signer types.Signer, isLondon bool, baseFee *big.Int, blockHash common.Hash, blockNumber uint64, perFolder, perFile uint64) (*TxLogger, error) {
	file, err := getFile("transactions", blockNumber, perFolder, perFile)
	if err != nil {
		return nil, err
	}
	return &TxLogger{
		blockNumber: blockNumber,
		blockHash:   blockHash,
		file:        file,
		encoder:     json.NewEncoder(file),
		signer:      signer,
		isLondon:    isLondon,
		baseFee:     baseFee,
	}, nil
}

func (t *TxLogger) Dump(index int, tx *types.Transaction, receipt *types.Receipt) error {
	from, _ := types.Sender(t.signer, tx)
	// Assign the effective gas price paid
	effectiveGasPrice := hexutil.Uint64(tx.GasPrice().Uint64())
	if t.isLondon {
		gasPrice := new(big.Int).Add(t.baseFee, tx.EffectiveGasTipValue(t.baseFee))
		effectiveGasPrice = hexutil.Uint64(gasPrice.Uint64())
	}
	entry := map[string]interface{}{
		"blockNumber":       t.blockNumber,
		"blockHash":         t.blockHash,
		"transactionIndex":  index,
		"transactionHash":   tx.Hash(),
		"from":              from,
		"to":                tx.To(),
		"gas":               tx.Gas(),
		"gasUsed":           receipt.GasUsed,
		"gasPrice":          tx.GasPrice(),
		"data":              tx.Data(),
		"accessList":        tx.AccessList(),
		"nonce":             tx.Nonce(),
		"gasFeeCap":         tx.GasFeeCap(),
		"gasTipCap":         tx.GasTipCap(),
		"effectiveGasPrice": effectiveGasPrice,
		"type":              tx.Type(),
		"value":             tx.Value(),
		"status":            receipt.Status,
	}
	if err := t.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to encode transaction entry %w", err)
	}
	return nil
}

func (t *TxLogger) Close() error {
	return t.file.Close()
}

func BlockDumpLogger(block *types.Block, perFolder, perFile uint64) error {
	file, err := getFile("blocks", block.NumberU64(), perFolder, perFile)
	if err != nil {
		return err
	}
	defer file.Close()

	entry := map[string]interface{}{
		"timestamp":   block.Time(),
		"blockNumber": block.NumberU64(),
		"blockHash":   block.Hash(),
		"parentHash":  block.ParentHash(),
		"gasLimit":    block.GasLimit(),
		"gasUsed":     block.GasUsed(),
		"miner":       block.Coinbase(),
		"difficulty":  block.Difficulty(),
		"nonce":       block.Nonce(),
		"size":        block.Size(),
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to encode transaction entry %w", err)
	}

	return nil
}
