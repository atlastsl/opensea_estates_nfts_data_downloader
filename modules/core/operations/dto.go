package operations

import (
	"decentraland_data_downloader/modules/core/transactions_infos"
)

type TransactionLogInfo struct {
	EventName        string
	IsMetaverseAsset bool
	From             string
	To               string
	Amount           string
	Asset            string
	Land             string
	Estate           string
	TransactionLog   *transactions_infos.TransactionLog
}

type TransactionFull struct {
	Transaction *transactions_infos.TransactionInfo
	Logs        []*transactions_infos.TransactionLog
}

type BlockNumberInput struct {
	BlockNumber int    `json:"block_number"`
	Blockchain  string `json:"blockchain"`
}
