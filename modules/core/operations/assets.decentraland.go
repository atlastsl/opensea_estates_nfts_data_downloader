package operations

import (
	"decentraland_data_downloader/modules/core/metaverses"
	"fmt"
)

func dclGetAssetIdentifierFromLogs(mtvInfo *metaverses.MetaverseInfo, logsInfo []*TransactionLogInfo) []map[string]string {
	result := make([]map[string]string, 0)
	inserted := make(map[string]bool)

	estateInfo := mtvInfo.GetAsset("estate")
	landInfo := mtvInfo.GetAsset("land")

	for _, logInfo := range logsInfo {
		contract := logInfo.TransactionLog.Address
		assetId := ""
		if logInfo.Asset != "" {
			assetId = logInfo.Asset
		} else if landInfo != nil && landInfo.Contract == logInfo.TransactionLog.Address && logInfo.Land != "" {
			assetId = logInfo.Land
		} else if estateInfo != nil && estateInfo.Contract == logInfo.TransactionLog.Address && logInfo.Estate != "" {
			assetId = logInfo.Estate
		}
		if assetId != "" {
			key := fmt.Sprintf("%s_%s", contract, assetId)
			_, ok := inserted[key]
			if !ok {
				result = append(result, map[string]string{"contract": contract, "asset_id": assetId})
				inserted[key] = true
			}
		}
	}
	return result
}
