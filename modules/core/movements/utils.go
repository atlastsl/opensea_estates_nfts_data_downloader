package movements

import (
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/helpers"
	"slices"
)

func getOpsEventTransactionHash(event *ops_events.EstateEvent) string {
	if event.Transaction != "" {
		return event.Transaction
	}
	return event.FixedTransaction
}

func compareOpsEvents(a, b *ops_events.EstateEvent) bool {
	if a == nil && b == nil {
		return true
	} else if a == nil || b == nil {
		return false
	} else {
		return a.Contract == b.Contract && a.Collection == b.Collection && a.AssetId == b.AssetId && a.EventType == b.EventType && getOpsEventTransactionHash(a) == getOpsEventTransactionHash(b) && a.Sender == b.Sender && b.Recipient == b.Recipient
	}
}

func cleanTxHashOpsEvents(opsEvents []*ops_events.EstateEvent) []*ops_events.EstateEvent {
	filteredEvents := make([]*ops_events.EstateEvent, 0)
	assetsIds := make([]string, 0)
	if opsEvents != nil && len(opsEvents) > 0 {
		for _, event := range opsEvents {
			found := slices.ContainsFunc(filteredEvents, func(t *ops_events.EstateEvent) bool {
				return compareOpsEvents(event, t)
			})
			if !found {
				filteredEvents = append(filteredEvents, event)
			}
			if !slices.Contains(assetsIds, event.AssetId) {
				assetsIds = append(assetsIds, event.AssetId)
			}
		}
	}
	cleanedEvents := make([]*ops_events.EstateEvent, 0)
	for _, assetId := range assetsIds {
		assetTransfers := helpers.ArrayFilter(filteredEvents, func(event *ops_events.EstateEvent) bool {
			return event.AssetId == assetId && event.EventType == "transfer"
		})
		assetSales := helpers.ArrayFilter(filteredEvents, func(event *ops_events.EstateEvent) bool {
			return event.AssetId == assetId && event.EventType == "sale"
		})
		sender, receiver := "", ""
		assetEvent := new(ops_events.EstateEvent)
		if len(assetTransfers) > 0 {
			senders := helpers.ArrayMap(assetTransfers, func(t *ops_events.EstateEvent) (bool, string) {
				return true, t.Sender
			}, true, "")
			receivers := helpers.ArrayMap(assetTransfers, func(t *ops_events.EstateEvent) (bool, string) {
				return true, t.Recipient
			}, true, "")
			fSenders := helpers.ArrayFilter(senders, func(s string) bool {
				return !slices.Contains(receivers, s)
			})
			fReceivers := helpers.ArrayFilter(receivers, func(s string) bool {
				return !slices.Contains(senders, s)
			})
			sender = fSenders[0]
			receiver = fReceivers[0]
		} else if len(assetSales) > 0 {
			sender = assetSales[0].Seller
			receiver = assetSales[0].Buyer
		}
		if len(assetSales) > 0 {
			assetEvent = assetSales[0]
			assetEvent.Sender = sender
			assetEvent.Recipient = receiver
			assetEvent.Seller = sender
			assetEvent.Buyer = receiver
		} else if len(assetTransfers) > 0 {
			assetEvent = assetTransfers[0]
			assetEvent.Sender = sender
			assetEvent.Recipient = receiver
		}
		if assetEvent != nil {
			cleanedEvents = append(cleanedEvents, assetEvent)
		}
	}
	return cleanedEvents
}

/**
* Filter Opensea Events (Take either transfer or sale)
 */
func filterOpenseaEventsOLD(opsEvents []*ops_events.EstateEvent, emptyHashSales []*ops_events.EstateEvent) (filtered []*ops_events.EstateEvent) {
	filtered = make([]*ops_events.EstateEvent, 0)
	if opsEvents != nil && len(opsEvents) > 0 {
		transfers := helpers.ArrayFilter(opsEvents, func(event *ops_events.EstateEvent) bool {
			return event.EventType == "transfer"
		})
		sales := helpers.ArrayFilter(opsEvents, func(event *ops_events.EstateEvent) bool {
			return event.EventType == "sale"
		})
		for _, transfer := range transfers {
			relatedSale1 := slices.IndexFunc(sales, func(sale *ops_events.EstateEvent) bool {
				return sale.Transaction == transfer.Transaction && sale.Collection == transfer.Collection && sale.Contract == transfer.Contract && sale.AssetId == transfer.AssetId
			})
			relatedSale2 := slices.IndexFunc(emptyHashSales, func(sale *ops_events.EstateEvent) bool {
				return sale.Transaction == transfer.Transaction && sale.Collection == transfer.Collection && sale.Contract == transfer.Contract && sale.AssetId == transfer.AssetId
			})
			if relatedSale1 >= 0 {
				filtered = append(filtered, sales[relatedSale1])
			} else if relatedSale2 >= 0 {
				sale := *emptyHashSales[relatedSale2]
				sale.Transaction = transfer.Transaction
				filtered = append(filtered, &sale)
			} else {
				filtered = append(filtered, transfer)
			}
		}
		for _, sale := range sales {
			relatedTransfer := slices.IndexFunc(transfers, func(transfer *ops_events.EstateEvent) bool {
				return sale.Transaction == transfer.Transaction && sale.Collection == transfer.Collection && sale.Contract == transfer.Contract && sale.AssetId == transfer.AssetId
			})
			if relatedTransfer <= 0 {
				filtered = append(filtered, sale)
			}
		}
	}
	return
}
