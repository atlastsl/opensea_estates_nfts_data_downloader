package ops_events

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"os"
	"slices"
	"sync"
	"time"
)

func parseEstateEventInfoProcess(estateEvent *helpers.OpenseaNftEvent) *EstateEvent {
	event := &EstateEvent{}
	event.CreatedAt = time.Now()
	event.UpdatedAt = time.Now()
	event.Collection = *estateEvent.Nft.Collection
	event.Contract = *estateEvent.Nft.Contract
	event.AssetId = *estateEvent.Nft.Identifier
	event.Transaction = *estateEvent.Transaction
	event.EventType = *estateEvent.EventType
	if estateEvent.ProtocolAddress != nil {
		event.Exchange = *estateEvent.ProtocolAddress
	}
	if estateEvent.Chain != nil {
		event.Chain = *estateEvent.Chain
	}
	if estateEvent.ClosingDate != nil {
		event.TxTimestamp = int64(*estateEvent.ClosingDate)
	}
	if estateEvent.EventTimestamp != nil {
		event.EvtTimestamp = int64(*estateEvent.EventTimestamp)
	}
	if event.EventType == "sale" {
		if estateEvent.Seller != nil {
			event.Seller = *estateEvent.Seller
			event.Sender = *estateEvent.Seller
		}
		if estateEvent.Buyer != nil {
			event.Buyer = *estateEvent.Buyer
			event.Recipient = *estateEvent.Buyer
		}
		if estateEvent.Payment != nil {
			if estateEvent.Payment.Symbol != nil {
				event.Currency = *estateEvent.Payment.Symbol
			}
			if estateEvent.Payment.TokenAddress != nil {
				event.CcyAddress = *estateEvent.Payment.TokenAddress
			}
			if estateEvent.Payment.Decimals != nil {
				event.CCyDecimals = int64(*estateEvent.Payment.Decimals)
			}
			if estateEvent.Payment.Quantity != nil && estateEvent.Payment.Decimals != nil {
				amt, _ := helpers.ConvertBigAmountToFloat64(*estateEvent.Payment.Quantity, *estateEvent.Payment.Decimals)
				event.Amount = amt
			}
		}
	} else if event.EventType == "transfer" {
		if estateEvent.FromAddress != nil {
			event.Sender = *estateEvent.FromAddress
		}
		if estateEvent.ToAddress != nil {
			event.Recipient = *estateEvent.ToAddress
		}
	}
	if estateEvent.Quantity != nil {
		event.Quantity = int64(*estateEvent.Quantity)
	}
	return event
}

func saveParsedEvents(events []*EstateEvent) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)
	err = saveOpsEventInDatabase(events, dbInstance)
	return err
}

func parseEstateEventInfo(collection collections.Collection, estateEvents []*helpers.OpenseaNftEvent, wg *sync.WaitGroup) error {
	var validContracts = make([]string, 0)
	if collection == collections.CollectionDcl {
		validContracts = []string{os.Getenv("DECENTRALAND_LAND_CONTRACT"), os.Getenv("DECENTRALAND_ESTATE_CONTRACT")}
	}
	if estateEvents != nil && len(estateEvents) > 0 {
		events := make([]*EstateEvent, 0)
		for _, estateEvent := range estateEvents {
			if estateEvent != nil && estateEvent.Nft != nil && estateEvent.Nft.Collection != nil && estateEvent.Nft.Identifier != nil && estateEvent.Nft.Contract != nil && estateEvent.EventType != nil && estateEvent.Transaction != nil {
				if slices.Contains(validContracts, *estateEvent.Nft.Contract) && *estateEvent.Nft.TokenStandard == "erc721" {
					events = append(events, parseEstateEventInfoProcess(estateEvent))
				}
			}
		}
		wg.Add(1)
		go func() {
			_ = saveParsedEvents(events)
			wg.Done()
		}()
	}
	return nil
}
