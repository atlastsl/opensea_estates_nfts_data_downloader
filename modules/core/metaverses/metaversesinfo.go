package metaverses

import "go.mongodb.org/mongo-driver/mongo"

type MetaverseName string

const (
	EthereumBlockchain               = "ethereum"
	PolygonBlockchain                = "polygon"
	MetaverseDcl       MetaverseName = "decentraland"
	MetaverseSmn       MetaverseName = "somnium-space"
	MetaverseVxl       MetaverseName = "crypto-voxels"
	MetaverseSnd       MetaverseName = "the-sandbox"
)

var (
	Metaverses = []MetaverseName{MetaverseDcl, MetaverseSmn, MetaverseVxl, MetaverseSnd}
)

var DecentralandMtvInfo = &MetaverseInfo{
	Name:       string(MetaverseDcl),
	Blockchain: []string{EthereumBlockchain},
	Currency:   "MANA",
	Assets: []MetaverseInfoAsset{
		{
			Blockchain:   EthereumBlockchain,
			Name:         "land",
			Contract:     "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeRELand,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    true,
					FixedValue:  "1",
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
		{
			Blockchain:   EthereumBlockchain,
			Name:         "estate",
			Contract:     "0x959e104e1a4db6317fa58f8295f586e1a978c297",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeREEstate,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
				{
					AttrName:       MtvAssetAttrNameLands,
					AttrDisName:    MtvAssetAttrDisNameLands,
					Constant:       false,
					DataType:       MtvAssetAttrDataTypeString,
					DataTypeParams: map[string]any{"separator": "|"},
				},
			},
		},
	},
	LogTopics: []MetaverseInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "AddLandInEstate",
			Hash:       "0xff0e52667d53255667dc777a00af81038a4646367b0d73d8ee8540ca5b0c9a2e",
			Contracts:  []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "RemoveLandFromEstate",
			Hash:       "0x7932eb5ab0d4d4d172776074ee15d13d708465ff5476902ed15a4965434fcab1",
			Contracts:  []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
	},
}

var TheSandboxMtvInfo = &MetaverseInfo{
	Name:       string(MetaverseSnd),
	Blockchain: []string{EthereumBlockchain, PolygonBlockchain},
	Currency:   "SAND",
	Assets: []MetaverseInfoAsset{
		{
			Blockchain:   EthereumBlockchain,
			Name:         "land",
			Contract:     "0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeRELand,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    true,
					FixedValue:  "1",
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
		{
			Blockchain:   PolygonBlockchain,
			Name:         "land",
			Contract:     "0x9d305a42A3975Ee4c1C57555BeD5919889DCE63F",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeRELand,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    true,
					FixedValue:  "1",
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
	},
	LogTopics: []MetaverseInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38"},
			EndBlock:   0,
		},
		{
			Blockchain: PolygonBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x9d305a42A3975Ee4c1C57555BeD5919889DCE63F"},
			EndBlock:   0,
		},
	},
}

var SomniumSpaceMtvInfo = &MetaverseInfo{
	Name:       string(MetaverseSmn),
	Blockchain: []string{EthereumBlockchain},
	Currency:   "CUBE",
	Assets: []MetaverseInfoAsset{
		{
			Blockchain:   EthereumBlockchain,
			Name:         "land",
			Contract:     "0x913ae503153d9a335398d0785ba60a2d63ddb4e2",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeRELand,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
		{
			Blockchain:   EthereumBlockchain,
			Name:         "world",
			Contract:     "0xf980759616a795b2d692a5c0a0f1bad651984bc1",
			AssetType:    MtvAssetTypeService,
			AssetSubtype: MtvAssetStypeSrSpace,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
	},
	LogTopics: []MetaverseInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x913ae503153d9a335398d0785ba60a2d63ddb4e2"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x913ae503153d9a335398d0785ba60a2d63ddb4e2"},
			EndBlock:   0,
		},
	},
}

var CryptoVoxelsMtvInfo = &MetaverseInfo{
	Name:       string(MetaverseVxl),
	Blockchain: []string{EthereumBlockchain},
	Currency:   "-",
	Assets: []MetaverseInfoAsset{
		{
			Blockchain:   EthereumBlockchain,
			Name:         "land",
			Contract:     "0x79986aF15539de2db9A5086382daEdA917A9CF0C",
			AssetType:    MtvAssetTypeRealEstate,
			AssetSubtype: MtvAssetStypeRELand,
			Attrs: []MetaverseInfoAssetAttr{
				{
					AttrName:    MtvAssetAttrNameSize,
					AttrDisName: MtvAssetAttrDisNameSize,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeInteger,
				},
				{
					AttrName:    MtvAssetAttrNameOwner,
					AttrDisName: MtvAssetAttrDisNameOwner,
					Constant:    false,
					DataType:    MtvAssetAttrDataTypeString,
				},
			},
		},
	},
	LogTopics: []MetaverseInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x79986aF15539de2db9A5086382daEdA917A9CF0C"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x79986aF15539de2db9A5086382daEdA917A9CF0C"},
			EndBlock:   0,
		},
	},
}

func getMetaverseInfoAsset(mtvInfo *MetaverseInfo, name, blockchain string) *MetaverseInfoAsset {
	if mtvInfo == nil {
		return nil
	}
	for _, asset := range mtvInfo.Assets {
		if asset.Name == name && asset.Blockchain == blockchain {
			return &asset
		}
	}
	return nil
}

func getMetaverseInfo(metaverse MetaverseName, dbInstance *mongo.Database) (*MetaverseInfo, error) {
	mtvInfoDb, err := getMetaverseInfoInDatabase(metaverse, dbInstance)
	if err != nil {
		return nil, err
	}
	if mtvInfoDb == nil {
		if metaverse == MetaverseDcl {
			err = saveMetaverseInfoInDatabase(DecentralandMtvInfo)
			mtvInfoDb = DecentralandMtvInfo
		} else if metaverse == MetaverseSnd {
			err = saveMetaverseInfoInDatabase(TheSandboxMtvInfo)
			mtvInfoDb = TheSandboxMtvInfo
		} else if metaverse == MetaverseSmn {
			err = saveMetaverseInfoInDatabase(SomniumSpaceMtvInfo)
			mtvInfoDb = SomniumSpaceMtvInfo
		} else if metaverse == MetaverseVxl {
			err = saveMetaverseInfoInDatabase(CryptoVoxelsMtvInfo)
			mtvInfoDb = CryptoVoxelsMtvInfo
		}
		if err != nil {
			return nil, err
		}
	}
	return mtvInfoDb, nil
}
