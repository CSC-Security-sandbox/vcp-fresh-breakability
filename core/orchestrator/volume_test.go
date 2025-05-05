package orchestrator

// func TestGetVolume(t *testing.T) {
//	t.Run("WhenVolumeDoesNotExist", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		volume, err := orch.GetVolume(ctx, "non-existent-uuid")
//		assert.EqualError(tt, err, "volume not found")
//		assert.Nil(tt, volume, "Expected nil volume")
//	})
//
//	t.Run("WhenVolumeExists", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		node := &datamodel.Node{
//			BaseModel:       datamodel.BaseModel{UUID: "test-volume-uuid"},
//			Name:            "test_node",
//			AccountID:       account.ID,
//			EndpointAddress: "12.12.12.12",
//			PoolID:          pool.ID,
//		}
//		err = store.DB().Create(node).Error
//		assert.NoError(tt, err, "Failed to create node")
//
//		volume := &datamodel.Volume{
//			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
//			Name:      "test_volume",
//			AccountID: account.ID,
//			Pool:      pool,
//			PoolID:    pool.ID,
//		}
//		err = store.DB().Create(volume).Error
//		assert.NoError(tt, err, "Failed to create volume")
//
//		result, err := orch.GetVolume(ctx, "test-volume-uuid")
//		assert.NoError(tt, err, "Failed to get volume")
//		assert.Equal(tt, volume.Name, result.DisplayName)
//		assert.Equal(tt, account.Name, result.AccountName)
//	})
// }
//
// func TestCreateVolume(t *testing.T) {
//	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
//		ctx := context.Background()
//		se := database.Storage(nil)
//
//		params := &CreateVolumeParams{
//			AccountName:  "test_account",
//			Region:       "test_region",
//			Name:         "test_pool",
//			VendorID:     "test_vendor",
//			QuotaInBytes: minQuotaInBytesPool,
//			Protocols:    []string{"NFS"},
//			Description:  "Some description",
//			DisplayName:  "Some display name",
//		}
//
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return nil, errors.New("account not found")
//		}
//
//		volume, err := createVolume(ctx, se, params)
//		assert.EqualError(tt, err, "account not found")
//		assert.Nil(tt, volume)
//	})
//	t.Run("WhenValidateCreateVolumeParamFails", func(tt *testing.T) {
//		ctx := context.Background()
//		se := database.Storage(nil)
//
//		params := &CreateVolumeParams{
//			AccountName:  "test_account",
//			Region:       "test_region",
//			Name:         "test_pool",
//			VendorID:     "test_vendor",
//			QuotaInBytes: minQuotaInBytesPool,
//			Protocols:    []string{"NFS"},
//			Description:  "Some description",
//			DisplayName:  "Some display name",
//		}
//
//		dbAccount := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{
//				UUID: "test-uuid",
//			},
//			Name: "test_account",
//		}
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return dbAccount, nil
//		}
//		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return errors.New("invalid volume params")
//		}
//		defer func() {
//			getOrCreateAccount = _getOrCreateAccount
//			validateCreateVolumeParams = _validateCreateVolumeParams
//		}()
//
//		_, err := createVolume(ctx, se, params)
//		assert.EqualError(tt, err, "invalid volume params")
//	})
//	t.Run("WhenGetPoolForCreateVolumeFails", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		// Create a PersistenceStore instance with the in-memory database
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			t.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		params := &CreateVolumeParams{
//			AccountName:  "test_account",
//			Region:       "test_region",
//			Name:         "test_pool",
//			VendorID:     "test_vendor",
//			QuotaInBytes: minQuotaInBytesPool,
//			Protocols:    []string{"NFS"},
//			Description:  "Some description",
//			DisplayName:  "Some display name",
//			PoolID:       "test-pool-uuid",
//		}
//
//		dbAccount := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{
//				UUID: "test-uuid",
//			},
//			Name: "test_account",
//		}
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return dbAccount, nil
//		}
//		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return nil
//		}
//		defer func() {
//			getOrCreateAccount = _getOrCreateAccount
//			validateCreateVolumeParams = _validateCreateVolumeParams
//		}()
//		volume, err := createVolume(ctx, store, params)
//		assert.Nil(tt, volume, "Expected nil volume")
//		assert.EqualError(tt, err, "pool not found")
//	})
//	t.Run("WhenGetSvmForCreateVolumeFails", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		// Create a PersistenceStore instance with the in-memory database
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			t.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create pool: %v", err)
//		}
//
//		params := &CreateVolumeParams{
//			AccountName:  "test_account",
//			Region:       "test_region",
//			Name:         "test_pool",
//			VendorID:     "test_vendor",
//			QuotaInBytes: minQuotaInBytesPool,
//			Protocols:    []string{"NFS"},
//			Description:  "Some description",
//			DisplayName:  "Some display name",
//			PoolID:       "test-pool-uuid",
//		}
//
//		dbAccount := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{
//				UUID: "test-uuid",
//			},
//			Name: "test_account",
//		}
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return dbAccount, nil
//		}
//		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return nil
//		}
//		defer func() {
//			getOrCreateAccount = _getOrCreateAccount
//			validateCreateVolumeParams = _validateCreateVolumeParams
//		}()
//		volume, err := createVolume(ctx, store, params)
//		assert.Nil(tt, volume, "Expected nil volume")
//		assert.EqualError(tt, err, "svm not found")
//	})
//	t.Run("WhenCreateVolumeSuccess", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		// Create a PersistenceStore instance with the in-memory database
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			t.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create pool: %v", err)
//		}
//
//		svm := &datamodel.Svm{
//			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
//			Name:      "test_svm",
//			AccountID: account.ID,
//			PoolID:    pool.ID,
//			Pool:      pool,
//		}
//
//		err = store.DB().Create(svm).Error
//		if err != nil {
//			tt.Fatalf("Failed to create svm: %v", err)
//		}
//
//		params := &CreateVolumeParams{
//			AccountName:    "test_account",
//			Region:         "test_region",
//			Name:           "test_volume",
//			VendorID:       "test_vendor",
//			QuotaInBytes:   minQuotaInBytesPool,
//			Protocols:      []string{"NFS"},
//			Description:    "Some description",
//			DisplayName:    "Some display name",
//			PoolID:         "test-pool-uuid",
//			CreationToken:  "test-creation-token",
//			Network: "test-subnet-id",
//		}
//
//		dbAccount := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{
//				UUID: "test-uuid",
//			},
//			Name: "test_account",
//		}
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return dbAccount, nil
//		}
//		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return nil
//		}
//		createVolumeAsync = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return nil
//		}
//		defer func() {
//			getOrCreateAccount = _getOrCreateAccount
//			validateCreateVolumeParams = _validateCreateVolumeParams
//			createVolumeAsync = _createVolumeAsync
//		}()
//		volume, err := orch.CreateVolume(ctx, params)
//		assert.NotNil(tt, volume, "Expected nil volume")
//		assert.NoError(tt, err, "error not found")
//		assert.Equal(tt, volume.DisplayName, "test_volume")
//		assert.Equal(tt, volume.AccountName, "test_account")
//		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
//		assert.Equal(tt, volume.PoolName, "test_pool")
//		assert.Equal(tt, volume.VendorID, "")
//		assert.Equal(tt, volume.Network, "test-subnet-id")
//		assert.Equal(tt, volume.CreationToken, "test-creation-token")
//		assert.Equal(tt, volume.Description, "Some description")
//		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
//		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
//		assert.Equal(tt, volume.LifeCycleState, "CREATING")
//		assert.Equal(tt, volume.LifeCycleStateDetails, "Creation in progress")
//	})
//	t.Run("WhenCreateVolumeAsyncFails", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		// Create a PersistenceStore instance with the in-memory database
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			t.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create pool: %v", err)
//		}
//
//		svm := &datamodel.Svm{
//			BaseModel: datamodel.BaseModel{UUID: "test-svm-uuid"},
//			Name:      "test_svm",
//			AccountID: account.ID,
//			PoolID:    pool.ID,
//			Pool:      pool,
//		}
//
//		err = store.DB().Create(svm).Error
//		if err != nil {
//			tt.Fatalf("Failed to create svm: %v", err)
//		}
//
//		params := &CreateVolumeParams{
//			AccountName:    "test_account",
//			Region:         "test_region",
//			Name:           "test_volume",
//			VendorID:       "test_vendor",
//			QuotaInBytes:   minQuotaInBytesPool,
//			Protocols:      []string{"NFS"},
//			Description:    "Some description",
//			DisplayName:    "Some display name",
//			PoolID:         "test-pool-uuid",
//			CreationToken:  "test-creation-token",
//			Network: "test-subnet-id",
//		}
//
//		dbAccount := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{
//				UUID: "test-uuid",
//			},
//			Name: "test_account",
//		}
//		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
//			return dbAccount, nil
//		}
//		validateCreateVolumeParams = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return nil
//		}
//		createVolumeAsync = func(ctx context.Context, se database.Storage, params *CreateVolumeParams) error {
//			return errors.New("failed to create volume async")
//		}
//		defer func() {
//			getOrCreateAccount = _getOrCreateAccount
//			validateCreateVolumeParams = _validateCreateVolumeParams
//			createVolumeAsync = _createVolumeAsync
//		}()
//		volume, err := orch.CreateVolume(ctx, params)
//		assert.NotNil(tt, volume, "Expected nil volume")
//		assert.NoError(tt, err, "error not found")
//		assert.Equal(tt, volume.DisplayName, "test_volume")
//		assert.Equal(tt, volume.AccountName, "test_account")
//		assert.Equal(tt, volume.PoolID, "test-pool-uuid")
//		assert.Equal(tt, volume.PoolName, "test_pool")
//		assert.Equal(tt, volume.VendorID, "")
//		assert.Equal(tt, volume.Network, "test-subnet-id")
//		assert.Equal(tt, volume.CreationToken, "test-creation-token")
//		assert.Equal(tt, volume.Description, "Some description")
//		assert.Equal(tt, volume.ProtocolTypes, []string{"NFS"})
//		assert.Equal(tt, volume.QuotaInBytes, minQuotaInBytesPool)
//		assert.Equal(tt, volume.LifeCycleState, "CREATING")
//		assert.Equal(tt, volume.LifeCycleStateDetails, "Creation in progress")
//	})
// }
//
// func TestDeleteVolume(t *testing.T) {
//	t.Run("WhenGetVolumeNotFound", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		deleteVolumeAsync = func(ctx context.Context, se database.Storage, volumeId string) error {
//			return nil
//		}
//		defer func() {
//			deleteVolumeAsync = _deleteVolumeAsync
//		}()
//
//		_, err = orch.DeleteVolume(ctx, "non-existent-uuid")
//		assert.EqualError(tt, err, "volume not found")
//	})
//	t.Run("WhenVolumeExistsAndSuccess", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		volume := &datamodel.Volume{
//			BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
//			Name:      "test_volume",
//			AccountID: account.ID,
//			Pool:      pool,
//			PoolID:    pool.ID,
//		}
//		err = store.DB().Create(volume).Error
//		assert.NoError(tt, err, "Failed to create volume")
//
//		deleteVolumeAsync = func(ctx context.Context, se database.Storage, volumeId string) error {
//			return nil
//		}
//		defer func() {
//			deleteVolumeAsync = _deleteVolumeAsync
//		}()
//
//		volumeResp, err := orch.DeleteVolume(ctx, "test-volume-uuid")
//		assert.NoError(tt, err, "Failed to get volume")
//		assert.Equal(tt, volume.Name, volumeResp.DisplayName)
//		assert.Equal(tt, account.Name, volumeResp.AccountName)
//		assert.Equal(tt, volumeResp.LifeCycleState, models.LifeCycleStateDeleting)
//		assert.Equal(tt, volumeResp.LifeCycleStateDetails, models.LifeCycleStateDeletingDetails)
//	})
//	t.Run("WhenVolumeAlreadyDeletingVolume", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		volume := &datamodel.Volume{
//			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
//			Name:         "test_volume",
//			AccountID:    account.ID,
//			Pool:         pool,
//			PoolID:       pool.ID,
//			State:        models.LifeCycleStateDeleting,
//			StateDetails: models.LifeCycleStateDeletingDetails,
//		}
//		err = store.DB().Create(volume).Error
//		assert.NoError(tt, err, "Failed to create volume")
//
//		deleteVolumeAsync = func(ctx context.Context, se database.Storage, volumeId string) error {
//			return nil
//		}
//		defer func() {
//			deleteVolumeAsync = _deleteVolumeAsync
//		}()
//
//		volumeResp, err := orch.DeleteVolume(ctx, "test-volume-uuid")
//		assert.EqualError(tt, err, "volume is already in deleting state")
//		assert.Nil(tt, volumeResp, "Expected nil volume")
//	})
//	t.Run("WhenVolumeAlreadyDeletingVolumeAndAsyncFlowFails", func(tt *testing.T) {
//		ctx := context.Background()
//
//		mockLogger := log.NewLogger()
//		store, err := database.NewTestStorage(mockLogger)
//		if err != nil {
//			tt.Fatalf("Failed to create test storage: %v", err)
//		}
//
//		// Clear the in-memory database
//		err = database.ClearInMemoryDB(store.DB())
//		if err != nil {
//			t.Fatalf("Failed to clean up test storage: %v", err)
//		}
//
//		orch := Orchestrator{
//			storage: store,
//		}
//
//		account := &datamodel.Account{
//			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
//			Name:      "test_account",
//		}
//		err = store.DB().Create(account).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		pool := &datamodel.Pool{
//			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
//			Name:      "test_pool",
//			AccountID: account.ID,
//		}
//
//		err = store.DB().Create(pool).Error
//		if err != nil {
//			tt.Fatalf("Failed to create account: %v", err)
//		}
//
//		volume := &datamodel.Volume{
//			BaseModel:    datamodel.BaseModel{UUID: "test-volume-uuid"},
//			Name:         "test_volume",
//			AccountID:    account.ID,
//			Pool:         pool,
//			PoolID:       pool.ID,
//			State:        models.LifeCycleStateDeleting,
//			StateDetails: models.LifeCycleStateDeletingDetails,
//		}
//		err = store.DB().Create(volume).Error
//		assert.NoError(tt, err, "Failed to create volume")
//
//		deleteVolumeAsync = func(ctx context.Context, se database.Storage, volumeId string) error {
//			return errors.New("Some error")
//		}
//		defer func() {
//			deleteVolumeAsync = _deleteVolumeAsync
//		}()
//
//		volumeResp, err := orch.DeleteVolume(ctx, "test-volume-uuid")
//		assert.EqualError(tt, err, "volume is already in deleting state")
//		assert.Nil(tt, volumeResp, "Expected nil volume")
//	})
// }
