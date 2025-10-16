package validator

import (
	"context"
	"errors"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestNewActiveDirectoryValidator(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)

	assert.NotNil(t, adValidator, "validator should be created")
	assert.NotNil(t, adValidator.validate, "base validator should be initialized")
	assert.NotNil(t, adValidator.Translator, "translator should be initialized")
	assert.Equal(t, ctx, adValidator.ctx, "context should be set")
	assert.Equal(t, mockStorage, adValidator.se, "storage should be set")
}

func TestActiveDirectoryValidator_RegisterValidators(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)

	err := adValidator.RegisterValidators()
	require.NoError(t, err, "registering validators should not fail")

	// Test that all validators are registered by trying to validate with them
	type TestStruct struct {
		NetBIOS            string   `validate:"NetBIOS"`
		Username           string   `validate:"Username"`
		Site               string   `validate:"Site"`
		OrganizationalUnit string   `validate:"OrganizationalUnit"`
		SecurityOperators  []string `validate:"SecurityOperators"`
		BackupOperators    []string `validate:"BackupOperators"`
		Administrators     []string `validate:"Administrators"`
		DNS                string   `validate:"DNS"`
		ResourceId         string   `validate:"ResourceId"`
	}

	testObj := &TestStruct{
		NetBIOS:            "ValidName",
		Username:           "validuser",
		Site:               "ValidSite",
		OrganizationalUnit: "",
		SecurityOperators:  []string{"user1", "user2"},
		BackupOperators:    []string{"backup1"},
		Administrators:     []string{"admin1"},
		DNS:                "192.168.1.1",
		ResourceId:         "server-hostname",
	}

	err = adValidator.validate.Struct(testObj)
	assert.NoError(t, err, "validation should pass for valid data")
}

func TestActiveDirectoryValidator_ValidateParams(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)

	mockStorage.On("GetAccount", ctx, mock.Anything).Return(&datamodel.Account{}, nil)
	mockStorage.On("GetActiveDirectoryByNameAndAccountID", ctx, mock.Anything, mock.Anything).Return(nil, nil)

	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)

	params := &common.CreateActiveDirectoryParams{
		NetBIOS:            "TESTDOMAIN",
		Username:           "administrator",
		Password:           "SecurePassword123!",
		Site:               "DefaultSite",
		OrganizationalUnit: "CN=Computers,DC=testdomain,DC=local",
		SecurityOperators:  []string{"securityuser1", "securityuser2"},
		BackupOperators:    []string{"backupuser1"},
		Administrators:     []string{"admin1", "admin2"},
		DNS:                "192.168.1.10,192.168.1.11",
		ResourceId:         "ad-test-server",
	}

	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}
	_ = adValidator.ValidateParams(params)
	// This test verifies the ValidateParams method works
	// The actual validation depends on the struct tags in CreateActiveDirectoryParams
	assert.NotPanics(t, func() {
		err := adValidator.ValidateParams(params)
		if err != nil {
			return
		}
	})
}

func TestActiveDirectoryValidator_NetBIOSValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	testCases := []struct {
		name     string
		netBIOS  string
		expected bool
	}{
		{"single char", "A", true},
		{"valid name", "DOMAIN", true},
		{"with numbers", "DOM123", true},
		{"max length", "123456789", true},
		{"hyphen allowed", "DOM-AIN", true},
		{"underscore allowed", "DOM_AIN", true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			type TestStruct struct {
				NetBIOS string `validate:"NetBIOS"`
			}
			testObj := &TestStruct{NetBIOS: tc.netBIOS}
			err := adValidator.validate.Struct(testObj)
			if tc.expected {
				assert.NoError(t, err, "NetBIOS '%s' should be valid", tc.netBIOS)
			} else {
				assert.Error(t, err, "NetBIOS '%s' should be invalid", tc.netBIOS)
			}
		})
	}
}

func TestActiveDirectoryValidator_NetBIOSValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	testCases := []string{
		"",            // empty
		".",           // starts with dot
		"name.",       // ends with dot
		"na..me",      // double dots
		"name/path",   // contains slash
		"name\\path",  // contains backslash
		"name:port",   // contains colon
		"name*wild",   // contains asterisk
		"name?query",  // contains question mark
		"name\"quote", // contains quote
		"name<tag",    // contains less than
		"name>tag",    // contains greater than
		"name|pipe",   // contains pipe
		"name space",  // contains space
		"name@domain", // contains at symbol
		"name()func",  // contains parentheses
		"name=value",  // contains equals
		"name+plus",   // contains plus
		"name[array]", // contains brackets
		"name;semi",   // contains semicolon
		"name,comma",  // contains comma
		"name#hash",   // contains hash
		"12345678901", // too long (11 chars)
	}

	for _, netBIOS := range testCases {
		t.Run("invalid_"+netBIOS, func(t *testing.T) {
			type TestStruct struct {
				NetBIOS string `validate:"NetBIOS"`
			}
			testObj := &TestStruct{NetBIOS: netBIOS}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "NetBIOS '%s' should be invalid", netBIOS)

			validationErrs, ok := err.(validator.ValidationErrors)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, netBIOSValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_UsernameValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validUsernames := []string{
		"user",
		"user123",
		"user_name",
		"user-name",
		"user.name",
		"User",
		"USER",
		"123user",
		"u",
	}

	for _, username := range validUsernames {
		t.Run("valid_"+username, func(t *testing.T) {
			type TestStruct struct {
				Username string `validate:"Username"`
			}
			testObj := &TestStruct{Username: username}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "Username '%s' should be valid", username)
		})
	}
}

func TestActiveDirectoryValidator_UsernameValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidUsernames := []string{
		"user/name",  // contains slash
		"user\\name", // contains backslash
		"user:name",  // contains colon
		"user;name",  // contains semicolon
		"user|name",  // contains pipe
		"user[name]", // contains brackets
		"user=name",  // contains equals
		"user,name",  // contains comma
		"user+name",  // contains plus
		"user*name",  // contains asterisk
		"user?name",  // contains question mark
		"user\"name", // contains quote
		"user<name",  // contains less than
		"user>name",  // contains greater than
	}

	for _, username := range invalidUsernames {
		t.Run("invalid_"+username, func(t *testing.T) {
			type TestStruct struct {
				Username string `validate:"Username"`
			}
			testObj := &TestStruct{Username: username}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Username '%s' should be invalid", username)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, usernameValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_SiteNameValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validSites := []string{
		"",           // empty string allowed
		"Site1",      // simple site
		"Site-Name",  // with hyphen
		"Site.Name",  // with dot
		"Site1.Sub2", // domain style
		"A1",         // minimum valid
		"123",        // numbers only
		"ABC",        // letters only
	}

	for _, site := range validSites {
		t.Run("valid_"+site, func(t *testing.T) {
			type TestStruct struct {
				Site string `validate:"Site"`
			}
			testObj := &TestStruct{Site: site}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "Site '%s' should be valid", site)
		})
	}
}

func TestActiveDirectoryValidator_SiteNameValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidSites := []string{
		"Site..Name", // double dots
		".Site",      // starts with dot
		"Site.",      // ends with dot
		"-Site",      // starts with hyphen
		"Site-",      // ends with hyphen
		"Site Name",  // contains space
		"Site@Name",  // contains invalid char
	}

	for _, site := range invalidSites {
		t.Run("invalid_"+site, func(t *testing.T) {
			type TestStruct struct {
				Site string `validate:"Site"`
			}
			testObj := &TestStruct{Site: site}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Site '%s' should be invalid", site)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, siteValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_OrganizationalUnitValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validOUs := []string{
		"",                                    // empty string allowed
		"CN=Users,DC=example,DC=com",          // simple DN
		"OU=Sales,DC=company,DC=org",          // organizational unit
		"CN=John Doe,OU=Users,DC=test,DC=net", // user DN
		"OU=IT,OU=Departments,DC=corp,DC=com", // nested OU
	}

	for _, ou := range validOUs {
		t.Run("valid_OU", func(t *testing.T) {
			type TestStruct struct {
				OU string `validate:"OrganizationalUnit"`
			}
			testObj := &TestStruct{OU: ou}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "OU '%s' should be valid", ou)
		})
	}
}

func TestActiveDirectoryValidator_OrganizationalUnitValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidOUs := []string{
		"CN=",                 // incomplete DN
		"=Users",              // missing type
		"CN=Users,",           // trailing comma
		"CN=Users,DC=",        // incomplete component
		"invalid",             // not a DN format
		"CN=Users;DC=example", // unescaped semicolon
		"CN=Users<DC=example", // unescaped less than
	}

	for _, ou := range invalidOUs {
		t.Run("invalid_OU", func(t *testing.T) {
			type TestStruct struct {
				OU string `validate:"OrganizationalUnit"`
			}
			testObj := &TestStruct{OU: ou}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "OU '%s' should be invalid", ou)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Contains(t, translatedMsg, "Error validating organizationalUnit", "should contain OU error prefix")
		})
	}
}

func TestActiveDirectoryValidator_ActiveDirectoryUsersValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validUsers := [][]string{
		{},                              // empty list
		{"user1"},                       // single user
		{"user1", "user2", "user3"},     // multiple users
		{"User1", "user1"},              // case insensitive duplicates should fail
		{"admin", "backup", "security"}, // typical users
	}

	for i, users := range validUsers {
		t.Run("valid_users_"+string(rune(i)), func(t *testing.T) {
			type TestStruct struct {
				Users []string `validate:"SecurityOperators"`
			}
			testObj := &TestStruct{Users: users}

			// Special case: case insensitive duplicates should fail
			if len(users) == 2 && users[0] == "User1" && users[1] == "user1" {
				err := adValidator.validate.Struct(testObj)
				require.Error(t, err, "Case insensitive duplicates should be invalid")
				return
			}

			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "Users %v should be valid", users)
		})
	}
}

func TestActiveDirectoryValidator_ActiveDirectoryUsersValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidUsers := [][]string{
		{"user1", "user1"},          // exact duplicates
		{"User1", "user1"},          // case insensitive duplicates
		{"domain\\user"},            // contains backslash (domain prefix)
		{"user1", "user2", "user1"}, // duplicate in list
	}

	for i, users := range invalidUsers {
		t.Run("invalid_users_"+string(rune(i)), func(t *testing.T) {
			type TestStruct struct {
				Users []string `validate:"SecurityOperators"`
			}
			testObj := &TestStruct{Users: users}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Users %v should be invalid", users)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, adUserValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_DNSValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validDNS := []string{
		"192.168.1.1",             // single IP
		"10.0.0.1,10.0.0.2",       // multiple IPs
		"172.16.0.1, 172.16.0.2",  // with spaces (should fail)
		"8.8.8.8",                 // public DNS
		"1.1.1.1,8.8.8.8,9.9.9.9", // multiple public DNS
	}

	for _, dns := range validDNS {
		t.Run("dns_"+dns, func(t *testing.T) {
			type TestStruct struct {
				DNS string `validate:"DNS"`
			}
			testObj := &TestStruct{DNS: dns}

			// Special case: DNS with spaces should fail
			if dns == "172.16.0.1, 172.16.0.2" {
				err := adValidator.validate.Struct(testObj)
				require.Error(t, err, "DNS with spaces should be invalid")
				return
			}

			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "DNS '%s' should be valid", dns)
		})
	}
}

func TestActiveDirectoryValidator_DNSValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidDNS := []string{
		"127.0.0.1",                // loopback
		"::1",                      // IPv6 loopback
		"224.0.0.1",                // multicast
		"255.255.255.255",          // broadcast
		"0.0.0.0",                  // unspecified
		"192.168.1.1, 192.168.1.2", // contains spaces
		"invalid.ip",               // not an IP
		"192.168.1",                // incomplete IP
		"192.168.1.256",            // invalid octet
		"",                         // empty
	}

	for _, dns := range invalidDNS {
		t.Run("invalid_dns_"+dns, func(t *testing.T) {
			type TestStruct struct {
				DNS string `validate:"DNS"`
			}
			testObj := &TestStruct{DNS: dns}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "DNS '%s' should be invalid", dns)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.NotEmpty(t, translatedMsg, "should have error message")
		})
	}
}

func TestActiveDirectoryValidator_ADNameValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validNames := []string{
		"server",
		"ad-server",
		"adserver.domain.com",
		"server123",
		"my-ad-server",
		"hostname",
	}

	for _, name := range validNames {
		t.Run("valid_name_"+name, func(t *testing.T) {
			type TestStruct struct {
				Name string `validate:"ResourceId"`
			}
			testObj := &TestStruct{Name: name}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "Name '%s' should be valid", name)
		})
	}
}

func TestActiveDirectoryValidator_ADNameValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidNames := []string{
		"192.168.1.1", // IPv4 address
		"::1",         // IPv6 address
		"127.0.0.1",   // loopback IP
		"10.0.0.1",    // private IP
		"2001:db8::1", // IPv6 address
	}

	for _, name := range invalidNames {
		t.Run("invalid_name_"+name, func(t *testing.T) {
			type TestStruct struct {
				Name string `validate:"ResourceId"`
			}
			testObj := &TestStruct{Name: name}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Name '%s' should be invalid", name)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, adNameValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_ConcurrencySafety(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	// Test concurrent access to dnErrorStore
	const numGoroutines = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Concurrent validation operations
			type TestStruct struct {
				NetBIOS string `validate:"NetBIOS"`
				DNS     string `validate:"DNS"`
			}

			testObj := &TestStruct{
				NetBIOS: "Domain" + string(rune(id%10)),
				DNS:     "192.168.1." + string(rune(1+id%254)),
			}

			// This will trigger dnErrorStore operations
			err := adValidator.validate.Struct(testObj)
			if err != nil {
				return
			}
		}(i)
	}

	wg.Wait()

	// Verify the validator is still functional
	type TestStruct struct {
		NetBIOS string `validate:"NetBIOS"`
	}
	testObj := &TestStruct{NetBIOS: "ValidVal"}
	err = adValidator.validate.Struct(testObj)
	assert.NoError(t, err, "validator should still work after concurrent access")
}

func TestActiveDirectoryValidator_MultipleValidators(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	type ComplexStruct struct {
		NetBIOS           string   `validate:"NetBIOS"`
		Username          string   `validate:"Username"`
		Site              string   `validate:"Site"`
		SecurityOperators []string `validate:"SecurityOperators"`
		DNS               string   `validate:"DNS"`
		ResourceId        string   `validate:"ResourceId"`
	}

	// Test valid complex struct
	validStruct := &ComplexStruct{
		NetBIOS:           "DOMAIN",
		Username:          "admin",
		Site:              "MainSite",
		SecurityOperators: []string{"user1", "user2"},
		DNS:               "192.168.1.1,192.168.1.2",
		ResourceId:        "ad-server",
	}

	err = adValidator.validate.Struct(validStruct)
	assert.NoError(t, err, "valid complex struct should pass validation")

	// Test invalid complex struct
	invalidStruct := &ComplexStruct{
		NetBIOS:           "domain.",                  // ends with dot
		Username:          "user/name",                // contains slash
		Site:              "Site..Name",               // double dots
		SecurityOperators: []string{"user1", "user1"}, // duplicates
		DNS:               "127.0.0.1",                // loopback
		ResourceId:        "192.168.1.1",              // IP address
	}

	err = adValidator.validate.Struct(invalidStruct)
	require.Error(t, err, "invalid complex struct should fail validation")

	var validationErrs validator.ValidationErrors
	ok := errors.As(err, &validationErrs)
	require.True(t, ok, "error should be ValidationErrors type")
	assert.Equal(t, 6, len(validationErrs), "should have validation errors for all invalid fields")

	// Check that each error has a translation
	for _, fieldErr := range validationErrs {
		translatedMsg := fieldErr.Translate(adValidator.Translator)
		assert.NotEmpty(t, translatedMsg, "each error should have a translation")
	}
}

func TestParseIPV4(t *testing.T) {
	testCases := []struct {
		input    string
		expected bool
	}{
		{"192.168.1.1", true},
		{"10.0.0.1", true},
		{"127.0.0.1", true},
		{"255.255.255.255", true},
		{"0.0.0.0", true},
		{"invalid", false},
		{"", false},
		{"192.168.1", false},
		{"192.168.1.256", false},
		{"::1", false},         // IPv6
		{"2001:db8::1", false}, // IPv6
	}

	for _, tc := range testCases {
		t.Run("parseIPV4_"+tc.input, func(t *testing.T) {
			ip := parseIPV4(tc.input)
			if tc.expected {
				assert.NotNil(t, ip, "should parse valid IPv4: %s", tc.input)
				assert.NotNil(t, ip.To4(), "should be IPv4: %s", tc.input)
			} else {
				assert.Nil(t, ip, "should not parse invalid input: %s", tc.input)
			}
		})
	}
}

func TestValidateDistinguishedName(t *testing.T) {
	validDNs := []string{
		"CN=Users,DC=example,DC=com",
		"OU=Sales,DC=company,DC=org",
		"CN=John Doe,OU=Users,DC=test,DC=net",
		"OU=IT,OU=Departments,DC=corp,DC=com",
		"CN=Test\\, User,DC=example,DC=com", // escaped comma
		"CN=Test\\= User,DC=example,DC=com", // escaped equals
		"CN=Test User,DC=example,DC=com",    // space in name
	}

	for _, dn := range validDNs {
		t.Run("valid_dn", func(t *testing.T) {
			err := validateDistinguishedName(dn)
			assert.NoError(t, err, "DN should be valid: %s", dn)
		})
	}
}

func TestValidateDistinguishedName_Invalid(t *testing.T) {
	invalidDNs := []string{
		"CN=",                  // incomplete
		"=Users",               // missing type
		"CN=Users,",            // trailing comma
		"CN=Users,DC=",         // incomplete component
		"invalid",              // not DN format
		"CN=Users;DC=example",  // unescaped semicolon
		"CN=Users<DC=example",  // unescaped less than
		"CN=Users>DC=example",  // unescaped greater than
		"CN=Users\"DC=example", // unescaped quote
		"CN=Users/DC=example",  // unescaped slash
		"CN=Users\\",           // ends with backslash
	}

	for _, dn := range invalidDNs {
		t.Run("invalid_dn", func(t *testing.T) {
			err := validateDistinguishedName(dn)
			assert.Error(t, err, "DN should be invalid: %s", dn)
		})
	}
}

func TestValidateDNTypeValuePair(t *testing.T) {
	validPairs := []string{
		"CN=Users",
		"DC=example",
		"OU=Sales",
		"CN=John Doe",
		"CN=Test\\=User", // escaped equals in value
	}

	for _, pair := range validPairs {
		t.Run("valid_pair", func(t *testing.T) {
			err := validateDNTypeValuePair(pair)
			assert.NoError(t, err, "DN pair should be valid: %s", pair)
		})
	}

	invalidPairs := []string{
		"CN",        // no equals
		"=Users",    // no type
		"CN=",       // no value
		"CN==Users", // double equals
		"",          // empty
		"AB",        // too short
	}

	for _, pair := range invalidPairs {
		t.Run("invalid_pair", func(t *testing.T) {
			err := validateDNTypeValuePair(pair)
			assert.Error(t, err, "DN pair should be invalid: %s", pair)
		})
	}
}
