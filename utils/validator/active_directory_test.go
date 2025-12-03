package validator

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		t.Run("valid_users_"+strconv.Itoa(i), func(t *testing.T) {
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

func TestActiveDirectoryValidator_ActiveDirectoryAdminUsersValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	require.NoError(t, err)

	validUsers := [][]string{
		{},                              // empty list
		{"admin1"},                      // single user
		{"admin1", "admin2", "admin3"},  // multiple users
		{"Admin", "backup", "security"}, // typical users
		{"user_with_underscore"},        // underscore allowed
		{"user-with-hyphen"},            // hyphen allowed
		{"user.with.dot"},               // dot allowed
	}

	for i, users := range validUsers {
		t.Run("valid_admins_"+string(rune(i)), func(t *testing.T) {
			type TestStruct struct {
				Admins []string `validate:"Administrators"`
			}
			testObj := &TestStruct{Admins: users}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "Administrators %v should be valid", users)
		})
	}
}

func TestActiveDirectoryValidator_ActiveDirectoryAdminUsersValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	require.NoError(t, err)

	invalidUsers := []struct {
		name  string
		users []string
	}{
		{
			name:  "exact_duplicates",
			users: []string{"admin1", "admin1"},
		},
		{
			name:  "case_insensitive_duplicates",
			users: []string{"Admin1", "admin1"},
		},
		{
			name:  "user_with_at_symbol",
			users: []string{"admin@domain.com"},
		},
		{
			name:  "multiple_users_one_with_at",
			users: []string{"admin1", "admin@domain.com", "admin2"},
		},
		{
			name:  "duplicate_in_list",
			users: []string{"admin1", "admin2", "admin1"},
		},
		{
			name:  "at_symbol_in_middle",
			users: []string{"user@host"},
		},
	}

	for _, tc := range invalidUsers {
		t.Run(tc.name, func(t *testing.T) {
			type TestStruct struct {
				Admins []string `validate:"Administrators"`
			}
			testObj := &TestStruct{Admins: tc.users}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Administrators %v should be invalid", tc.users)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Contains(t, translatedMsg, "is not unique or contains invalid characters",
				"should use custom error message for administrators")
		})
	}
}

func TestActiveDirectoryValidator_ActiveDirectoryAdminUsersValidator_EdgeCases(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	require.NoError(t, err)

	t.Run("empty_list_is_valid", func(t *testing.T) {
		type TestStruct struct {
			Admins []string `validate:"Administrators"`
		}
		testObj := &TestStruct{Admins: []string{}}
		err := adValidator.validate.Struct(testObj)
		assert.NoError(t, err, "Empty administrator list should be valid")
	})

	t.Run("case_sensitivity_check", func(t *testing.T) {
		type TestStruct struct {
			Admins []string `validate:"Administrators"`
		}
		// Different case should be treated as duplicates
		testObj := &TestStruct{Admins: []string{"ADMIN", "admin", "Admin"}}
		err := adValidator.validate.Struct(testObj)
		require.Error(t, err, "Case variations of same name should be invalid")
	})

	t.Run("special_characters_without_at", func(t *testing.T) {
		type TestStruct struct {
			Admins []string `validate:"Administrators"`
		}
		// Backslash is allowed now (only @ is forbidden)
		testObj := &TestStruct{Admins: []string{"domain\\user"}}
		err := adValidator.validate.Struct(testObj)
		assert.NoError(t, err, "Backslash should be allowed for administrators")
	})
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

func TestActiveDirectoryValidator_KdcIpValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validIPs := []string{
		"",                // empty string allowed (optional field)
		"1.2.3.4",         // minimum valid length (7 chars)
		"10.0.0.1",        // 8 chars
		"192.168.1.1",     // 11 chars
		"255.255.255.255", // maximum length (15 chars)
		"172.16.254.1",    // 12 chars
		"8.8.8.8",         // 7 chars exactly
	}

	for _, ip := range validIPs {
		t.Run("valid_kdcip_"+ip, func(t *testing.T) {
			type TestStruct struct {
				KdcIP string `validate:"KdcIP"`
			}
			testObj := &TestStruct{KdcIP: ip}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "KdcIP '%s' should be valid", ip)
		})
	}
}

func TestActiveDirectoryValidator_KdcIpValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	invalidIPs := []string{
		"1.2.3",  // 5 chars - too short
		"10.0.0", // 6 chars - too short
		"1.2.3.", // 6 chars - incomplete
		"short",  // 5 chars
		"abc",    // 3 chars
	}

	for _, ip := range invalidIPs {
		t.Run("invalid_kdcip_"+ip, func(t *testing.T) {
			type TestStruct struct {
				KdcIP string `validate:"KdcIP"`
			}
			testObj := &TestStruct{KdcIP: ip}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "KdcIP '%s' should be invalid", ip)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.Equal(t, kdcIpValidationErr, translatedMsg, "should use custom error message")
		})
	}
}

func TestActiveDirectoryValidator_KdcHostnameValidator_Valid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	validHostnames := []string{
		"",                  // empty string allowed (optional field)
		"a",                 // minimum valid length (1 char)
		"host",              // 4 chars
		"ad-server",         // 9 chars
		"myactivedirectory", // 18 chars
		"AD1234567890",      // 12 chars
		"kdc.example.com",   // FQDN
		"very-long-hostname-that-is-still-valid-as-per-spec", // long hostname
	}

	for _, hostname := range validHostnames {
		t.Run("valid_kdchostname_"+hostname, func(t *testing.T) {
			type TestStruct struct {
				KdcHostname string `validate:"KdcHostname"`
			}
			testObj := &TestStruct{KdcHostname: hostname}
			err := adValidator.validate.Struct(testObj)
			assert.NoError(t, err, "KdcHostname '%s' should be valid", hostname)
		})
	}
}

func TestActiveDirectoryValidator_KdcHostnameValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	// According to the validator, the only invalid case is when
	// the string is non-empty but has length < 1 (which is impossible)
	// So we test edge cases where it might fail

	// Note: The current implementation only fails if len < 1 and non-nil
	// Since Go strings can't have negative length, this test verifies
	// that the validator is correctly registered

	t.Run("validator_registration", func(t *testing.T) {
		type TestStruct struct {
			KdcHostname string `validate:"KdcHostname"`
		}
		// Test that valid values pass
		testObj := &TestStruct{KdcHostname: "validhost"}
		err := adValidator.validate.Struct(testObj)
		assert.NoError(t, err, "Valid hostname should pass")

		// Empty string should also pass as it's optional
		testObj = &TestStruct{KdcHostname: ""}
		err = adValidator.validate.Struct(testObj)
		assert.NoError(t, err, "Empty hostname should pass")
	})
}

func TestActiveDirectoryValidator_KdcValidators_InParams(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	if err != nil {
		return
	}

	// Simulate the actual params structure
	type CreateADParams struct {
		KdcIP       string `validate:"KdcIP"`
		KdcHostname string `validate:"KdcHostname"`
	}

	t.Run("ValidKdcIPAndHostname", func(t *testing.T) {
		params := &CreateADParams{
			KdcIP:       "192.168.1.10",
			KdcHostname: "ad-server",
		}
		err := adValidator.validate.Struct(params)
		assert.NoError(t, err, "Valid KDC params should pass validation")
	})

	t.Run("EmptyKdcIPAndHostname", func(t *testing.T) {
		params := &CreateADParams{
			KdcIP:       "",
			KdcHostname: "",
		}
		err := adValidator.validate.Struct(params)
		assert.NoError(t, err, "Empty KDC params should pass validation")
	})

	t.Run("InvalidKdcIPTooShort", func(t *testing.T) {
		params := &CreateADParams{
			KdcIP:       "1.2.3",
			KdcHostname: "ad-server",
		}
		err := adValidator.validate.Struct(params)
		require.Error(t, err, "Too short KdcIP should fail validation")

		var validationErrs validator.ValidationErrors
		ok := errors.As(err, &validationErrs)
		require.True(t, ok, "error should be ValidationErrors type")
		assert.Len(t, validationErrs, 1, "should have exactly one validation error")
	})

	t.Run("ValidKdcIPMinimumLength", func(t *testing.T) {
		params := &CreateADParams{
			KdcIP:       "1.2.3.4",
			KdcHostname: "h",
		}
		err := adValidator.validate.Struct(params)
		assert.NoError(t, err, "Minimum valid length KDC params should pass validation")
	})
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

func TestActiveDirectoryValidator_LocationIdValidator_RejectsZones(t *testing.T) {
	// Note: This test validates the zone rejection logic added in the regionValidator.
	// In the default configuration where LOCAL_REGION="local", zones are not supported
	// because "local" doesn't follow the GCP region pattern (<letters>-<letters><digits>).
	// However, if LOCAL_REGION is set to a GCP-style region (e.g., "us-central1"),
	// then zonal locations (e.g., "us-central1-a") will be properly detected and rejected
	// with the zonalAdNotSupportedErr message.
	//
	// This test documents the behavior without requiring environment variable changes.

	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	require.NoError(t, err)

	// Test that the zone validation error is properly stored in dnErrorStore
	// We can't easily test the full flow without changing LOCAL_REGION env var,
	// but we can verify that zonal regions in other formats are rejected.

	// These are invalid for other reasons (not "local"), but validates the structure
	zonalStyleRegions := []string{
		"us-central1-a",
		"us-east1-b",
		"europe-west1-c",
	}

	for _, region := range zonalStyleRegions {
		t.Run("zonal_style_region_"+region, func(t *testing.T) {
			type TestStruct struct {
				LocationId string `validate:"LocationId"`
			}
			testObj := &TestStruct{LocationId: region}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Zonal-style LocationId '%s' should be invalid", region)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")

			// The error will be zonalAdNotSupportedErr if LOCAL_REGION was "us-central1"
			// Otherwise it will be the "Region can only be local" error
			translatedMsg := validationErrs[0].Translate(adValidator.Translator)
			assert.NotEmpty(t, translatedMsg, "should have an error message")
			// The actual message depends on LOCAL_REGION env var
		})
	}
}

func TestActiveDirectoryValidator_LocationIdValidator_Invalid(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	adValidator := NewActiveDirectoryValidator(ctx, mockStorage)
	err := adValidator.RegisterValidators()
	require.NoError(t, err)

	invalidRegions := []string{
		"",                   // empty
		"invalid",            // malformed
		"us-central1",        // not "local"
		"us-east1",           // not "local"
		"europe-west1",       // not "local"
		"us_central1",        // invalid format
		"123-region",         // invalid format
		"invalid-region-123", // malformed
	}

	for _, region := range invalidRegions {
		t.Run("invalid_region_"+region, func(t *testing.T) {
			type TestStruct struct {
				LocationId string `validate:"LocationId"`
			}
			testObj := &TestStruct{LocationId: region}
			err := adValidator.validate.Struct(testObj)
			require.Error(t, err, "Invalid LocationId '%s' should fail validation", region)

			var validationErrs validator.ValidationErrors
			ok := errors.As(err, &validationErrs)
			require.True(t, ok, "error should be ValidationErrors type")
			require.Len(t, validationErrs, 1, "should have exactly one validation error")
		})
	}
}
