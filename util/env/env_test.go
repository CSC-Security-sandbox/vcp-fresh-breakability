package env

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasEnv(t *testing.T) {
	key := "ENVUTIL_TEST_HAS_ENV"
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if IsEnvSet(key) {
			tt.Fail()
		}
	})
	t.Run("WhenEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		if !IsEnvSet(key) {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetStringFromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_STRING"
	def := "default value"
	t.Run("WhenNotSpecifyingStringAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetStringFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifyingStringAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetStringFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != "non-default value" {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifyingStringAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		v := "valuable"
		val := GetStringFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingStringAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		v := "valuable"
		val := GetStringFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetString(t *testing.T) {
	key := "ENVUTIL_TEST_GET_STRING"
	def := "default value"
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetString(key, def) != def {
			tt.Fail()
		}
	})
	t.Run("WhenEnvironmentVariableIsEmpty", func(tt *testing.T) {
		err := os.Setenv(key, "")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		if GetString(key, def) != "" {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenEnvironmentVariableIsNotEmpty", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		if GetString(key, def) != "non-default value" {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetStrings(t *testing.T) {
	env1 := "ENV_VAR_1"
	env2 := "ENV_VAR_2"
	env3 := "ENV_var_3_no_match"
	env4 := "ENV_no_match_VAR_4"
	env5 := "prefix_ENV_VAR_5"
	env6 := "ENV_VAR_6"
	defer func() {
		_ = os.Unsetenv(env1)
		_ = os.Unsetenv(env2)
		_ = os.Unsetenv(env3)
		_ = os.Unsetenv(env4)
		_ = os.Unsetenv(env5)
		_ = os.Unsetenv(env6)
	}()
	_ = os.Setenv(env1, "someValue1")
	_ = os.Setenv(env2, "someValue2")
	_ = os.Setenv(env3, "someValue3")
	_ = os.Setenv(env4, "someValue4")
	_ = os.Setenv(env5, "someValue5")
	_ = os.Setenv(env6, "someValue6")
	t.Run("WhenInvalidRegex", func(tt *testing.T) {
		vars, err := GetStrings("BrokenRegex((¢}^2")
		assert.EqualError(tt, err, "error parsing regexp: missing closing ): `BrokenRegex((¢}^2`")
		assert.Empty(tt, vars)
	})
	t.Run("WhenNoEnvironmentVariableMatches", func(tt *testing.T) {
		vars, err := GetStrings("ThisShouldNotMatch")
		assert.NoError(tt, err)
		assert.Empty(tt, vars)
	})
	t.Run("WhenEnvironmentVariablesMatch", func(tt *testing.T) {
		vars, err := GetStrings("^ENV_VAR.*")
		assert.NoError(tt, err)
		assert.Len(tt, vars, 3)
		assert.Equal(tt, "someValue1", vars[env1])
		assert.Equal(tt, "someValue2", vars[env2])
		assert.Equal(tt, "someValue6", vars[env6])
	})
}

func TestGetIntFromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_INT"
	def := 9999
	t.Run("WhenNotSpecifyingIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetIntFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifyingIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "1234")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetIntFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != 1234 {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifyingIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		v := 1234
		val := GetIntFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		v := 1234
		val := GetIntFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetInt(t *testing.T) {
	key := "ENVUTIL_TEST_GET_INT"
	def := 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetInt(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env         string
		expectedInt int
	}
	testData := []data{
		{env: "", expectedInt: def},
		{env: "zero", expectedInt: def},
		{env: "-2147483648", expectedInt: -2147483648},
		{env: "-13", expectedInt: -13},
		{env: "-1", expectedInt: -1},
		{env: "0", expectedInt: 0},
		{env: "1", expectedInt: 1},
		{env: "13", expectedInt: 13},
		{env: "2147483647", expectedInt: 2147483647},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetInt(key, def)
		if val != td.expectedInt {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetUintFromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_UINT"
	var def uint = 9999
	t.Run("WhenNotSpecifyingUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetUintFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifyingUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "1234")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetUintFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != 1234 {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifyingUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		var v uint = 1234
		val := GetUintFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		var v uint = 1234
		val := GetUintFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetUint(t *testing.T) {
	key := "ENVUTIL_TEST_GET_UINT"
	var def uint = 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetUint(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env          string
		expectedUint uint
	}
	testData := []data{
		{env: "", expectedUint: def},
		{env: "zero", expectedUint: def},
		{env: "-2147483648", expectedUint: def},
		{env: "-13", expectedUint: def},
		{env: "-1", expectedUint: def},
		{env: "0", expectedUint: 0},
		{env: "1", expectedUint: 1},
		{env: "13", expectedUint: 13},
		{env: "4294967295", expectedUint: 4294967295},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetUint(key, def)
		if val != td.expectedUint {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetIntNotNegative(t *testing.T) {
	key := "ENVUTIL_TEST_GET_INT"
	var def = 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetIntNotNegative(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env         string
		expectedInt int
	}
	testData := []data{
		{env: "", expectedInt: def},
		{env: "zero", expectedInt: def},
		{env: "-2147483648", expectedInt: def},
		{env: "-13", expectedInt: def},
		{env: "-1", expectedInt: def},
		{env: "0", expectedInt: 0},
		{env: "1", expectedInt: 1},
		{env: "13", expectedInt: 13},
		{env: "4294967295", expectedInt: 4294967295},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetIntNotNegative(key, def)
		if val != td.expectedInt {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetUint16(t *testing.T) {
	key := "ENVUTIL_TEST_GET_UINT16"
	var def uint16 = 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetUint16(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env            string
		expectedUint16 uint16
	}
	testData := []data{
		{env: "", expectedUint16: def},
		{env: "zero", expectedUint16: def},
		{env: "-2147483648", expectedUint16: def},
		{env: "-13", expectedUint16: def},
		{env: "-1", expectedUint16: def},
		{env: "0", expectedUint16: 0},
		{env: "1", expectedUint16: 1},
		{env: "13", expectedUint16: 13},
		{env: "4294967295", expectedUint16: def},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetUint16(key, def)
		if val != td.expectedUint16 {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetInt64FromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_INT64"
	var def int64 = 9999
	t.Run("WhenNotSpecifying64bitIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetInt64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifying64bitIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "1234")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetInt64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != 1234 {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifying64bitIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		var v int64 = 1234
		val := GetInt64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifying64bitIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		var v int64 = 1234
		val := GetInt64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetInt64(t *testing.T) {
	key := "ENVUTIL_TEST_GET_INT64"
	var def int64 = 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetInt64(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env           string
		expectedInt64 int64
	}
	testData := []data{
		{env: "", expectedInt64: def},
		{env: "zero", expectedInt64: def},
		{env: "-2147483648", expectedInt64: -2147483648},
		{env: "-13", expectedInt64: -13},
		{env: "-1", expectedInt64: -1},
		{env: "0", expectedInt64: 0},
		{env: "1", expectedInt64: 1},
		{env: "13", expectedInt64: 13},
		{env: "2147483647", expectedInt64: 2147483647},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetInt64(key, def)
		if val != td.expectedInt64 {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetUint64FromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_UINT64"
	var def uint64 = 9999
	t.Run("WhenNotSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetUint64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "1234")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetUint64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != 1234 {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		var v uint64 = 1234
		val := GetUint64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		var v uint64 = 1234
		val := GetUint64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetUint64(t *testing.T) {
	key := "ENVUTIL_TEST_GET_UINT64"
	var def uint64 = 9999
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetUint64(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env            string
		expectedUint64 uint64
	}
	testData := []data{
		{env: "", expectedUint64: def},
		{env: "zero", expectedUint64: def},
		{env: "-2147483648", expectedUint64: def},
		{env: "-13", expectedUint64: def},
		{env: "-1", expectedUint64: def},
		{env: "0", expectedUint64: 0},
		{env: "1", expectedUint64: 1},
		{env: "13", expectedUint64: 13},
		{env: "4294967295", expectedUint64: 4294967295},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetUint64(key, def)
		if val != td.expectedUint64 {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetFloat64FromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_FLOAT64"
	def := 99.99
	t.Run("WhenNotSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetFloat64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "12.34")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetFloat64FromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != 12.34 {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		v := 12.34
		val := GetFloat64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifying64bitUnsignedIntegerAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		v := 12.34
		val := GetFloat64FromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetFloat64(t *testing.T) {
	key := "ENVUTIL_TEST_GET_FLOAT64"
	def := 99.99
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetFloat64(key, def) != def {
			tt.Fail()
		}
	})

	type data struct {
		env             string
		expectedFloat64 float64
	}
	testData := []data{
		{env: "", expectedFloat64: def},
		{env: "zero", expectedFloat64: def},
		{env: "-21474.83648", expectedFloat64: -21474.83648},
		{env: "-1.3", expectedFloat64: -1.3},
		{env: "-1", expectedFloat64: -1},
		{env: "0", expectedFloat64: 0},
		{env: "1", expectedFloat64: 1},
		{env: "1.3", expectedFloat64: 1.3},
		{env: "42949.67295", expectedFloat64: 42949.67295},
	}
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetFloat64(key, def)
		if val != td.expectedFloat64 {
			t.Errorf("Returned value %v does not match expected one for env '%s'", val, td.env)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}

func TestGetBoolFromEnvIfNil(t *testing.T) {
	key := "ENVUTIL_TEST_GET_BOOL"
	def := true
	t.Run("WhenNotSpecifyingBoolAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		val := GetBoolFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != def {
			tt.Fail()
		}
	})
	t.Run("WhenNotSpecifyingBoolAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "false")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetBoolFromEnvIfNil(nil, key, def)
		if val == nil {
			tt.Error("Value not returned")
		} else if *val != false {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
	t.Run("WhenSpecifyingBoolAndEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		v := false
		val := GetBoolFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
	})
	t.Run("WhenSpecifyingBoolAndEnvironmentVariableIsSet", func(tt *testing.T) {
		err := os.Setenv(key, "non-default value")
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		v := false
		val := GetBoolFromEnvIfNil(&v, key, def)
		if val != &v {
			tt.Fail()
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	})
}

func TestGetBool(t *testing.T) {
	key := "ENVUTIL_TEST_GET_BOOL"

	// Test when the environment variable is not set
	t.Run("WhenEnvironmentVariableIsNotSet", func(tt *testing.T) {
		err := os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
		if GetBool(key, false) {
			tt.Error("Expected false when environment variable is not set and default is false")
		}
		if !GetBool(key, true) {
			tt.Error("Expected true when environment variable is not set and default is true")
		}
	})

	// Define test data
	type data struct {
		env                     string
		expectedForFalseDefault bool
		expectedForTrueDefault  bool
	}

	testData := []data{
		// Empty or invalid values should return the default
		{env: "", expectedForFalseDefault: false, expectedForTrueDefault: true},
		{env: "maybe", expectedForFalseDefault: false, expectedForTrueDefault: true},
		{env: "truetru", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "yesyes", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "falsefals", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "nono", expectedForFalseDefault: false, expectedForTrueDefault: false},

		// Numeric values
		{env: "-6161616161", expectedForFalseDefault: true, expectedForTrueDefault: true}, // Non-zero is true
		{env: "-13", expectedForFalseDefault: true, expectedForTrueDefault: true},         // Non-zero is true
		{env: "-1", expectedForFalseDefault: true, expectedForTrueDefault: true},          // Non-zero is true
		{env: "0", expectedForFalseDefault: false, expectedForTrueDefault: false},         // Zero is false
		{env: "1", expectedForFalseDefault: true, expectedForTrueDefault: true},           // Non-zero is true
		{env: "13", expectedForFalseDefault: true, expectedForTrueDefault: true},          // Non-zero is true
		{env: "6161616161", expectedForFalseDefault: true, expectedForTrueDefault: true},  // Non-zero is true

		// True values (case-insensitive)
		{env: "true", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "True", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "TRUE", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "tRuE", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "yes", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "Yes", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "YES", expectedForFalseDefault: true, expectedForTrueDefault: true},
		{env: "yEs", expectedForFalseDefault: true, expectedForTrueDefault: true},

		// False values (case-insensitive)
		{env: "false", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "False", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "FALSE", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "fAlSe", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "no", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "No", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "NO", expectedForFalseDefault: false, expectedForTrueDefault: false},
		{env: "nO", expectedForFalseDefault: false, expectedForTrueDefault: false},
	}

	// Run tests for each case
	for _, td := range testData {
		err := os.Setenv(key, td.env)
		if err != nil {
			t.Errorf("Error setting environment variable %s: %v", key, err)
		}
		val := GetBool(key, false)
		if val != td.expectedForFalseDefault {
			t.Errorf("For env '%s' with default false: expected %v, got %v", td.env, td.expectedForFalseDefault, val)
		}
		val = GetBool(key, true)
		if val != td.expectedForTrueDefault {
			t.Errorf("For env '%s' with default true: expected %v, got %v", td.env, td.expectedForTrueDefault, val)
		}
		err = os.Unsetenv(key)
		if err != nil {
			t.Errorf("Error unsetting environment variable %s: %v", key, err)
		}
	}
}
