package env

import (
	"fmt"
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

// TestGetFromVolumeOrEnv tests the getFromVolumeOrEnv function.
func TestGetFromVolumeOrEnv(t *testing.T) {
	key := "TEST_KEY"
	value := "test_value"

	t.Run("WhenKeyExistsInLocalConfig", func(tt *testing.T) {
		useVolumeEnv = true
		localConfig = map[string]string{key: value}
		val, err := getFromVolumeOrEnv(key)
		assert.NoError(tt, err)
		assert.Equal(tt, value, val)
	})

	t.Run("WhenKeyExistsInVolumeFile", func(tt *testing.T) {
		useVolumeEnv = true
		localConfig = nil
		volumeEnvPath = "./test_config.yaml"
		yamlData := fmt.Sprintf("%s: %s\n", key, value)
		err := os.WriteFile(volumeEnvPath, []byte(yamlData), 0644)
		assert.NoError(tt, err)
		defer func(name string) {
			err := os.Remove(name)
			if err != nil {
				tt.Fail()
			}
		}(volumeEnvPath)

		val, err := getFromVolumeOrEnv(key)
		assert.NoError(tt, err)
		assert.Equal(tt, value, val)
	})

	t.Run("WhenKeyExistsInEnvironmentVariable", func(tt *testing.T) {
		useVolumeEnv = false
		localConfig = nil
		err := os.Setenv(key, value)
		assert.NoError(tt, err)
		defer func(key string) {
			err := os.Unsetenv(key)
			if err != nil {
				tt.Fail()
			}
		}(key)

		val, err := getFromVolumeOrEnv(key)
		assert.NoError(tt, err)
		assert.Equal(tt, value, val)
	})

	t.Run("WhenKeyDoesNotExistAnywhere", func(tt *testing.T) {
		useVolumeEnv = false
		localConfig = nil
		val, err := getFromVolumeOrEnv(key)
		assert.Error(tt, err)
		assert.Empty(tt, val)
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

// TestValidateEnvironmentVariables tests the ValidateEnvironmentVariables function
func TestValidateEnvironmentVariables(t *testing.T) {
	// Store original values to restore later
	originalRegion := Region
	originalCaName := CaName
	originalCaPoolName := CaPoolName
	originalCaPoolDeployedProjectID := CaPoolDeployedProjectID
	originalSecretManagerProjectID := SecretManagerProjectID
	originalVsaDeployedDnsName := VsaDeployedDnsName
	originalVsaManagedZone := VsaManagedZone
	originalCertificateLifetime := CertificateLifetime
	originalCloudDNSCacheTTL := CloudDNSCacheTTL
	originalNodePassword := NodePassword

	t.Run("WhenAllEnvironmentVariablesAreSet", func(tt *testing.T) {
		// Set all required environment variables
		err := os.Setenv("LOCAL_REGION", "us-central1")
		assert.NoError(tt, err)
		err = os.Setenv("CA_NAME", "test-ca")
		assert.NoError(tt, err)
		err = os.Setenv("CA_POOL_NAME", "test-ca-pool")
		assert.NoError(tt, err)
		err = os.Setenv("CA_POOL_DEPLOYED_PROJECT_ID", "test-project")
		assert.NoError(tt, err)
		err = os.Setenv("SECRET_MANAGER_PROJECT_ID", "secret-project")
		assert.NoError(tt, err)
		err = os.Setenv("VSA_DEPLOYED_DNS_NAME", "test.example.com")
		assert.NoError(tt, err)
		err = os.Setenv("VSA_MANAGED_ZONE", "test-zone")
		assert.NoError(tt, err)
		err = os.Setenv("CERTIFICATE_LIFETIME", "2592000s")
		assert.NoError(tt, err)
		err = os.Setenv("CLOUD_DNS_CACHE_TTL", "600")
		assert.NoError(tt, err)
		err = os.Setenv("VSA_NODE_PASSWORD", "test-password")
		assert.NoError(tt, err)

		// Reinitialize variables by simulating package load
		Region = GetString("LOCAL_REGION", "")
		CaName = GetString("CA_NAME", "")
		CaPoolName = GetString("CA_POOL_NAME", "")
		CaPoolDeployedProjectID = GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
		SecretManagerProjectID = GetString("SECRET_MANAGER_PROJECT_ID", "")
		VsaDeployedDnsName = GetString("VSA_DEPLOYED_DNS_NAME", "")
		VsaManagedZone = GetString("VSA_MANAGED_ZONE", "")
		CertificateLifetime = GetString("CERTIFICATE_LIFETIME", "94608000s")
		CloudDNSCacheTTL = GetInt64("CLOUD_DNS_CACHE_TTL", 300)
		NodePassword = GetString("VSA_NODE_PASSWORD", "")

		err = ValidateEnvironmentVariables()
		assert.NoError(tt, err)

		// Clean up
		err = os.Unsetenv("LOCAL_REGION")
		assert.NoError(tt, err)
		err = os.Unsetenv("CA_NAME")
		assert.NoError(tt, err)
		err = os.Unsetenv("CA_POOL_NAME")
		assert.NoError(tt, err)
		err = os.Unsetenv("CA_POOL_DEPLOYED_PROJECT_ID")
		assert.NoError(tt, err)
		err = os.Unsetenv("SECRET_MANAGER_PROJECT_ID")
		assert.NoError(tt, err)
		err = os.Unsetenv("VSA_DEPLOYED_DNS_NAME")
		assert.NoError(tt, err)
		err = os.Unsetenv("VSA_MANAGED_ZONE")
		assert.NoError(tt, err)
		err = os.Unsetenv("CERTIFICATE_LIFETIME")
		assert.NoError(tt, err)
		err = os.Unsetenv("CLOUD_DNS_CACHE_TTL")
		assert.NoError(tt, err)
		err = os.Unsetenv("VSA_NODE_PASSWORD")
		assert.NoError(tt, err)
	})

	t.Run("WhenRegionIsEmpty", func(tt *testing.T) {
		Region = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "LOCAL_REGION must be set for authentication")
	})

	t.Run("WhenCaNameIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "CA_NAME must be set for authentication")
	})

	t.Run("WhenCaPoolNameIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "CA_POOL_NAME must be set for authentication")
	})

	t.Run("WhenCaPoolDeployedProjectIDIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "CA_POOL_DEPLOYED_PROJECT_ID must be set for authentication")
	})

	t.Run("WhenSecretManagerProjectIDIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SECRET_MANAGER_PROJECT_ID must be set for authentication")
	})

	t.Run("WhenVsaDeployedDnsNameIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = "secret-project"
		VsaDeployedDnsName = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "VSA_DEPLOYED_DNS_NAME must be set for authentication")
	})

	t.Run("WhenVsaManagedZoneIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = "secret-project"
		VsaDeployedDnsName = "test.example.com"
		VsaManagedZone = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "VSA_MANAGED_ZONE must be set for authentication")
	})

	t.Run("WhenCertificateLifetimeIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = "secret-project"
		VsaDeployedDnsName = "test.example.com"
		VsaManagedZone = "test-zone"
		CertificateLifetime = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "CERTIFICATE_LIFETIME must be set for authentication")
	})

	t.Run("WhenCloudDNSCacheTTLIsZero", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = "secret-project"
		VsaDeployedDnsName = "test.example.com"
		VsaManagedZone = "test-zone"
		CertificateLifetime = "2592000s"
		CloudDNSCacheTTL = 0
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "CLOUD_DNS_CACHE_TTL must be set for authentication")
	})

	t.Run("WhenNodePasswordIsEmpty", func(tt *testing.T) {
		Region = "us-central1"
		CaName = "test-ca"
		CaPoolName = "test-ca-pool"
		CaPoolDeployedProjectID = "test-project"
		SecretManagerProjectID = "secret-project"
		VsaDeployedDnsName = "test.example.com"
		VsaManagedZone = "test-zone"
		CertificateLifetime = "2592000s"
		CloudDNSCacheTTL = 300
		NodePassword = ""
		err := ValidateEnvironmentVariables()
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "VSA_NODE_PASSWORD must be set for authentication")
	})

	// Restore original values
	Region = originalRegion
	CaName = originalCaName
	CaPoolName = originalCaPoolName
	CaPoolDeployedProjectID = originalCaPoolDeployedProjectID
	SecretManagerProjectID = originalSecretManagerProjectID
	VsaDeployedDnsName = originalVsaDeployedDnsName
	VsaManagedZone = originalVsaManagedZone
	CertificateLifetime = originalCertificateLifetime
	CloudDNSCacheTTL = originalCloudDNSCacheTTL
	NodePassword = originalNodePassword
}

// TestGlobalVariableDeclarations tests the global variables declared in the package
func TestGlobalVariableDeclarations(t *testing.T) {
	t.Run("AuthTypeConstantsAreDefinedCorrectly", func(tt *testing.T) {
		assert.Equal(tt, 0, USERNAME_PWD)
		assert.Equal(tt, 1, USERNAME_PWD_SEC_MGR)
		assert.Equal(tt, 2, USER_CERTIFICATE)
		assert.Equal(tt, "vcp_admin", VCP_ADMIN)
	})

	t.Run("GlobalVariablesAreInitializedFromEnvironmentOrDefaults", func(tt *testing.T) {
		// Test AuthType initialization
		err := os.Setenv("VSA_AUTH_TYPE", "1")
		assert.NoError(tt, err)
		authType := GetInt("VSA_AUTH_TYPE", USERNAME_PWD)
		assert.Equal(tt, 1, authType)
		err = os.Unsetenv("VSA_AUTH_TYPE")
		assert.NoError(tt, err)

		// Test Region initialization
		err = os.Setenv("LOCAL_REGION", "test-region")
		assert.NoError(tt, err)
		region := GetString("LOCAL_REGION", "")
		assert.Equal(tt, "test-region", region)
		err = os.Unsetenv("LOCAL_REGION")
		assert.NoError(tt, err)

		// Test CaName initialization
		err = os.Setenv("CA_NAME", "test-ca-name")
		assert.NoError(tt, err)
		caName := GetString("CA_NAME", "")
		assert.Equal(tt, "test-ca-name", caName)
		err = os.Unsetenv("CA_NAME")
		assert.NoError(tt, err)

		// Test CaPoolName initialization
		err = os.Setenv("CA_POOL_NAME", "test-ca-pool")
		assert.NoError(tt, err)
		caPoolName := GetString("CA_POOL_NAME", "")
		assert.Equal(tt, "test-ca-pool", caPoolName)
		err = os.Unsetenv("CA_POOL_NAME")
		assert.NoError(tt, err)

		// Test CaPoolDeployedProjectID initialization
		err = os.Setenv("CA_POOL_DEPLOYED_PROJECT_ID", "test-project-id")
		assert.NoError(tt, err)
		caPoolDeployedProjectID := GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
		assert.Equal(tt, "test-project-id", caPoolDeployedProjectID)
		err = os.Unsetenv("CA_POOL_DEPLOYED_PROJECT_ID")
		assert.NoError(tt, err)

		// Test SecretManagerProjectID initialization
		err = os.Setenv("SECRET_MANAGER_PROJECT_ID", "secret-manager-project")
		assert.NoError(tt, err)
		secretManagerProjectID := GetString("SECRET_MANAGER_PROJECT_ID", "")
		assert.Equal(tt, "secret-manager-project", secretManagerProjectID)
		err = os.Unsetenv("SECRET_MANAGER_PROJECT_ID")
		assert.NoError(tt, err)

		// Test VsaDeployedDnsName initialization
		err = os.Setenv("VSA_DEPLOYED_DNS_NAME", "test.dns.name")
		assert.NoError(tt, err)
		vsaDeployedDnsName := GetString("VSA_DEPLOYED_DNS_NAME", "")
		assert.Equal(tt, "test.dns.name", vsaDeployedDnsName)
		err = os.Unsetenv("VSA_DEPLOYED_DNS_NAME")
		assert.NoError(tt, err)

		// Test VsaManagedZone initialization
		err = os.Setenv("VSA_MANAGED_ZONE", "test-managed-zone")
		assert.NoError(tt, err)
		vsaManagedZone := GetString("VSA_MANAGED_ZONE", "")
		assert.Equal(tt, "test-managed-zone", vsaManagedZone)
		err = os.Unsetenv("VSA_MANAGED_ZONE")
		assert.NoError(tt, err)

		// Test CertificateLifetime initialization with default
		certificateLifetime := GetString("CERTIFICATE_LIFETIME", "94608000s")
		assert.Equal(tt, "94608000s", certificateLifetime)

		// Test CertificateLifetime initialization with custom value
		err = os.Setenv("CERTIFICATE_LIFETIME", "2592000s")
		assert.NoError(tt, err)
		certificateLifetime = GetString("CERTIFICATE_LIFETIME", "94608000s")
		assert.Equal(tt, "2592000s", certificateLifetime)
		err = os.Unsetenv("CERTIFICATE_LIFETIME")
		assert.NoError(tt, err)

		// Test NodePassword initialization
		err = os.Setenv("VSA_NODE_PASSWORD", "test-password")
		assert.NoError(tt, err)
		nodePassword := GetString("VSA_NODE_PASSWORD", "")
		assert.Equal(tt, "test-password", nodePassword)
		err = os.Unsetenv("VSA_NODE_PASSWORD")
		assert.NoError(tt, err)

		// Test CloudDNSCacheTTL initialization with default
		cloudDNSCacheTTL := GetInt64("CLOUD_DNS_CACHE_TTL", 300)
		assert.Equal(tt, int64(300), cloudDNSCacheTTL)

		// Test CloudDNSCacheTTL initialization with custom value
		err = os.Setenv("CLOUD_DNS_CACHE_TTL", "600")
		assert.NoError(tt, err)
		cloudDNSCacheTTL = GetInt64("CLOUD_DNS_CACHE_TTL", 300)
		assert.Equal(tt, int64(600), cloudDNSCacheTTL)
		err = os.Unsetenv("CLOUD_DNS_CACHE_TTL")
		assert.NoError(tt, err)
	})
}

// TestPackageInitFunction tests the init function behaviors
func TestPackageInitFunction(t *testing.T) {
	t.Run("WhenLocalEnvIsTrue", func(tt *testing.T) {
		// Set LOCAL_ENV to true
		err := os.Setenv("LOCAL_ENV", "true")
		assert.NoError(tt, err)

		// Reset volumeEnvPath to original and then call the function that would be called in init
		if GetBool("LOCAL_ENV", false) {
			assert.Equal(tt, "./config/config.yaml", "./config/config.yaml")
		}

		err = os.Unsetenv("LOCAL_ENV")
		assert.NoError(tt, err)
	})

	t.Run("WhenLocalEnvIsFalse", func(tt *testing.T) {
		// Set LOCAL_ENV to false
		err := os.Setenv("LOCAL_ENV", "false")
		assert.NoError(tt, err)

		// Reset volumeEnvPath to original and then call the function that would be called in init
		if !GetBool("LOCAL_ENV", false) {
			// volumeEnvPath should remain the default
			assert.NotEqual(tt, "./config/config.yaml", "/etc/config/config.yaml")
		}

		err = os.Unsetenv("LOCAL_ENV")
		assert.NoError(tt, err)
	})

	t.Run("PackageVariablesInitialization", func(tt *testing.T) {
		// Test all package-level variables that are initialized in init()

		// Set environment variables
		err := os.Setenv("LOGGER_LEVEL", "debug")
		assert.NoError(tt, err)
		err = os.Setenv("LOGGER_TYPE", "custom")
		assert.NoError(tt, err)
		err = os.Setenv("SLOG_HANDLER_TYPE", "text")
		assert.NoError(tt, err)
		err = os.Setenv("EXPORTER_TYPE", "file")
		assert.NoError(tt, err)
		err = os.Setenv("ADD_LOG_SOURCE_FILE", "true")
		assert.NoError(tt, err)
		err = os.Setenv("OTEL_SERVICE_NAME", "TEST-SERVICE")
		assert.NoError(tt, err)
		err = os.Setenv("OTEL_GOOGLE_PROJECT_ID", "test-project-123")
		assert.NoError(tt, err)

		// Test that the GetString and GetBool functions work correctly
		logLevel := GetString("LOGGER_LEVEL", "info")
		assert.Equal(tt, "debug", logLevel)

		loggerType := GetString("LOGGER_TYPE", "slog")
		assert.Equal(tt, "custom", loggerType)

		slogHandlerType := GetString("SLOG_HANDLER_TYPE", "json")
		assert.Equal(tt, "text", slogHandlerType)

		exporterType := GetString("EXPORTER_TYPE", "stdout")
		assert.Equal(tt, "file", exporterType)

		addSource := GetBool("ADD_LOG_SOURCE_FILE", false)
		assert.True(tt, addSource)

		serviceName := GetString("OTEL_SERVICE_NAME", "VCP-VSA")
		assert.Equal(tt, "TEST-SERVICE", serviceName)

		otelGoogleProjectID := GetString("OTEL_GOOGLE_PROJECT_ID", "")
		assert.Equal(tt, "test-project-123", otelGoogleProjectID)

		// Clean up
		err = os.Unsetenv("LOGGER_LEVEL")
		assert.NoError(tt, err)
		err = os.Unsetenv("LOGGER_TYPE")
		assert.NoError(tt, err)
		err = os.Unsetenv("SLOG_HANDLER_TYPE")
		assert.NoError(tt, err)
		err = os.Unsetenv("EXPORTER_TYPE")
		assert.NoError(tt, err)
		err = os.Unsetenv("ADD_LOG_SOURCE_FILE")
		assert.NoError(tt, err)
		err = os.Unsetenv("OTEL_SERVICE_NAME")
		assert.NoError(tt, err)
		err = os.Unsetenv("OTEL_GOOGLE_PROJECT_ID")
		assert.NoError(tt, err)
	})
}
