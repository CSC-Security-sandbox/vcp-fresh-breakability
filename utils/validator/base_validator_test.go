package validator

import (
	"sync"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseValidator_Setup(t *testing.T) {
	bv := &BaseValidator{}

	// Test setup initializes validator and translator
	bv.setup()

	assert.NotNil(t, bv.validate, "validator should be initialized")
	assert.NotNil(t, bv.Translator, "translator should be initialized")
}

func TestBaseValidator_SetupMultipleCalls(t *testing.T) {
	bv := &BaseValidator{}

	// Multiple calls to setup should not panic
	bv.setup()
	originalValidator := bv.validate
	originalTranslator := bv.Translator

	bv.setup()

	// Should create new instances
	assert.NotNil(t, bv.validate, "validator should remain initialized")
	assert.NotNil(t, bv.Translator, "translator should remain initialized")
	assert.NotSame(t, originalValidator, bv.validate, "new validator instance should be created")
	assert.NotSame(t, originalTranslator, bv.Translator, "new translator instance should be created")
}

func TestBaseValidator_AddTranslation_WithoutCustomMessage(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Add translation for 'required' tag
	bv.AddTranslation("required")

	// Create a test struct to validate
	type TestStruct struct {
		Name string `validate:"required"`
	}

	testObj := &TestStruct{}
	err := bv.validate.Struct(testObj)
	require.Error(t, err, "validation should fail for empty required field")

	// Verify the error can be translated
	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	require.Len(t, validationErrs, 1, "should have exactly one validation error")

	translatedMsg := validationErrs[0].Translate(bv.Translator)
	assert.Equal(t, "Invalid Name", translatedMsg, "should use default translation format")
}

func TestBaseValidator_AddTranslation_WithCustomMessage(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Store custom error message
	customMsg := "Custom error message for Name field"
	bv.dnErrorStore.Store("Name", customMsg)

	// Add translation for 'required' tag
	bv.AddTranslation("required")

	// Create a test struct to validate
	type TestStruct struct {
		Name string `validate:"required"`
	}

	testObj := &TestStruct{}
	err := bv.validate.Struct(testObj)
	require.Error(t, err, "validation should fail for empty required field")

	// Verify the error uses custom message
	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	require.Len(t, validationErrs, 1, "should have exactly one validation error")

	translatedMsg := validationErrs[0].Translate(bv.Translator)
	assert.Equal(t, customMsg, translatedMsg, "should use custom error message")
}

func TestBaseValidator_AddTranslation_EmptyCustomMessage(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Store empty custom error message
	bv.dnErrorStore.Store("Name", "")

	// Add translation for 'required' tag
	bv.AddTranslation("required")

	// Create a test struct to validate
	type TestStruct struct {
		Name string `validate:"required"`
	}

	testObj := &TestStruct{}
	err := bv.validate.Struct(testObj)
	require.Error(t, err, "validation should fail for empty required field")

	// Verify the error uses default message when custom message is empty
	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	require.Len(t, validationErrs, 1, "should have exactly one validation error")

	translatedMsg := validationErrs[0].Translate(bv.Translator)
	assert.Equal(t, "Invalid Name", translatedMsg, "should use default translation when custom message is empty")
}

func TestBaseValidator_AddTranslation_MultipleFields(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Store custom messages for different fields
	bv.dnErrorStore.Store("Name", "Name is mandatory")
	bv.dnErrorStore.Store("Email", "Valid email is required")

	// Add translation for 'required' tag
	bv.AddTranslation("required")

	// Create a test struct with multiple required fields
	type TestStruct struct {
		Name  string `validate:"required"`
		Email string `validate:"required"`
		Age   int    `validate:"required"`
	}

	testObj := &TestStruct{}
	err := bv.validate.Struct(testObj)
	require.Error(t, err, "validation should fail for empty required fields")

	// Verify each error uses appropriate message
	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	require.Len(t, validationErrs, 3, "should have three validation errors")

	fieldMessages := make(map[string]string)
	for _, fieldErr := range validationErrs {
		fieldMessages[fieldErr.StructField()] = fieldErr.Translate(bv.Translator)
	}

	assert.Equal(t, "Name is mandatory", fieldMessages["Name"], "Name field should use custom message")
	assert.Equal(t, "Valid email is required", fieldMessages["Email"], "Email field should use custom message")
	assert.Equal(t, "Invalid Age", fieldMessages["Age"], "Age field should use default message")
}

func TestBaseValidator_AddTranslation_DifferentTags(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Add translations for different validation tags
	bv.AddTranslation("required")
	bv.AddTranslation("email")
	bv.AddTranslation("min")

	// Create a test struct with different validation rules
	type TestStruct struct {
		Name  string `validate:"required,min=2"`
		Email string `validate:"required,email"`
	}

	testObj := &TestStruct{
		Name:  "A",       // Too short
		Email: "invalid", // Invalid email
	}

	err := bv.validate.Struct(testObj)
	require.Error(t, err, "validation should fail")

	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	assert.Greater(t, len(validationErrs), 0, "should have validation errors")

	// Verify all errors can be translated
	for _, fieldErr := range validationErrs {
		translatedMsg := fieldErr.Translate(bv.Translator)
		assert.NotEmpty(t, translatedMsg, "translated message should not be empty")
		assert.Contains(t, translatedMsg, "Invalid", "should contain 'Invalid' in default translation")
	}
}

func TestBaseValidator_ConcurrencySafety(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()
	bv.AddTranslation("required")

	// Test concurrent access to dnErrorStore
	const numGoroutines = 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Concurrent writes to dnErrorStore
			key := "Field" + string(rune(id%10))
			value := "Error message " + string(rune(id))
			bv.dnErrorStore.Store(key, value)

			// Concurrent reads from dnErrorStore
			if msg, ok := bv.dnErrorStore.Load(key); ok {
				assert.IsType(t, string(""), msg, "stored value should be string")
			}
		}(i)
	}

	wg.Wait()

	// Verify the sync.Map is still functional
	bv.dnErrorStore.Store("TestKey", "TestValue")
	value, ok := bv.dnErrorStore.Load("TestKey")
	assert.True(t, ok, "should be able to load stored value")
	assert.Equal(t, "TestValue", value, "should retrieve correct value")
}

func TestBaseValidator_AddTranslation_NilTranslator(t *testing.T) {
	// This test verifies that AddTranslation fails fast when translator is not initialized
	// Since log.Fatal() is used, we need to test this in a subprocess or skip this scenario
	t.Skip("AddTranslation with nil translator calls log.Fatal() which exits the process")
}

func TestBaseValidator_IntegrationWithValidator(t *testing.T) {
	bv := &BaseValidator{}
	bv.setup()

	// Test integration with actual validator package
	bv.AddTranslation("required")
	bv.AddTranslation("email")

	// Create a more complex struct
	type Address struct {
		Street string `validate:"required"`
		City   string `validate:"required"`
	}

	type User struct {
		Name    string   `validate:"required,min=2"`
		Email   string   `validate:"required,email"`
		Age     int      `validate:"required,min=18"`
		Address *Address `validate:"required"`
	}

	// Test with invalid data
	user := &User{
		Name:  "A",             // Too short
		Email: "invalid-email", // Invalid format
		Age:   16,              // Too young
		Address: &Address{
			Street: "", // Required but empty
			City:   "", // Required but empty
		},
	}

	err := bv.validate.Struct(user)
	require.Error(t, err, "validation should fail for invalid user")

	validationErrs, ok := err.(validator.ValidationErrors)
	require.True(t, ok, "error should be ValidationErrors type")
	assert.Greater(t, len(validationErrs), 0, "should have multiple validation errors")

	// Verify all errors can be translated
	for _, fieldErr := range validationErrs {
		translatedMsg := fieldErr.Translate(bv.Translator)
		assert.NotEmpty(t, translatedMsg, "each error should have a translation")
	}
}
