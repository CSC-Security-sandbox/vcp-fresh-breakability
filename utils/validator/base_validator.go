package validator

import (
	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	"log"
	"sync"
)

// BaseValidator provides a reusable validation and translation mechanism.
// It is concurrency-safe and supports dynamic error message overrides.
type BaseValidator struct {
	validate     *validator.Validate
	Translator   ut.Translator
	dnErrorStore sync.Map
}

// setup initializes the validator and English translator.
// Fatal error if the translator cannot be found.
func (bv *BaseValidator) setup() {
	enLocale := en.New()
	uni := ut.New(enLocale, enLocale)
	trans, found := uni.GetTranslator("en")
	if !found {
		log.Fatal("translator not found")
	}
	bv.validate = validator.New()
	bv.Translator = trans
}

// AddTranslation registers a custom translation for a validation tag.
// If a custom error message is present in dnErrorStore for the field, it is used.
func (bv *BaseValidator) AddTranslation(tag string, optionalErrMsg ...string) {
	if bv.Translator == nil {
		log.Fatal("translator not initialized - call setup() first")
	}

	registerFn := func(ut ut.Translator) error {
		return ut.Add(tag, "{0}", true)
	}

	transFn := func(ut ut.Translator, fe validator.FieldError) string {
		if optionalErrMsg != nil {
			return optionalErrMsg[0]
		}
		key := fe.StructField()
		if msg, ok := bv.dnErrorStore.Load(key); ok && msg != "" {
			return msg.(string)
		}
		return "Invalid " + fe.Field()
	}

	_ = (*bv.validate).RegisterTranslation(tag, bv.Translator, registerFn, transFn)
}
