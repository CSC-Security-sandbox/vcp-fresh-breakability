package validator

import (
	"bytes"
	"context"
	"encoding/hex"
	ber "github.com/go-asn1-ber/asn1-ber"
	"github.com/go-openapi/validate"
	"github.com/go-playground/validator/v10"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"net"
	"regexp"
	"strings"
)

type ActiveDirectoryValidator struct {
	BaseValidator
	se  database.Storage
	ctx context.Context
}

const (
	netBIOSPattern        = `(^[^\.\\/:*?"<>|\s@()=+\[\];,#]$)|(^[^\.\\/:*?"<>|\s@()=+\[\];,#][^\\/:*?"<>|\s@()=+\[\];,#]{0,8}[^\.\\/:*?"<>|\s@()=+\[\];,#]$)`
	sitePattern           = `(^[A-Za-z0-9]+[A-Za-z0-9\.\-]*[A-Za-z0-9]$)`
	netBIOSValidationErr  = `netBIOS in body must not contain any of the following characters: \/;*?"<>|@#()=+[]:, nor start or end with a dot.`
	siteValidationErr     = `Site names have to be at least 2 characters long (an empty string clears site assignment), can contain only alphabetical characters (A-Z), numeric characters (0-9), the minus sign (-), and the period (.). Period characters are allowed only when they are used to delimit the components of domain style names.`
	adUserValidationErr   = `Active Directory users must be unique and should not have the domain prefixed.`
	dnsValidationErr      = `Active Directory DNS cannot be a loopback ip, broadcast ip or multicast ip.`
	adNameValidationErr   = `Active Directory AD server name cannot be an ip address, provide AD server hostname`
	usernameValidationErr = `Active directory username should not contain any of the following characters: /\[]:;|=,+*?"<>`
)

var usernameInvalidCharsRe = regexp.MustCompile(`[/\\:;|\[\]=,+*?<>"]`)

var NewActiveDirectoryValidator = func(ctx context.Context, se database.Storage) *ActiveDirectoryValidator {
	adValidator := &ActiveDirectoryValidator{
		se:  se,
		ctx: ctx,
	}
	adValidator.setup()
	return adValidator
}

func (adValidator *ActiveDirectoryValidator) ValidateParams(params *common.CreateActiveDirectoryParams) error {
	return adValidator.validate.Struct(params)
}

func (adValidator *ActiveDirectoryValidator) RegisterValidators() error {
	err := adValidator.validate.RegisterValidation("NetBIOS", adValidator.netBIOSValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("NetBIOS")

	err = adValidator.validate.RegisterValidation("Username", adValidator.usernameValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("Username")

	err = adValidator.validate.RegisterValidation("Site", adValidator.siteNameValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("Site")

	err = adValidator.validate.RegisterValidation("OrganizationalUnit", adValidator.organizationalUnitValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("OrganizationalUnit")

	err = adValidator.validate.RegisterValidation("SecurityOperators", adValidator.activeDirectoryUsersValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("SecurityOperators")

	err = adValidator.validate.RegisterValidation("BackupOperators", adValidator.activeDirectoryUsersValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("BackupOperators")

	err = adValidator.validate.RegisterValidation("Administrators", adValidator.activeDirectoryUsersValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("Administrators")

	err = adValidator.validate.RegisterValidation("DNS", adValidator.dnsValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("DNS")

	err = adValidator.validate.RegisterValidation("ResourceId", adValidator.adNameValidator)
	if err != nil {
		return err
	}
	adValidator.AddTranslation("ResourceId")

	return nil
}

func (adValidator *ActiveDirectoryValidator) netBIOSValidator(fl validator.FieldLevel) bool {
	adValidator.dnErrorStore.Store(fl.StructFieldName(), netBIOSValidationErr)
	if err := validate.Pattern("netBIOS", "body", fl.Field().String(), netBIOSPattern); err != nil || strings.Contains(fl.Field().String(), "..") {
		return false
	}
	return true
}

func (adValidator *ActiveDirectoryValidator) usernameValidator(fl validator.FieldLevel) bool {
	adValidator.dnErrorStore.Store(fl.StructFieldName(), usernameValidationErr)
	if usernameInvalidCharsRe.MatchString(fl.Field().String()) {
		return false
	}
	return true
}

func (adValidator *ActiveDirectoryValidator) siteNameValidator(fl validator.FieldLevel) bool {
	adValidator.dnErrorStore.Store(fl.StructFieldName(), siteValidationErr)
	siteName := fl.Field().String()
	if siteName != "" {
		if err := validate.Pattern("site", "body", siteName, sitePattern); err != nil || strings.Contains(siteName, "..") {
			return false
		}
	}
	return true
}

func (adValidator *ActiveDirectoryValidator) organizationalUnitValidator(fl validator.FieldLevel) bool {
	ou := fl.Field().String()
	if ou == "" {
		return true
	}

	err := validateDistinguishedName(ou)
	if err != nil {
		// Store error message keyed by field
		key := fl.StructFieldName()
		adValidator.dnErrorStore.Store(key, "Error validating organizationalUnit: "+err.Error())
		return false
	}
	// Clean up any previous error
	adValidator.dnErrorStore.Delete(fl.StructFieldName())
	return true
}

func (adValidator *ActiveDirectoryValidator) activeDirectoryUsersValidator(fl validator.FieldLevel) bool {
	adValidator.dnErrorStore.Store(fl.StructFieldName(), adUserValidationErr)
	users := fl.Field().Interface().([]string)
	seen := make(map[string]bool, len(users))
	for _, elem := range users {
		if !seen[strings.ToLower(elem)] && !strings.Contains(elem, `\`) {
			seen[strings.ToLower(elem)] = true
		} else {
			return false
		}
	}
	return true
}

func (adValidator *ActiveDirectoryValidator) dnsValidator(fl validator.FieldLevel) bool {
	DNSs := strings.Split(fl.Field().String(), ",")
	key := fl.StructFieldName()
	for i := range DNSs {
		strippedDNS := strings.TrimSpace(DNSs[i])
		if strippedDNS != DNSs[i] {
			adValidator.dnErrorStore.Store(key, "ActiveDirectory DNS cannot contain trailing or leading whitespace: '"+DNSs[i]+"'")
			return false
		}

		ip := parseIPV4(DNSs[i])
		if ip == nil {
			adValidator.dnErrorStore.Store(key, `Active Directory DNS input '`+DNSs[i]+`' is not a valid IPv4 address`)
			return false
		}

		if ip.IsMulticast() || ip.IsLoopback() || ip.IsUnspecified() || fl.Field().String() == "255.255.255.255" {
			adValidator.dnErrorStore.Store(key, dnsValidationErr)
			return false
		}
	}

	// Clean up any previous error
	adValidator.dnErrorStore.Delete(fl.StructFieldName())
	return true
}

func (adValidator *ActiveDirectoryValidator) adNameValidator(fl validator.FieldLevel) bool {
	adValidator.dnErrorStore.Store(fl.StructFieldName(), adNameValidationErr)
	ip := net.ParseIP(fl.Field().String())
	if ip != nil {
		return false
	}
	return true
}

func validateDistinguishedName(dn string) error {
	escaping := false
	multiValue := false
	buffer := bytes.Buffer{}
	stringFromBuffer := func() string {
		s := buffer.String()
		buffer.Reset()
		return s
	}

	for i := 0; i < len(dn); i++ {
		char := dn[i]
		switch {
		case escaping:
			escaping = false
			// Only the leading or trailing space of a value may be escaped
			if char == ' ' {
				// Make sure a leading space is preceded by an unescaped equals sign =
				// Ignore cases where the string is too short to check for a preceding escaped equals sign (X=\i) as
				// those cases are either valid or, if invalid, caught by validateDNTypeValuePair
				if i < 4 || (dn[i-2] == '=' && dn[i-3] != '\\') {
					buffer.WriteByte(char)
					continue
				}
				// Make sure a trailing space is the final character or followed by an unescaped plus sign + or comma ,
				if len(dn) == i+1 || dn[i+1] == ',' || dn[i+1] == '+' {
					buffer.WriteByte(char)
					continue
				}
				return errors.New("Got corrupt escape character, spaces should not be escaped")
			}
			// While a hashtag only requires escaping if present at the start of a non-BER-encoded value, it is not
			// rejected by our AD even if it is escaped elsewhere
			specialCharacters := `,+"\<>;=/#`
			if strings.Contains(specialCharacters, string(char)) {
				buffer.WriteByte(char)
				continue
			}
			// Not a special character, assume hex encoded octet
			// A hex encoded octet is represented by two hex characters
			if len(dn) < i+2 {
				return errors.New("Failed to decode escaped character")
			}
			dst := []byte{0}
			n, err := hex.Decode(dst, []byte(dn[i:i+2]))
			if err != nil {
				return errors.New("Failed to decode escaped character")
			} else if n != 1 {
				return errors.New("Expected 1 byte when decoding escaped character, got " + string(rune(n)))
			}
			buffer.WriteByte(dst[0])
			i++
		case char == '\\':
			escaping = true
			buffer.WriteByte(char)
		case char == '=':
			buffer.WriteByte(char)
			// Special case: If the first character in the value is # the following data is BER encoded so we can just
			// fast forward and decode
			if len(dn) > i+1 && dn[i+1] == '#' {
				i += 2
				index := strings.IndexAny(dn[i:], ",+")
				var data string
				if index > 0 {
					data = dn[i : i+index]
				} else {
					data = dn[i:]
				}
				rawBER, err := hex.DecodeString(data)
				if err != nil {
					return errors.New("Failed to decode BER encoding: " + err.Error())
				}
				packet, err := ber.DecodePacketErr(rawBER)
				if err != nil {
					return errors.New("Failed to decode BER packet: " + err.Error())
				}
				buffer.WriteString(packet.Data.String())
				i += len(data) - 1
			}
		case char == ',' || char == '+':
			multiValue = true
			// Type, value pair complete. Validate and reset our buffer
			currentDN := stringFromBuffer()
			if err := validateDNTypeValuePair(currentDN); err != nil {
				return err
			}
		case char == '"' || char == '<' || char == '>' || char == ';' || char == '/':
			return errors.New("Got unescaped special character")
		default:
			buffer.WriteByte(char)
		}
	}
	// Make sure we don't end the string with an unescaped backslash
	if escaping {
		return errors.New("Got corrupt escape character")
	}
	// Finally we must validate the last type, value pair (or the only type, value pair)
	if multiValue {
		dn = stringFromBuffer()
	}
	return validateDNTypeValuePair(dn)
}

func validateDNTypeValuePair(dnTypeValuePair string) error {
	if len(dnTypeValuePair) < 3 {
		return errors.New("Incomplete type, value pair")
	}
	// Replace escaped equals signs, so that we can then validate that we have a "type=value" string
	dnTypeValuePair = strings.Replace(dnTypeValuePair, `\=`, `ab`, -1)
	if strings.Count(dnTypeValuePair, "=") != 1 || strings.HasPrefix(dnTypeValuePair, "=") || strings.HasSuffix(dnTypeValuePair, "=") {
		return errors.New("Incomplete type, value pair")
	}
	return nil
}

func parseIPV4(DNS string) net.IP {
	ip := net.ParseIP(DNS)
	if ip == nil {
		return nil
	}
	return ip.To4()
}
