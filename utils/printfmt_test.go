package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Identifier struct {
	Id int
}
type TestStruct struct {
	Number string
	Title  string
	User   struct {
		Login string
	}
	Head struct {
		Identifier
		Ref  string
		Sha  string
		Repo string
	}
	Params    []string
	Map       map[string]string
	Interface interface{ GetInterfaceName() string }
}

func TestPrintFmt(t *testing.T) {
	t.Run("PrintPullRequest", func(tt *testing.T) {
		out := PrintObject(TestStruct{
			Number: "123",
			Title:  "title",
			User: struct {
				Login string
			}{Login: "login"},
			Head: struct {
				Identifier
				Ref  string
				Sha  string
				Repo string
			}{Ref: "ref", Sha: "sha1", Repo: "repo_name"},
			Params: []string{"param1", "param2"},
			Map:    map[string]string{"map1": "value1", "map2": "value2"},
		})
		assert.Contains(tt, out, "*Number*:  123", "The string should contain '*Number*: 123'")
	})
}
