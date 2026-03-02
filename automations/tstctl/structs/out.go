package structs

import "encoding/xml"

type TestSuites struct {
	XMLName    xml.Name    `xml:"testsuites"`
	TestSuites []TestSuite `xml:"testsuite"`
}

type TestSuite struct {
	Name      string     `xml:"name,attr"`
	Errors    int        `xml:"errors,attr"`
	Failures  int        `xml:"failures,attr"`
	Skipped   int        `xml:"skipped,attr"`
	Tests     int        `xml:"tests,attr"`
	TestCases []TestCase `xml:"testcase"`
}

type TestCase struct {
	ClassName string   `xml:"classname,attr"`
	Name      string   `xml:"name,attr"` // <-- Add this line
	Time      float64  `xml:"time,attr"`
	Failure   *Failure `xml:"failure"`
}

type Failure struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}
