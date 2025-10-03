package utils

import "testing"

func Test_ParseBizOpsReportParams(t *testing.T) {
	tests := []struct {
		name    string
		params  BizOpsReportParams
		wantErr bool
	}{
		{
			name:    "valid UTC",
			params:  BizOpsReportParams{TimeZone: UTC},
			wantErr: false,
		},
		{
			name:    "valid PST",
			params:  BizOpsReportParams{TimeZone: PST},
			wantErr: false,
		},
		{
			name:    "invalid timezone",
			params:  BizOpsReportParams{TimeZone: "IST"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ParseBizOpsReportParams(&tt.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBizOpsReportParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
