package bundlr

import (
	"testing"
)

func Test_filter_check(t *testing.T) {
	allowList := []string{"*.a", "*.b", "*.abc"}
	denyList := []string{"a.*", "b.*", "*abc*.*", ".*"}
	tests := []struct {
		name    string
		want    bool
		wantErr bool
	}{
		{"1.a", true, false},
		{"1.b", true, false},
		{"1.abc", true, false},
		{"a.a", false, false},
		{"b.b", false, false},
		{"abc.abc", false, false},
		{"aaabccc.abc", false, false},
		{".1.a", false, false},
	}
	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			f := filter{
				allowList: allowList,
				denyList:  denyList,
			}
			got, err := f.check(tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("filter.check(%v) error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("filter.check(%v) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
