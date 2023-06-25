package kafka

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestRetryMetadataString(t *testing.T) {
	// ARRANGE
	testcases := []struct {
		sut    *RetryMetadata
		result string
	}{
		{sut: &RetryMetadata{}, result: "seq=0 num=0 max=0 interval=0s"},
		{sut: &RetryMetadata{Seq: 1}, result: "seq=1 num=0 max=0 interval=0s"},
		{sut: &RetryMetadata{Seq: 1, Num: 2}, result: "seq=1 num=2 max=0 interval=0s"},
		{sut: &RetryMetadata{Seq: 1, Num: 2, Max: 3}, result: "seq=1 num=2 max=3 interval=0s"},
		{sut: &RetryMetadata{Seq: 1, Num: 2, Max: 3, Interval: 4 * time.Second}, result: "seq=1 num=2 max=3 interval=4s"},
		{sut: &RetryMetadata{Seq: 1, Num: 2, Max: -1, Interval: 4 * time.Second}, result: "seq=1 num=2 interval=4s"},
	}
	for tn, tc := range testcases {
		t.Run(fmt.Sprintf("%d", tn), func(t *testing.T) {
			// ACT
			got := tc.sut.String()

			// ASSERT
			wanted := tc.result
			if wanted != got {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}

func TestRetryMetadataFromContext(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name string
		args args
		want *RetryMetadata
	}{
		{"nil", args{ctx: nil}, nil},
		{"empty", args{ctx: context.Background()}, nil},
		{"not empty", args{ctx: context.WithValue(context.Background(), kRetryMetadata, &RetryMetadata{})}, &RetryMetadata{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RetryMetadataFromContext(tt.args.ctx); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RetryMetadataFromContext() = %v, want %v", got, tt.want)
			}
		})
	}
}
