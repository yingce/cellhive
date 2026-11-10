package bucket

import (
	"errors"
	"fmt"
	oss "github.com/aliyun/aliyun-oss-go-sdk/oss"
	cos "github.com/tencentyun/cos-go-sdk-v5"
	"testing"
)

func TestRetiredNativeAppendIsPrecondition(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  error
	}{
		{"cos", nativeCOSError(fmt.Errorf("append: %w", &cos.ErrorResponse{Code: "ObjectNotAppendable"}))},
		{"oss", nativeOSSError(fmt.Errorf("append: %w", oss.ServiceError{Code: "ObjectNotAppendable"}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.got, ErrPrecondition) {
				t.Fatalf("retired append err=%v", tc.got)
			}
		})
	}
}
