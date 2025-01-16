package kafkaclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateClientID(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateClientID("valid_client-id1.2"))

	var invalidClientIDs []string //nolint:prealloc // ok
	for i := range 256 {
		if (i >= 'a' && i <= 'z') || (i >= 'A' && i <= 'Z') || i == '.' || i == '_' || i == '-' || (i >= '0' && i <= '9') {
			continue
		}
		invalidClientIDs = append(invalidClientIDs, "id_"+string(rune(i)))
	}

	for _, s := range invalidClientIDs {
		t.Logf("invalid client ID: %q", s)
		require.Error(t, ValidateClientID(s))
	}
}
