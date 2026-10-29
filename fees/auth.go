package fees

import (
	"context"
	"crypto/subtle"

	"encore.dev/beta/auth"
	"encore.dev/beta/errs"
)

// serviceUID identifies callers authenticated with the service token.
const serviceUID auth.UID = "service"

// AuthHandler authenticates callers by a shared service token, sent as
// "Authorization: Bearer <token>". The Fees API is called by other internal
// services, not end users, so every holder of the token is trusted equally;
// per-caller identities and account ownership checks would need a real
// identity model and are out of scope.
//
//encore:authhandler
func AuthHandler(ctx context.Context, token string) (auth.UID, error) {
	want := secrets.ServiceToken
	// Fail closed: with no token configured nobody gets in.
	if want == "" || subtle.ConstantTimeCompare([]byte(token), []byte(want)) != 1 {
		return "", errs.B().Code(errs.Unauthenticated).Msg("invalid service token").Err()
	}
	return serviceUID, nil
}
