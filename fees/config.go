package fees

import "encore.dev/config"

// Config is the fees service's per-environment configuration. Values are
// set in config.cue.
type Config struct {
	// TemporalHostPort is the Temporal frontend address, e.g.
	// "localhost:7233" or "<namespace>.<account>.tmprl.cloud:7233".
	TemporalHostPort  string
	TemporalNamespace string
}

var cfg = config.Load[*Config]()

var secrets struct {
	// TemporalAPIKey authenticates to Temporal Cloud; setting it also turns
	// on TLS. Leave it unset locally: the dev server in docker-compose.yml
	// has no auth, and Encore reads an unset secret as "" when running
	// locally (in the cloud a missing secret stops the service).
	TemporalAPIKey string
}
