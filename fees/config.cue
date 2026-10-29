package fees

// Temporal connection per environment. Locally (encore run / encore test)
// this is the dev server from docker-compose.yml. Every other environment
// must set TemporalHostPort here, or the build fails, so a deploy can't
// silently point at localhost.
TemporalHostPort:  string
TemporalNamespace: string | *"default"

if #Meta.Environment.Cloud == "local" {
	TemporalHostPort: "localhost:7233"
}
