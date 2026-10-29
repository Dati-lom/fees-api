package fees

import "encore.dev/storage/sqldb"

var db = sqldb.NewDatabase("fees", sqldb.DatabaseConfig{
	Migrations: "./migrations",
})
