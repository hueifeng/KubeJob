# Migration and rollback

Apply `sql/002_distributed_runtime_v2.sql` with a backup and a migration-only
PostgreSQL account. The migration is additive and does not run from web startup.
Historical `timestamp without time zone` values are interpreted as UTC; correct
that assumption before applying it if the existing data was written otherwise.

To roll back application behavior, deploy the previous build/configuration and
set `RuntimeMode=LegacyDispatcher`. Do not run the legacy Dispatcher, legacy
Cron scheduler, or offline-node reset concurrently with LeaseV2. Leave the V2
columns/tables in place during the rollback window so a retry does not destroy
attempt and payload evidence. After the retention window, remove V2 objects only
with a separately reviewed destructive migration and backup.
