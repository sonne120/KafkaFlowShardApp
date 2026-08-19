-- Runs once on first start of mysql-master (empty datadir).
-- All users replicate to the slave automatically via GTID replication,
-- so they exist on both nodes — required for failover symmetry.

-- 1) replication user (used by CHANGE REPLICATION SOURCE on the slave,
--    and by the old master when Orchestrator re-parents it after failover)
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'replpass';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';

-- 2) orchestrator topology user.
--    SUPER is deprecated in 8.4 -> use the dynamic privileges it needs
--    for detection, promotion (toggling super_read_only) and re-parenting.
CREATE USER IF NOT EXISTS 'orch'@'%' IDENTIFIED WITH caching_sha2_password BY 'orchpass';
GRANT PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'orch'@'%';
GRANT SYSTEM_VARIABLES_ADMIN, REPLICATION_SLAVE_ADMIN, CONNECTION_ADMIN ON *.* TO 'orch'@'%';
GRANT SELECT ON mysql.slave_master_info TO 'orch'@'%';
GRANT SELECT ON performance_schema.* TO 'orch'@'%';

-- 3) proxysql monitor user (polls @@global.super_read_only to decide
--    which hostgroup a node belongs to — the automatic writer election)
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED WITH caching_sha2_password BY 'monitorpass';
GRANT REPLICATION CLIENT ON *.* TO 'monitor'@'%';

-- 4) application user (srv_ingest outbox writes, srv_pub relay).
--    EXECUTE is required for the GetDataFromTempTable stored procedure,
--    CREATE/CREATE ROUTINE for IOutboxInitializer.InitializeAsync.
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED WITH caching_sha2_password BY 'apppass';
--    CREATE TEMPORARY TABLES is required too: GetDataFromTempTable builds a
--    TempOutboxIds temp table to hold the ids it reserved. Without it every
--    relay poll fails with "Access denied for user 'app'@'%' to database
--    'outboxdb'", which reads like a database-level grant problem rather than
--    a missing privilege on one statement.
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, DROP,
      CREATE ROUTINE, ALTER ROUTINE, EXECUTE, LOCK TABLES,
      CREATE TEMPORARY TABLES
      ON outboxdb.* TO 'app'@'%';

FLUSH PRIVILEGES;

-- Arm the split-brain failsafe. PERSIST (not the config file) so that the
-- bootstrap above could run: this writes mysqld-auto.cnf, so every later start
-- comes back read-only. Promotion uses SET GLOBAL, which does not persist —
-- so a promoted node that restarts reverts to read-only and waits to be
-- appointed again.
SET PERSIST read_only       = ON;
SET PERSIST super_read_only = ON;
