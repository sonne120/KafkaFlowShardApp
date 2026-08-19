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
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, INDEX, DROP,
      CREATE ROUTINE, ALTER ROUTINE, EXECUTE, LOCK TABLES
      ON outboxdb.* TO 'app'@'%';

FLUSH PRIVILEGES;
