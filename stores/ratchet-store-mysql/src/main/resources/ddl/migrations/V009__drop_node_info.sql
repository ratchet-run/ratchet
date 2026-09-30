-- Drops scheduler_node.node_info. No code path ever wrote it: node registration and heartbeats
-- use native SQL that leaves the column out, so every row held NULL. MySQL 8.0 has no DROP
-- COLUMN IF EXISTS, so guard through information_schema; this also lets the migrator adopt a
-- current consolidated schema whose version ledger is empty.

SET @ratchet_drop_node_info_ddl =
    (SELECT IF(COUNT(*) > 0,
               'ALTER TABLE scheduler_node DROP COLUMN node_info',
               'SELECT 1')
       FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = 'scheduler_node'
        AND column_name = 'node_info');
PREPARE ratchet_drop_node_info_statement FROM @ratchet_drop_node_info_ddl;
EXECUTE ratchet_drop_node_info_statement;
DEALLOCATE PREPARE ratchet_drop_node_info_statement;
