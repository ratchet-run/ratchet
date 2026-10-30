-- ratchet:single-statement
-- Drops scheduler_node.node_info. No code path ever wrote it: node registration and heartbeats
-- use native SQL that leaves the column out, so every row held NULL. Oracle has no DROP COLUMN
-- IF EXISTS, so guard through user_tab_columns; this also lets the migrator adopt a current
-- consolidated schema whose version ledger is empty.
DECLARE
  column_count PLS_INTEGER;
BEGIN
  SELECT COUNT(*) INTO column_count FROM user_tab_columns
   WHERE table_name = 'SCHEDULER_NODE'
     AND column_name = 'NODE_INFO';
  IF column_count > 0 THEN
    EXECUTE IMMEDIATE 'ALTER TABLE scheduler_node DROP COLUMN node_info';
  END IF;
END;
