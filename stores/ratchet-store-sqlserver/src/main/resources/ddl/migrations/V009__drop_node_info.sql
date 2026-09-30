-- Drops scheduler_node.node_info. No code path ever wrote it: node registration and heartbeats
-- use native SQL that leaves the column out, so every row held NULL. The column never carried a
-- default constraint, so no constraint needs dropping first.
ALTER TABLE scheduler_node DROP COLUMN IF EXISTS node_info;
