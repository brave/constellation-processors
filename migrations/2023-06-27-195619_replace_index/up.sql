--- Drop constraint/btree index to increase performance
ALTER TABLE recovered_msgs DROP CONSTRAINT recovered_msgs_msg_tag_epoch_tag_key;

-- Replace btree indices with hash indices
DROP INDEX pending_msgs_epoch_tag_idx;
DROP INDEX pending_msgs_msg_tag_idx;
CREATE INDEX ON pending_msgs USING HASH (msg_tag);
CREATE INDEX ON recovered_msgs USING HASH (msg_tag);
