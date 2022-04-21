CREATE TABLE recovered_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	metric_name varchar(32) not null,
	metric_value varchar(32) not null,
	parent_recovered_msg_id bigint null,
	count bigint not null,
	key bytea not null,
	has_children boolean not null,
	UNIQUE (msg_tag, epoch_tag)
);
CREATE INDEX on recovered_msgs (parent_recovered_msg_id);

CREATE TABLE pending_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	parent_recovered_msg_id bigint null,
	message bytea not null
);
CREATE INDEX on pending_msgs (msg_tag);
