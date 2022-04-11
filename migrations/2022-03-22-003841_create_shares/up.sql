CREATE TABLE recovered_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	metric_name varchar(32) not null,
	metric_value varchar(32) not null,
	key bytea not null,
	UNIQUE (msg_tag, epoch_tag)
);

CREATE TABLE pending_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	parent_recovered_msg_id bigint null,
	message bytea not null
);
CREATE INDEX on pending_msgs (epoch_tag);
CREATE INDEX on pending_msgs (msg_tag);
