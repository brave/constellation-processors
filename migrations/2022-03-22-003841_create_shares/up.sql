CREATE TABLE recovered_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	metric_name varchar(32) not null,
	metric_value varchar(32) not null,
	parent_recovered_msg_tag bytea null,
	count bigint not null,
	key bytea not null,
	has_children boolean not null,
	UNIQUE (msg_tag, epoch_tag)
);

CREATE TABLE pending_msgs (
	id bigserial PRIMARY KEY,
	msg_tag bytea not null,
	epoch_tag smallint not null,
	message bytea not null
);
ALTER TABLE pending_msgs ALTER msg_tag SET STORAGE PLAIN;
ALTER TABLE pending_msgs ALTER message SET STORAGE EXTERNAL;
CREATE INDEX on pending_msgs (epoch_tag);
CREATE INDEX on pending_msgs (msg_tag);
