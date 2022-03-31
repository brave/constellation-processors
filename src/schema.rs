table! {
    msg_sequences (id) {
        id -> Int8,
        epoch_tag -> Bpchar,
        recovered_msg_ids -> Nullable<Array<Int8>>,
        next_pending_msg_id -> Nullable<Int8>,
    }
}

table! {
    pending_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Bpchar,
        message -> Bytea,
    }
}

table! {
    recovered_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Bpchar,
        metric_name -> Varchar,
        metric_value -> Varchar,
        key -> Bytea,
    }
}

joinable!(msg_sequences -> pending_msgs (next_pending_msg_id));

allow_tables_to_appear_in_same_query!(
    msg_sequences,
    pending_msgs,
    recovered_msgs,
);
