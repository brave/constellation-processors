table! {
    pending_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Int2,
        message -> Bytea,
    }
}

table! {
    recovered_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Int2,
        metric_name -> Varchar,
        metric_value -> Varchar,
        parent_recovered_msg_tag -> Nullable<Bytea>,
        count -> Int8,
        key -> Bytea,
        has_children -> Bool,
    }
}

allow_tables_to_appear_in_same_query!(
    pending_msgs,
    recovered_msgs,
);
