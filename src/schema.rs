// @generated automatically by Diesel CLI.

diesel::table! {
    pending_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Int2,
        message -> Bytea,
    }
}

diesel::table! {
    recovered_msgs (id) {
        id -> Int8,
        msg_tag -> Bytea,
        epoch_tag -> Int2,
        #[max_length = 64]
        metric_name -> Varchar,
        #[max_length = 128]
        metric_value -> Varchar,
        parent_recovered_msg_tag -> Nullable<Bytea>,
        count -> Int8,
        key -> Bytea,
        has_children -> Bool,
    }
}

diesel::allow_tables_to_appear_in_same_query!(pending_msgs, recovered_msgs,);
