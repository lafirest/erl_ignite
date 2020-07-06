-module(schema_manager).

-record(type_schema,
        {type_id :: integer(),
         type_name :: string(),
         schema_id :: integer(),
         fields :: list(string()),
         field_ids :: list(integer()),
         to_spec :: (fun((term()) -> term())),
         from_sepc :: (fun((term()) -> term()))
        }).

put_type({TypeName, Fields}) -> ok.
get_type(TypeId) -> ok.


