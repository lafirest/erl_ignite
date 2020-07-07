-type schema_format() :: full | compact.
-type version() :: non_neg_integer().
-type value() :: term().
-type values() :: list(value()).
-type field_spec() :: {atom(), value()}.
-type field_specs() :: list(field_spec()).

-record(type_schema,
        {type_id :: integer(),
         type_name :: string(),
         schema_id :: integer(),
         version :: integer(),
         fields :: list(string()),
         field_ids :: list(integer()),
         to_spec :: (fun((value()) -> field_specs())),
         from_reader :: (fun((values()) -> value())),
         schema_format :: schema_format()
        }).

-record(type_register_data,
        {type_name :: string(),
         version :: version(),
         fields :: list(string()),
         to_spec :: (fun((value()) -> field_specs())),
         from_reader :: (fun((values()) -> value()))
        }).

