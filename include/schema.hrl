-type schema_format() :: full | compact.
-type version()       :: non_neg_integer().
-type value()         :: term().
-type values()        :: list(value()).
-type field_id()      :: integer().
-type type_type()     :: tuple | map.

-type collection_type() :: array
                    | sets
                    | ordsets.

-type field_type() :: byte       
                    | short             
                    | int  
                    | long
                    | float
                    | double
                    | char
                    | bool
                    | undefined
                    | term               
                    | bin_string
                    | string
                    | uuid
                    | timestamp
                    | date
                    | time 
                    | {enum, string()} 
                    | byte_array
                    | short_array
                    | int_array
                    | long_array
                    | float_array
                    | double_array
                    | char_array
                    | bool_array
                    | bin_string_array
                    | string_array
                    | uuid_array
                    | timestamp_array
                    | date_array
                    | time_array
                    | {object_array, string()}
                    | {collection, collection_type(), field_type()}
                    | {map, field_type(), field_type()}
                    | {orddict, field_type(), field_type()}
                    | {enum_array, string()}
                    | {complex_object, string()}
                    | wrapped
                    | {binary_enum, string()}.

-type map_key_type() :: atom() | string() | binary().
-type upgrade_hook() :: fun((term()) -> term()).
-type constructor() :: fun((list(term())) -> term()).
-type enum_info() :: {atom(), integer(), string()}. %% {Tag, Value, Name}

-type field_info() ::
    #{name := string(),
      type := field_type(),
      %% key default(undefined) for tuple, others for map
      key  => map_key_type()
     }.

-record(type_schema,
        {type_id        :: integer(),
         type_name      :: string(),
         type_type      :: type_type(),
         type_tag       :: undefined | atom(), %% default(undefined) for map, atom() for tuple
         schema_id      :: integer(),
         version        :: integer(),
         %% field type list, using when encoder
         field_types    :: list(field_type()),
         %% field id order, using when encoder the full mode schema
         field_id_order :: list(field_id()),
         %% use for the order map fields, using when en/decoder, undefined for tuple
         field_keys     :: undefined | list(map_key_type()),
         schema_format  :: schema_format(),
         constructor   :: undefined | constructor(),
         on_upgrades    :: list(upgrade_hook())
        }).

-record(enum_schema, 
        {type_id        :: integer(),
         type_name      :: string(),
         values         :: list(enum_info())
        }).

-type type_register() ::
    #{name          := string(),
      type          := type_type(),
      version       := version(),
      schema_format := schema_format(),
      fields        := list(field_info()),
      type_tag    => atom(),
      name_tag    => atom(),
      constructor => constructor(),
      on_upgrades => list(upgrade_hook())
     }.

-type enum_register() ::
    #{name     := string(),
      enums    := list(enum_info())}.

-define(ENUM_ATOM_POS, 1).
-define(ENUM_VALUE_POS, 2).
-define(ENUM_NAME_POS, 3).

-define(COMPLEX_OBJECT_HEAD_OFFSET, 24).
