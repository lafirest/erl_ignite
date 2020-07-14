%%----Cache Peek Mode----------------------------------------------------------
-define(CACHEPEEKMODE_ALL, 0).
-define(CACHEPEEKMODE_NEAR, 1).
-define(CACHEPEEKMODE_PRIMARY, 2).
-define(CACHEPEEKMODE_BACKUP, 3).
-define(CACHEPEEKMODE_ONHEAP, 4).
-define(CACHEPEEKMODE_OFFHEAP, 5).

%%----Default Types Name--------------------------------------------------------
-define(ATOM_TYPE_NAME, "ErlangAtom").

%%----Encoder Options-----------------------------------------------------------
-type write_option() :: fast_term.  %% use byte array encoder term

-type write_options() :: list(write_option()).  

-record(write_option,
        {fast_term = false :: boolean()}).

%%----Options-----------------------------------------------------------
-type read_option() :: fast_term 
                    | keep_wrap
                    | keep_binary_object
                    | {timeout, infinity | non_neg_integer()}.

-type read_options() :: list(read_option()).

-record(read_option,
        {fast_term = false :: boolean(),
         keep_wrapped = false :: boolean(),
         keep_binary_object = false :: boolean(),
         timeout = infinity }).

-record(wrapped,
        {binary :: binary(),
         offset :: non_neg_integer()}).

-record(binary_object,
        {body          :: binary(),
         schemas       :: #{integer() => non_neg_integer()}
        }).

-type async_query_callback() :: undefined  %% sync
                            | ignore
                            | {function, fun()}
                            | {message, pid(), atom()}.

-type sql_options() :: atom().

-type query_options() :: #{write => write_options(),
                           read => read_options(),
                           async => async_query_callback(),
                           sql => sql_options()}.

-record(sql_query_result,
        {cursor_id :: integer(),
         pairs :: list({term(), term()}),
         has_more :: boolean()}
       ).

-record(sql_query_fields_result,
        {cursor_id :: integer(),
         column    :: non_neg_integer(),
         names     :: list(string()),
         rows      :: list(list()),
         has_more :: boolean()}
       ).
