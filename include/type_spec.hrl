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
-type write_options() :: fast_term.  %% use byte array encoder term
-record(write_option,
        {fast_term = false :: boolean()}).

%%----Decoder Options-----------------------------------------------------------
-type read_options() :: fast_term 
                    | keep_wrap
                    | keep_binary_object.

-record(read_option,
        {fast_term = false :: boolean(),
         keep_wrapped = false :: boolean(),
         keep_binary_object = false :: boolean()}).

-record(wrapped,
        {binary :: binary(),
         offset :: non_neg_integer()}).

-record(binary_object,
        {body          :: binary(),
         schemas       :: #{integer() => non_neg_integer()}
        }).
