-module(ignite).

-export([get/2, get/3, 
         get_all/2, get_all/3,
         put/3, put/4, 
         put_all/2, put_all/3,
         contains_key/2, contains_key/3, 
         contains_keys/2, contains_keys/3, 
         get_and_put/3, get_and_put/4, 
         get_and_replace/3, get_and_replace/4, 
         get_and_remove/2, get_and_remove/3, 
         put_if_absent/3, put_if_absent/4,
         get_put_if_absent/3, get_put_if_absent/4,
         replace/3, replace/4,
         replace_if_equals/4, replace_if_equals/5, 
         clear/1, clear/2,
         clear_key/2, clear_key/3, 
         clear_keys/2, clear_keys/3, 
         get_size/2, get_size/3,
         remove_key/2, remove_key/3, 
         remove_if_equals/3, remove_if_equals/4, 
         remove_keys/2, remove_keys/3, 
         remove_all/1, remove_all/2
        ]).

-include("type_spec.hrl").

%%----sync API ----------------------------------------------------------
get(Cache, Key) -> get(Cache, Key, #{}).
get(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_all(Cache, Key) -> get_all(Cache, Key, #{}).
get_all(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_all(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put(Cache, Key, Value) -> put(Cache, Key, Value, #{}).
put(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:put(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put_all(Cache, Pairs) -> put_all(Cache, Pairs, #{}).
put_all(Cache, Pairs, Options) -> 
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:put_all(Cache, Pairs, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

contains_key(Cache, Key) -> contains_key(Cache, Key, #{}).
contains_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:contains_key(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

contains_keys(Cache, Key) -> contains_keys(Cache, Key, #{}).
contains_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:contains_keys(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_put(Cache, Key, Value) -> get_and_put(Cache, Key, Value, #{}).
get_and_put(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_and_put(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_replace(Cache, Key, Value) -> get_and_replace(Cache, Key, Value, #{}).
get_and_replace(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_and_replace(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_remove(Cache, Key) -> get_and_remove(Cache, Key, #{}).
get_and_remove(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_and_remove(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put_if_absent(Cache, Key, Value) -> put_if_absent(Cache, Key, Value, #{}).
put_if_absent(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:put_if_absent(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_put_if_absent(Cache, Key, Value) -> get_put_if_absent(Cache, Key, Value, #{}).
get_put_if_absent(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_put_if_absent(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

replace(Cache, Key, Value) -> replace(Cache, Key, Value, #{}).
replace(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:replace(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

replace_if_equals(Cache, Key, Compare, Value) -> replace_if_equals(Cache, Key, Compare, Value, #{}).
replace_if_equals(Cache, Key, Compare, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:replace_if_equals(Cache, Key, Compare, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear(Cache) -> clear(Cache, #{}).
clear(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:clear(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear_key(Cache, Key) -> clear_key(Cache, Key, #{}).
clear_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:clear_key(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear_keys(Cache, Key) -> clear_keys(Cache, Key, #{}).
clear_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:clear_keys(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_size(Cache, Mode) -> get_size(Cache, Mode, #{}).
get_size(Cache, Mode, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:get_size(Cache, Mode),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_key(Cache, Key) -> remove_key(Cache, Key, #{}).
remove_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:remove_key(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_if_equals(Cache, Key, Compare) -> remove_if_equals(Cache, Key, Compare, #{}).
remove_if_equals(Cache, Key, Compare, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:remove_if_equals(Cache, Key, Compare, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_keys(Cache, Key) -> remove_keys(Cache, Key, #{}).
remove_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:remove_keys(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_all(Cache) -> remove_all(Cache, #{}).
remove_all(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:remove_all(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

%%---- Internal functions-------------------------------------------------------------------
do_query(Query, Handler, undefined, Option) -> sync_query(Query, Handler, Option);
do_query(Query, Handler, Callback, Option) ->
    erlang:spawn(fun() -> 
                    case sync_query(Query, Handler, Option) of
                        {on_query_failed, _, _} = Error -> Error;
                        Value -> do_async_callback(Callback, Value)
                    end
                 end).

sync_query(Query, Handler, #read_option{timeout = Timeout} = Option) ->
    Ref = erlang:make_ref(),
    From = self(),
    wpool:cast(ignite, {query, From, Ref, Query}, random_worker),
    receive {on_query_success, Ref, Content} ->
                ignite_op_response_handler:Handler(Content, Option);
            {on_query_failed, Ref, ErrorMsg} ->
                {on_query_failed, Query, ErrorMsg}
    after Timeout -> 
              {on_query_failed, Query, timeout}
    end.

do_async_callback({function, Func}, Value) ->
    Func(Value);

do_async_callback({message, Pid, Tag}, Value) ->
    erlang:send(Pid, {Tag, Value});

do_async_callback(ignore, Value) -> Value.
