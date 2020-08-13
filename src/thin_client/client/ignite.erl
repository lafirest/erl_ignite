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
         remove_all/1, remove_all/2,
         query/3, query/4, query/5,
         query_fields/2, query_fields/3, query_fields/4,
         execute/2, execute/3, execute/4,
         query_row/2, query_row/3, query_row/4,
         query_one/2, query_one/4, query_one/5,
         batch_query/6,
         batch_query_fields/5,
         upsert/5, upsert/6,
         get_name/2, get_name/3,
         register_name/3, register_name/4,
         get_type/1, get_type/2,
         put_type/1, put_type/2,
         create_cache/1, create_cache/2,
         get_or_create_cache/1, get_or_create_cache/2,
         get_cache_names/0, get_cache_names/1,
         get_configuration/1, get_configuration/2,
         create_with_configuration/1, create_with_configuration/2,
         get_or_create_with_configuration/1, get_or_create_with_configuration/2,
         destroy_cache/1, destroy_cache/2
        ]).

-include("type_spec.hrl").

%%----KV API ----------------------------------------------------------
get(Cache, Key) -> get(Cache, Key, #{}).
get(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_all(Cache, Key) -> get_all(Cache, Key, #{}).
get_all(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put(Cache, Key, Value) -> put(Cache, Key, Value, #{}).
put(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put_all(Cache, Pairs) -> put_all(Cache, Pairs, #{}).
put_all(Cache, Pairs, Options) -> 
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Pairs, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

contains_key(Cache, Key) -> contains_key(Cache, Key, #{}).
contains_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

contains_keys(Cache, Key) -> contains_keys(Cache, Key, #{}).
contains_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_put(Cache, Key, Value) -> get_and_put(Cache, Key, Value, #{}).
get_and_put(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_replace(Cache, Key, Value) -> get_and_replace(Cache, Key, Value, #{}).
get_and_replace(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_and_remove(Cache, Key) -> get_and_remove(Cache, Key, #{}).
get_and_remove(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put_if_absent(Cache, Key, Value) -> put_if_absent(Cache, Key, Value, #{}).
put_if_absent(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_put_if_absent(Cache, Key, Value) -> get_put_if_absent(Cache, Key, Value, #{}).
get_put_if_absent(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

replace(Cache, Key, Value) -> replace(Cache, Key, Value, #{}).
replace(Cache, Key, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

replace_if_equals(Cache, Key, Compare, Value) -> replace_if_equals(Cache, Key, Compare, Value, #{}).
replace_if_equals(Cache, Key, Compare, Value, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Compare, Value, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear(Cache) -> clear(Cache, #{}).
clear(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear_key(Cache, Key) -> clear_key(Cache, Key, #{}).
clear_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

clear_keys(Cache, Key) -> clear_keys(Cache, Key, #{}).
clear_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_size(Cache, Mode) -> get_size(Cache, Mode, #{}).
get_size(Cache, Mode, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Mode),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_key(Cache, Key) -> remove_key(Cache, Key, #{}).
remove_key(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_if_equals(Cache, Key, Compare) -> remove_if_equals(Cache, Key, Compare, #{}).
remove_if_equals(Cache, Key, Compare, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, Compare, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_keys(Cache, Key) -> remove_keys(Cache, Key, #{}).
remove_keys(Cache, Key, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache, Key, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

remove_all(Cache) -> remove_all(Cache, #{}).
remove_all(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_kv_query:?FUNCTION_NAME(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

%%----SQL API ----------------------------------------------------------
query(Cache, Table, Sql) -> query(Cache, Table, Sql, [], #{}).
query(Cache, Table, Sql, Arguments) -> query(Cache, Table, Sql, Arguments, #{}).
query(Cache, Table, Sql, Arguments, Option) ->
    {Async, SqlOptions, WO, RO} = utils:parse_sql_options(Option),
    case Async of
        undefined -> inner_query(Cache, Table, Sql, Arguments, SqlOptions, WO, RO);
        _ ->
            erlang:spawn(fun() -> 
                                 case inner_query(Cache, Table, Sql, Arguments, SqlOptions, WO, RO) of
                                     {on_query_failed, _, _} = Error -> Error;
                                     Value -> do_async_callback(Async, Value)
                                 end
                         end)
    end.

query_fields(Cache, Sql) -> query_fields(Cache, Sql, [], #{}).
query_fields(Cache, Sql, Arguments) -> query_fields(Cache, Sql, Arguments, #{}).
query_fields(Cache, Sql, Arguments, Option) ->
    {Async, SqlOptions, WO, RO} = utils:parse_sql_options(Option),
    case Async of
        undefined -> inner_query_fields(Cache, Sql, Arguments, SqlOptions, WO, RO);
        _ ->
            erlang:spawn(fun() -> 
                                 case inner_query_fields(Cache, Sql, Arguments, SqlOptions, WO, RO) of
                                     {on_query_failed, _, _} = Error -> Error;
                                     Value -> do_async_callback(Async, Value)
                                 end
                         end)
    end.

query_row(Cache, Sql) -> safe_row(query_fields(Cache, Sql)).
query_row(Cache, Sql, Arguments) -> safe_row(query_fields(Cache, Sql, Arguments)).
query_row(Cache, Sql, Arguments, Option) -> safe_row(query_fields(Cache, Sql, Arguments, Option)).

query_one(Cache, Sql) -> safe_one(query_fields(Cache, Sql), undefined).
query_one(Cache, Sql, Arguments, Default) -> safe_one(query_fields(Cache, Sql, Arguments), Default).
query_one(Cache, Sql, Arguments, Default, Option) -> safe_one(query_fields(Cache, Sql, Arguments, Option), Default).

execute(Cache, Sql) -> query_one(Cache, Sql, [], 0).
execute(Cache, Sql, Arguments) -> query_one(Cache, Sql, Arguments, 0).
execute(Cache, Sql, Arguments, Option) -> query_one(Cache, Sql, Arguments, 0, Option).

safe_row([]) -> [];
safe_row([Row|_]) -> Row.

safe_one([[H|_]|_], _) -> H;
safe_one(_, Default) -> Default.

%%----UPSERT API ----------------------------------------------------------
upsert(Cache, Sql, Arguments, Key, Value) -> upsert(Cache, Sql, Arguments, Key, Value, #{}).
upsert(Cache, Sql, Arguments, Key, Value, Option) ->
    case execute(Cache, Sql, Arguments, Option) of
        0 -> 
            case put_if_absent(Cache, Key, Value) of
                false -> execute(Cache, Sql, Arguments, Option);
                _ -> 1
            end;
        _ -> 1
    end.

%%----Batch SQL API ----------------------------------------------------------
batch_query(Cache, Table, Sql, BatchCall, Arguments, Option) ->
    {Async, SqlOptions, WO, RO} = utils:parse_sql_options(Option),
    case Async of
        undefined -> inner_batch_query(Cache, Table, Sql, BatchCall, Arguments, SqlOptions, WO, RO);
        _ -> erlang:spawn(fun() -> 
                                  inner_batch_query(Cache, Table, Sql, BatchCall, Arguments, SqlOptions, WO, RO),
                                  do_async_callback(Async, ok)
                          end)
    end.

batch_query_fields(Cache, Sql, BatchCall, Arguments, Option) ->
    {Async, SqlOptions, WO, RO} = utils:parse_sql_options(Option),
    case Async of
        undefined -> inner_batch_query_fields(Cache, Sql, BatchCall, Arguments, SqlOptions, WO, RO);
        _ ->
            erlang:spawn(fun() -> 
                                 inner_batch_query_fields(Cache, Sql, BatchCall, Arguments, SqlOptions, WO, RO), 
                                 do_async_callback(Async, ok)
                         end)
    end.

%%----Binary Meta API ----------------------------------------------------------
get_name(Platform, HashCode) -> get_name(Platform, HashCode, #{}).
get_name(Platform, HashCode, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_binary_query:?FUNCTION_NAME(Platform, HashCode),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

register_name(Platform, HashCode, Name) -> register_name(Platform, HashCode, Name, #{}).
register_name(Platform, HashCode, Name, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_binary_query:?FUNCTION_NAME(Platform, HashCode, Name, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_type(HashCode) -> get_type(HashCode, #{}).
get_type(HashCode, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_binary_query:?FUNCTION_NAME(HashCode),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

put_type(Type) -> put_type(Type, #{}).
put_type(Type, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_binary_query:?FUNCTION_NAME(Type, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

%%----configuration Meta API ----------------------------------------------------------
create_cache(Name) -> create_cache(Name, #{}).
create_cache(Name, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Name, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_or_create_cache(Name) -> get_or_create_cache(Name, #{}).
get_or_create_cache(Name, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Name, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_cache_names() -> get_cache_names(#{}).
get_cache_names(Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_configuration(Cache) -> get_configuration(Cache, #{}).
get_configuration(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

create_with_configuration(Config) -> create_with_configuration(Config, #{}).
create_with_configuration(Cache, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Cache, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

get_or_create_with_configuration(Config) -> get_or_create_with_configuration(Config, #{}).
get_or_create_with_configuration(Cache, Options) ->
    {Async, WO, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Cache, WO),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

destroy_cache(Cache) -> destroy_cache(Cache, #{}).
destroy_cache(Cache, Options) ->
    {Async, _, RO} = utils:parse_query_options(Options),
    Query = ignite_configuration_query:?FUNCTION_NAME(Cache),
    do_query(Query, ?FUNCTION_NAME, Async, RO).

%%---- KV Internal functions-------------------------------------------------------------------
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
    receive {query_result, Ref, Result} -> 
            case Result of
                {on_query_success, Content} ->
                    ignite_op_response_handler:Handler(Content, Option);
                {on_query_failed, ErrorMsg} ->
                    {on_query_failed, Query, ErrorMsg}
            end
    after Timeout -> 
              {on_query_failed, Query, timeout}
    end.

do_async_callback(Fun, Value) when is_function(Fun) ->
    Fun(Value);

do_async_callback({message, Pid, Tag}, Value) ->
    erlang:send(Pid, {Tag, Value});

do_async_callback(ignore, Value) -> Value.

%%---- SQL Internal functions-------------------------------------------------------------------
sql_sync_query(Worker, Query, #read_option{timeout = Timeout}) ->
    Ref = erlang:make_ref(),
    From = self(),
    wpool_process:cast(Worker, {query, From, Ref, Query}),
    receive {query_result, Ref, Result} -> Result
    after Timeout -> {on_query_failed, Query, timeout}
    end.

inner_query(Cache, Table, Sql, Arguments, SqlOptions, WriteOption, ReadOption) ->
    Worker = wpool_pool:random_worker(ignite),
    Query = ignite_sql_query:query(Cache, Table, Sql, Arguments, SqlOptions, WriteOption),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_result{cursor_id = CursorId,
                              pairs = Pairs,
                              has_more = HasMore} = ignite_op_response_handler:query(Content, ReadOption),
            case HasMore of
                false -> Pairs;
                true -> inner_query_next_page(Worker, CursorId, Pairs, ReadOption)
            end;
        Error -> Error
    end.

inner_query_next_page(Worker, CursorId, Acc, ReadOption) ->
    Query = ignite_sql_query:query_next_page(CursorId),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_result{pairs = Pairs,
                              has_more = HasMore} = ignite_op_response_handler:query_next_page(Content, ReadOption),
            Acc2 = Acc ++ Pairs,
            case HasMore of
                false -> Acc2;
                true -> inner_query_next_page(Worker, CursorId, Acc2, ReadOption)
            end;
        Error -> Error
    end.

inner_query_fields(Cache, Sql, Arguments, SqlOptions, WriteOption, ReadOption) ->
    Worker = wpool_pool:random_worker(ignite),
    {HasNames, Query} = ignite_sql_query:query_fields(Cache, Sql, Arguments, SqlOptions, WriteOption),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_fields_result{cursor_id = CursorId,
                                     column = Column,
                                     names = Names,
                                     rows = Rows,
                                     has_more = HasMore} = ignite_op_response_handler:query_fields(HasNames, Content, ReadOption),
            case HasMore of
                false -> make_query_fields_result(Names, Rows);
                true -> inner_query_fields_next_page(Worker, CursorId, Column, Names, Rows, ReadOption)
            end
    end.

inner_query_fields_next_page(Worker, CursorId, Column, Names, Acc, ReadOption) ->
    Query = ignite_sql_query:query_fields_next_page(CursorId),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_fields_result{rows = Rows,
                                     has_more = HasMore} = ignite_op_response_handler:query_fields_next_page(Column, Content, ReadOption),
            Acc2 = Acc ++ Rows,
            case HasMore of
                false -> make_query_fields_result(Names, Acc2);
                true -> inner_query_fields_next_page(Worker, CursorId, Column, Names, Acc2, ReadOption)
            end;
        Error -> Error
    end.

make_query_fields_result([], Values) -> Values;
make_query_fields_result(Names, Values) -> {Names, Values}.

%%---- Batch SQL Internal functions-------------------------------------------------------------------
inner_batch_query(Cache, Table, Sql, BatchCall, Arguments, SqlOptions, WriteOption, ReadOption) ->
    Worker = wpool_pool:random_worker(ignite),
    Query = ignite_sql_query:query(Cache, Table, Sql, Arguments, SqlOptions, WriteOption),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_result{cursor_id = CursorId,
                              pairs = Pairs,
                              has_more = HasMore} = ignite_op_response_handler:query(Content, ReadOption),
            BatchCall(Pairs),
            case HasMore of
                false -> ok;
                true -> inner_batch_query_next_page(Worker, CursorId, BatchCall, ReadOption)
            end;
        Error -> Error
    end.

inner_batch_query_next_page(Worker, CursorId, BatchCall, ReadOption) ->
    Query = ignite_sql_query:query_next_page(CursorId),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_result{pairs = Pairs,
                              has_more = HasMore} = ignite_op_response_handler:query_next_page(Content, ReadOption),
            BatchCall(Pairs),
            case HasMore of
                false -> ok;
                true -> inner_batch_query_next_page(Worker, CursorId, BatchCall, ReadOption)
            end;
        Error -> Error
    end.

inner_batch_query_fields(Cache, Sql, BatchCall, Arguments, SqlOptions, WriteOption, ReadOption) ->
    Worker = wpool_pool:random_worker(ignite),
    {HasNames, Query} = ignite_sql_query:query_fields(Cache, Sql, Arguments, SqlOptions, WriteOption),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_fields_result{cursor_id = CursorId,
                                     column = Column,
                                     names = Names,
                                     rows = Rows,
                                     has_more = HasMore} = ignite_op_response_handler:query_fields(HasNames, Content, ReadOption),
            BatchCall(Names, Rows),
            case HasMore of
                false -> ok;
                true -> inner_batch_query_fields_next_page(Worker, CursorId, Column, Names, BatchCall, ReadOption)
            end
    end.

inner_batch_query_fields_next_page(Worker, CursorId, Column, Names, BatchCall, ReadOption) ->
    Query = ignite_sql_query:query_fields_next_page(CursorId),
    case sql_sync_query(Worker, Query, ReadOption) of
        {on_query_success, Content} ->
            #sql_query_fields_result{rows = Rows,
                                     has_more = HasMore} = ignite_op_response_handler:query_fields_next_page(Column, Content, ReadOption),
            BatchCall(Names, Rows),
            case HasMore of
                false -> ok;
                true -> inner_batch_query_fields_next_page(Worker, CursorId, Column, Names, BatchCall, ReadOption)
            end;
        Error -> Error
    end.

