-module(ignite_sql_query).
-compile({parse_transform, category}).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-define(DEFAULT_PAGE_SIZE, 1024).
-define(DEFAULT_TIMEOUT, 0).
-define(DEFAULT_MAX_ROWS, -1).

-export([query/6,
         query_next_page/1,
         query_fields/5,
         query_fields_next_page/1]).

%% TODO ?? i think i don't need implement OP_QUERY_SCAN and OP_QUERY_SCAN_CURSOR_GET_PAGE

-type query_options() :: join
                    | local
                    | replicated 
                    | page_size 
                    | timeout.

-type statement() :: any
                | select
                | update.

-type fields_query_options() :: query_options()
                    | schema 
                    | max_rows
                    | statement
                    | join_order
                    | collocated 
                    | lazy
                    | names.

query(Cache, Table, Sql, Arguments, OptionsArg, WriteOption) -> 
    Options = maps:from_list(OptionsArg),
    Join = utils:to_raw_bool(maps:get(join, Options, false)),
    Local = utils:to_raw_bool(maps:get(local, Options, false)),
    Replicated = utils:to_raw_bool(maps:get(replicated, Options, false)),
    PageSize = maps:get(page_size, Options, ?DEFAULT_PAGE_SIZE),
    Timeout = maps:get(timeout, Options, ?DEFAULT_TIMEOUT),
    ArgLen = erlang:length(Arguments),
    Content = 
    [identity ||
        ignite_encoder:write({string, Table}, <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, WriteOption),
        ignite_encoder:write({string, Sql}, _, WriteOption),
        lists:foldl(fun(Argument, DataAcc) ->
                        ignite_encoder:write(Argument, DataAcc, WriteOption)
                    end,
                    <<_/binary, ArgLen:?sint_spec>>,
                    Arguments)
    ],
    Content2 = <<Content/binary, 
                 Join:?sbyte_spec, 
                 Local:?sbyte_spec, 
                 Replicated:?sbyte_spec, 
                 PageSize:?sint_spec,
                 Timeout:?slong_spec>>,
    {?OP_QUERY_SQL, Content2}.

query_next_page(CursorId) -> 
    {?OP_QUERY_SQL_CURSOR_GET_PAGE, <<CursorId:?slong_spec>>}.

query_fields(Cache, Sql, Arguments, OptionsArg, WriteOption) -> 
    Options = maps:from_list(OptionsArg),
    Join = utils:to_raw_bool(maps:get(join, Options, false)),
    Local = utils:to_raw_bool(maps:get(local, Options, false)),
    Replicated = utils:to_raw_bool(maps:get(replicated, Options, false)),
    PageSize = maps:get(page_size, Options, ?DEFAULT_PAGE_SIZE),
    Timeout = maps:get(timeout, Options, ?DEFAULT_TIMEOUT),
    Schema = maps:get(schema, Options, undefined),
    MaxRows = maps:get(max_rows, Options, ?DEFAULT_MAX_ROWS),
    Statement = get_raw_statement(maps:get(statement, Options, any)),
    JoinOrder = utils:to_raw_bool(maps:get(join_order, Options, false)),
    Collocated = utils:to_raw_bool(maps:get(collocated, Options, false)),
    Lazy = utils:to_raw_bool(maps:get(lazy, Options, false)),
    Names = maps:get(names, Options, false),
    RawNames = utils:to_raw_bool(Names),
    ArgLen = erlang:length(Arguments),
    Content = 
    [identity ||
        ignite_encoder:write(if Schema =:= undefined -> undefined;
                                true -> {string, Schema}
                             end,
                             <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>,
                            WriteOption),
        ignite_encoder:write({string, Sql}, <<_/binary, PageSize:?sint_spec, MaxRows:?sint_spec>>, WriteOption),
        lists:foldl(fun(Argument, DataAcc) ->
                        ignite_encoder:write(Argument, DataAcc, WriteOption)
                    end,
                    <<_/binary, ArgLen:?sint_spec>>,
                    Arguments)],
    Content2 = <<Content/binary,
                 Statement:?sbyte_spec,
                 Join:?sbyte_spec, 
                 Local:?sbyte_spec, 
                 Replicated:?sbyte_spec, 
                 JoinOrder:?sbyte_spec, 
                 Collocated:?sbyte_spec, 
                 Lazy:?sbyte_spec, 
                 Timeout:?slong_spec,
                 RawNames:?sbyte_spec>>,
    {Names, {?OP_QUERY_SQL_FIELDS, Content2}}.

query_fields_next_page(CursorId) -> 
    {?OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE, <<CursorId:?slong_spec>>}.

get_raw_statement(any) -> 0;
get_raw_statement(select) -> 1;
get_raw_statement(update) -> 2.
