-module(ignite_op_response_handler).

-export([on_response/3]).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-spec on_response(non_neg_integer(), term(), binary()) -> term().

%%----KV Response-------------------------------------------------------------------
on_response(?OP_CACHE_GET, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_ALL, _, <<Len:?sint_spec, Body/binary>>) -> 
    {ValueR, _} = loop:do_times(fun({PairAcc, BinAcc}) ->
                                        {Key, BinAcc2} = ignite_decoder:read(BinAcc),
                                        {Value, BinAcc3} = ignite_decoder:read(BinAcc2),
                                        {[{Key, Value} | PairAcc], BinAcc3}
                                end,
                                Len,
                                {[], Body}),
    lists:reverse(ValueR);

on_response(?OP_CACHE_PUT, _, _) -> ok;
on_response(?OP_CACHE_PUT_ALL, _, _) -> ok;
on_response(?OP_CACHE_CONTAINS_KEY, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_CONTAINS_KEYS, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_AND_PUT, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_AND_REPLACE, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_AND_REMOVE, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_PUT_IF_ABSENT, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_AND_PUT_IF_ABSENT, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_REPLACE, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_REPLACE_IF_EQUALS, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_CLEAR, _, _) -> ok;
on_response(?OP_CACHE_CLEAR_KEY, _, _) -> ok;
on_response(?OP_CACHE_CLEAR_KEYS, _, _) -> ok;
on_response(?OP_CACHE_REMOVE_KEY, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_REMOVE_IF_EQUALS, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_GET_SIZE, _, Content) -> 
    ignite_decoder:read_value(Content);

on_response(?OP_CACHE_REMOVE_KEYS, _, _) -> ok;
on_response(?OP_CACHE_REMOVE_ALL, _, _) -> ok;

%%----SQL Response-------------------------------------------------------------------
on_response(?OP_QUERY_SQL, _, <<CursorId:?slong_spec, Row:?sint_spec, Bin/binary>>) ->  
    handle_sql_query_response(CursorId, Row, Bin);

on_response(?OP_QUERY_SQL_CURSOR_GET_PAGE, CursorId, <<Row:?sint_spec, Bin/binary>>) ->  
    handle_sql_query_response(CursorId, Row, Bin);

on_response(?OP_QUERY_SQL_FIELDS, HasNames, <<CursorId:?slong_spec, Column:?sint_spec, Bin/binary>>) ->  
    if HasNames ->
           {NameR, Bin2} = loop:dotimes(fun({NameAcc, DataAcc}) ->
                                                  {Name, DataAcc2} = ignite_decoder:read(DataAcc),
                                                  {[Name | NameAcc], DataAcc2}
                                          end,
                                          Column,
                                          {[], Bin}),
           Names = lists:reverse(NameR);
       true ->
            Names = [],
            Bin2 = Bin
    end,

    <<RowNum:?sint_spec, Bin3/binary>> = Bin2,
    {Rows, <<RawHasMore:?sbyte_spec, _/binary>>} = read_field_grid(RowNum, Column, Bin3),

    {sql_query_fields_result,
     CursorId,
     Names,
     Rows,
     utils:from_raw_bool(RawHasMore)};

on_response(?OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE, {CursorId, Names, Column}, <<RowNum:?sint_spec, Bin/binary>>) ->  
    {Rows, <<RawHasMore:?sbyte_spec, _/binary>>} = read_field_grid(RowNum, Column, Bin),
    {sql_query_fields_result,
     CursorId,
     Names,
     Rows,
     utils:from_raw_bool(RawHasMore)};

on_response(?OP_GET_BINARY_TYPE_NAME, _, Bin) ->
    ignite_decoder:read_value(Bin);

on_response(?OP_REGISTER_BINARY_TYPE_NAME, _, _) -> ok;

on_response(_, _, _) -> ok.

handle_sql_query_response(CursorId, Row, Bin) ->
    {Pairs, <<HasMore:?sbyte_spec, _/binary>>} = 
    loop:dotimes(fun({PairAcc, BinAcc}) -> 
                         {K, BinAcc2} = ignite_decoder:read(BinAcc),
                         {V, BinAcc3} = ignite_decoder:read(BinAcc2),
                         Pair = {K, V},
                         {[Pair | PairAcc], BinAcc3}
                 end,
                 Row,
                 {[], Bin}),
    {sql_query_result, 
     CursorId, 
     Pairs,
     utils:from_raw_bool(HasMore)
    }.

read_field_grid(Row, Column, Bin) ->
    loop:dotimes(fun({RowAcc, DataAcc}) ->
                         {ColumnR, DataAcc2} = loop:dotimes(fun({ColumnAcc, InDataAcc}) ->
                                                                    {ColumnValue, InDataAcc2} = ignite_decoder:read(InDataAcc),
                                                                    {[ColumnValue | ColumnAcc] , InDataAcc2}
                                                            end,
                                                            Column,
                                                            {[], DataAcc}),
                         Columns = lists:reverse(ColumnR),
                         {[Columns | RowAcc], DataAcc2}
                 end,
                 Row,
                 {[], Bin}).


