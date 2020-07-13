-module(ignite_op_response_handler).

-export([get/2, 
         get_all/2,
         put/2,
         put_all/2,
         contains_key/2,
         contains_keys/2,
         get_and_put/2,
         get_and_replace/2,
         get_and_remove/2,
         put_if_absent/2,
         get_put_if_absent/2,
         replace/2,
         replace_if_equals/2,
         clear/2,
         clear_key/2,
         clear_keys/2,
         get_size/2,
         remove_key/2,
         remove_if_equals/2,
         remove_keys/2,
         remove_all/2,
         on_response/3]).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-spec on_response(non_neg_integer(), term(), binary()) -> term().

%%----KV Response-------------------------------------------------------------------
get(Content, Option) -> ignite_decoder:read_value(Content, Option).

get_all(<<Len:?sint_spec, Body/binary>>, Option) -> 
    {ValueR, _} = loop:dotimes(fun({PairAcc, BinAcc}) ->
                                       {Key, BinAcc2} = ignite_decoder:read(BinAcc, Option),
                                       {Value, BinAcc3} = ignite_decoder:read(BinAcc2, Option),
                                       {[{Key, Value} | PairAcc], BinAcc3}
                                end,
                                Len,
                                {[], Body}),
    lists:reverse(ValueR).

put(_, _) -> ok.
put_all(_, _) -> ok.
contains_key(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

contains_keys(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

get_and_put(Content, Option) ->  ignite_decoder:read_value(Content, Option).

get_and_replace(Content, Option) -> ignite_decoder:read_value(Content, Option).

get_and_remove(Content, Option) -> ignite_decoder:read_value(Content, Option).

put_if_absent(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

get_put_if_absent(Content, Option) -> ignite_decoder:read_value(Content, Option).

replace(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

replace_if_equals(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

clear(_, _) -> ok.
clear_key(_, _) -> ok.
clear_keys(_, _) -> ok.

remove_key(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

remove_if_equals(<<Result:?sbyte_spec>>, _) -> utils:from_raw_bool(Result).

get_size(<<Size:?slong_spec, _/binary>>, _) -> Size.

remove_keys(_, _) -> ok.
remove_all(_, _) -> ok.

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

on_response(?OP_GET_BINARY_TYPE, _, Content) -> schema:read(Content);
on_response(?OP_PUT_BINARY_TYPE, _, _) -> ok;
on_response(?OP_CACHE_CREATE_WITH_NAME, _, _) -> ok;
on_response(?OP_CACHE_GET_OR_CREATE_WITH_NAME, _, _) -> ok;
on_response(?OP_CACHE_GET_NAMES, _, <<Len:?sint_spec, Content/binary>>) -> 
    loop:dotimes(fun({NameAcc, DataAcc}) -> 
                        {Name, DataAcc2} = ignite_decoder:read(DataAcc),
                        {[Name | NameAcc], DataAcc2}
                 end,
                 Len,
                 {[], Content});

on_response(?OP_CACHE_GET_CONFIGURATION, _, <<_:?sint_spec, Bin/binary>>) -> 
    configuration:read(Bin);

on_response(?OP_CACHE_CREATE_WITH_CONFIGURATION, _, _) -> ok;
on_response(?OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION, _, _) -> ok;
on_response(?OP_CACHE_DESTROY, _, _) -> ok;

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
