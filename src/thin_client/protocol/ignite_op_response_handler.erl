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
         query/2,
         query_next_page/2,
         query_fields/3,
         query_fields_next_page/3,
         query_scan/2,
         query_scan_next_page/2,
         get_name/2,
         register_name/2,
         get_type/2,
         put_type/2,
         create_cache/2,
         get_or_create_cache/2,
         get_cache_names/2,
         get_configuration/2,
         create_with_configuration/2,
         get_or_create_with_configuration/2,
         destroy_cache/2]).

-include("operation.hrl").
-include("type_spec.hrl").
-include("type_binary_spec.hrl").

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
query(<<CursorId:?slong_spec, Row:?sint_spec, Bin/binary>>, ReadOption) ->  
    handle_sql_query_response(CursorId, Row, Bin, ReadOption).

query_next_page(<<Row:?sint_spec, Bin/binary>>, ReadOption) ->  
    handle_sql_query_response(0, Row, Bin, ReadOption).

query_fields(HasNames, <<CursorId:?slong_spec, Column:?sint_spec, Bin/binary>>, ReadOption) ->  
    if HasNames ->
           {NameR, Bin2} = loop:dotimes(fun({NameAcc, DataAcc}) ->
                                                {Name, DataAcc2} = ignite_decoder:read(DataAcc, ReadOption),
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
    {Rows, <<RawHasMore:?sbyte_spec, _/binary>>} = read_field_grid(RowNum, Column, Bin3, ReadOption),

    #sql_query_fields_result
    {cursor_id = CursorId,
     column = Column,
     names = Names,
     rows = Rows,
     has_more = utils:from_raw_bool(RawHasMore)}.

query_fields_next_page(Column, <<RowNum:?sint_spec, Bin/binary>>, ReadOption) ->  
    {Rows, <<RawHasMore:?sbyte_spec, _/binary>>} = read_field_grid(RowNum, Column, Bin, ReadOption),
    #sql_query_fields_result
    {rows = Rows,
     has_more = utils:from_raw_bool(RawHasMore)}.

query_scan(<<CursorId:?slong_spec, Row:?sint_spec, Bin/binary>>, ReadOption) ->  
    handle_sql_scan_response(CursorId, Row, Bin, ReadOption).

query_scan_next_page(<<CursorId:?slong_spec, Row:?slong_spec, Bin/binary>>, ReadOption) ->  
    handle_sql_scan_response(CursorId, Row, Bin, ReadOption).

get_name(Bin, ReadOption) -> ignite_decoder:read_value(Bin, ReadOption).

register_name(_, _) -> ok.

get_type(Content, ReadOption) -> schema:read(Content, ReadOption).

put_type(_, _) -> ok.

create_cache(_, _) -> ok.

get_or_create_cache(_, _) -> ok.

get_cache_names(<<Len:?sint_spec, Content/binary>>, Option) -> 
    erlang:element(1, 
                   loop:dotimes(fun({NameAcc, DataAcc}) -> 
                                        {Name, DataAcc2} = ignite_decoder:read(DataAcc, Option),
                                        {[Name | NameAcc], DataAcc2}
                                end,
                                Len,
                                {[], Content})).

get_configuration(<<_:?sint_spec, Bin/binary>>, Option) -> 
    configuration:read(Bin, Option).

create_with_configuration(_, _) -> ok.
get_or_create_with_configuration(_, _) -> ok.
destroy_cache(_, _) -> ok.

handle_sql_query_response(CursorId, Row, Bin, ReadOption) ->
    {Pairs, <<HasMore:?sbyte_spec, _/binary>>} = 
    loop:dotimes(fun({PairAcc, BinAcc}) -> 
                         {K, BinAcc2} = ignite_decoder:read(BinAcc, ReadOption),
                         {V, BinAcc3} = ignite_decoder:read(BinAcc2, ReadOption),
                         Pair = {K, V},
                         {[Pair | PairAcc], BinAcc3}
                 end,
                 Row,
                 {[], Bin}),
    #sql_query_result{cursor_id = CursorId, 
                      pairs = lists:reverse(Pairs),
                      has_more = utils:from_raw_bool(HasMore)}.

handle_sql_scan_response(CursorId, Row, Bin, ReadOption) ->
    {Pairs, <<HasMore:?sbyte_spec, _/binary>>} = 
    loop:dotimes(fun({PairAcc, BinAcc}) -> 
                         {K, BinAcc2} = ignite_decoder:read(BinAcc, ReadOption),
                         {V, BinAcc3} = ignite_decoder:read(BinAcc2, ReadOption),
                         Pair = {K, V},
                         {[Pair | PairAcc], BinAcc3}
                 end,
                 Row,
                 {[], Bin}),
    #sql_query_result{cursor_id = CursorId, 
                      pairs = lists:reverse(Pairs),
                      has_more = utils:from_raw_bool(HasMore)}.

read_field_grid(Row, Column, Bin, ReadOption) ->
    {RowR, Bin2} = 
    loop:dotimes(fun({RowAcc, DataAcc}) ->
                         {ColumnR, DataAcc2} = 
                         loop:dotimes(fun({ColumnAcc, InDataAcc}) ->
                                              {ColumnValue, InDataAcc2} = ignite_decoder:read(InDataAcc, ReadOption),
                                              {[ColumnValue | ColumnAcc] , InDataAcc2}
                                      end,
                                      Column,
                                      {[], DataAcc}),
                         Columns = lists:reverse(ColumnR),
                         {[Columns | RowAcc], DataAcc2}
                 end,
                 Row,
                 {[], Bin}),
    {lists:reverse(RowR), Bin2}.
