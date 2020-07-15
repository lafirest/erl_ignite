-module(utils).
-export([hash_name/1, 
         hash_data/1, 
         calculate_schemaId/1,
         get_cache_id/1,
         to_raw_bool/1,
         from_raw_bool/1,
         parse_read_options/1,
         parse_write_options/1,
         parse_query_options/1,
         parse_sql_options/1]).

-include("type_spec.hrl").
-define(FNV1_OFFSET_BASIS, 16#811C9DC5).
-define(FNV1_PRIME, 16#01000193).

hash_name(Bin) when is_binary(Bin) -> hash_code(Bin, fun name_hash/2);
hash_name(Str) -> hash_code(Str, fun name_hash/2).

hash_data(Bin) when is_binary(Bin) -> hash_code(Bin, fun data_hash/2);
hash_data(Str) -> hash_code(Str, fun data_hash/2).

hash_code(Bin, Func) when is_binary(Bin) ->
    Len = erlang:byte_size(Bin),
    hash_code_bin(Bin, 0, Len, Func, 0);
hash_code(Str, Func) -> hash_code_str(Str, Func, 0).

hash_code_bin(_, Len, Len, _, Acc) -> check_result(Acc);
hash_code_bin(Bin, Pos, Len, Func, Acc) ->
    Byte = binary:at(Bin, Pos),
    hash_code_bin(Bin, Pos + 1, Len, Func, check_size(Func(Byte, Acc))).

hash_code_str([], _, Acc) -> check_result(Acc);
hash_code_str([H|T], Func, Acc) ->
    hash_code_str(T, Func, check_size(Func(H, Acc))).

check_size(Value) -> Value band 16#FFFFFFFF.
check_result(Value) ->
    if Value =< 16#7FFFFFFF -> Value;
       true -> Value - 16#100000000
    end.

to_lower(Char) ->
    if Char >= 'A' andalso Char =< 'Z' -> Char bor 16#20;
       true -> Char
    end.

name_hash(Char, Acc) -> 31 * Acc + to_lower(Char).
data_hash(Byte, Acc) -> 31 * Acc + Byte.

calculate_schemaId([]) -> 0;
calculate_schemaId(Fields) ->
    Id = lists:foldl(fun(FieldName, Acc) ->
                             FieldId = utils:hash_name(FieldName),
                             lists:foldl(fun(Shift, IAcc) ->
                                                 IAcc2 = (IAcc bxor ((FieldId bsr Shift) band 16#FF)) band 16#FFFFFFFF,
                                                 (IAcc2 * ?FNV1_PRIME) band 16#FFFFFFFF
                                         end, Acc, [0, 8, 16, 24])
                     end,
                     ?FNV1_OFFSET_BASIS, 
                     Fields),
    if Id =< 16#7FFFFFFF -> Id;
       true -> Id - 16#100000000
    end.

get_cache_id(CacheId) when is_integer(CacheId) -> CacheId;
get_cache_id(CacheName) -> utils:hash_data(CacheName).

to_raw_bool(true) -> 1;
to_raw_bool(false) -> 0.

from_raw_bool(1) -> true;
from_raw_bool(0) -> false.

parse_read_options(Options) -> 
    lists:foldl(fun(E, Acc) -> i_parse_read_options(E, Acc) end, #read_option{}, Options).

i_parse_read_options(fast_term, Option) -> Option#read_option{fast_term = true};
i_parse_read_options(keep_wrapped, Option) -> Option#read_option{keep_wrapped = true};
i_parse_read_options(keep_binary_object, Option) -> Option#read_option{keep_binary_object = true};
i_parse_read_options({timeout, Timeout}, Option) -> Option#read_option{timeout = Timeout}.

parse_write_options(Options) ->
    lists:foldl(fun(E, Acc) -> i_parse_write_options(E, Acc) end, #write_option{}, Options).

i_parse_write_options(fast_term, Option) -> Option#write_option{fast_term = true}.

parse_query_options(Options) -> 
    WriteOptions = maps:get(write, Options, []),
    ReadOptions = maps:get(read, Options, []),
    AsyncCallback = maps:get(async, Options, undefined),
    {AsyncCallback, parse_write_options(WriteOptions), parse_read_options(ReadOptions)}.

parse_sql_options(Options) ->
    WriteOptions = maps:get(write, Options, []),
    ReadOptions = maps:get(read, Options, []),
    AsyncCallback = maps:get(async, Options, undefined),
    SqlOptions = maps:get(sql, Options, []),
    {AsyncCallback, SqlOptions, parse_write_options(WriteOptions), parse_read_options(ReadOptions)}.
