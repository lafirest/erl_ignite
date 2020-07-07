-module(ignite_kv_query).
-export([get/2, 
         get_all/2,
         put/3,
         on_response/2]).

-include("type_binary_spec.hrl").

-define(OP_CACHE_GET, 1000).
-define(OP_CACHE_PUT, 1001).
-define(OP_CACHE_PUT_IF_ABSENT, 1002).
-define(OP_CACHE_GET_ALL, 1003).
-define(OP_CACHE_PUT_ALL, 1004).
-define(OP_CACHE_GET_AND_PUT, 1005).
-define(OP_CACHE_GET_AND_REPLACE, 1006).
-define(OP_CACHE_GET_AND_REMOVE, 1007).
-define(OP_CACHE_GET_AND_PUT_IF_ABSENT, 1008).
-define(OP_CACHE_REPLACE, 1009).
-define(OP_CACHE_REPLACE_IF_EQUALS, 1010).
-define(OP_CACHE_CONTAINS_KEY, 1011).
-define(OP_CACHE_CONTAINS_KEYS, 1012).
-define(OP_CACHE_CLEAR, 1013).
-define(OP_CACHE_CLEAR_KEY, 1014).
-define(OP_CACHE_CLEAR_KEYS, 1015).
-define(OP_CACHE_REMOVE_KEY, 1016).
-define(OP_CACHE_REMOVE_IF_EQUALS, 1017).
-define(OP_CACHE_REMOVE_KEYS, 1018).
-define(OP_CACHE_REMOVE_ALL, 1019).
-define(OP_CACHE_GET_SIZE, 1020).

get(Cache, Key) ->
    Content = ignite_encoder:write(Key, <<(get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>),
    {ignite_kv_query, ?OP_CACHE_GET, Content}.

get_all(Cache, Keys) ->
    Len = erlang:length(Keys),
    Content = <<(get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>,
    Content2 = 
    lists:foldl(fun(Key, ContentAcc) -> ignite_encoder:write(Key, ContentAcc) end, 
                Content, 
                Keys),
    {ignite_kv_query, ?OP_CACHE_GET_ALL, Content2}.

put(Cache, Key, Value) ->
    Content = <<(get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content),
    Content3 = ignite_encoder:write(Value, Content2),
    {ignite_kv_query, ?OP_CACHE_PUT, Content3}.

on_response(?OP_CACHE_GET, Content) -> ignite_decoder:read_value(Content);
on_response(?OP_CACHE_GET_ALL, <<Len:?sint_spec, Body/binary>>) -> 
    {Values, _} =
    do_times(Len,
            fun({PairAcc, BinAcc}) ->
                    {Key, BinAcc2} = ignite_decoder:read(BinAcc),
                    {Value, BinAcc3} = ignite_decoder:read(BinAcc2),
                    {[{Key, Value} | PairAcc], BinAcc3}
            end,
            {[], Body}),
    Values;
on_response(?OP_CACHE_PUT, _) -> ok.

get_cache_id(CacheId) when is_integer(CacheId) -> CacheId;
get_cache_id(CacheName) -> utils:hash_data(CacheName).

do_times(0, _, Acc) -> Acc;
do_times(N, Fun, Acc) ->
    do_times(N - 1, Fun, Fun(Acc)).
