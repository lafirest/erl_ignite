-module(ignite_kv_query).
-export([get/2, 
         get_all/2,
         put/3,
         put_all/2,
         contains_key/2,
         contains_keys/2,
         get_and_put/3,
         get_and_replace/3,
         get_and_remove/2,
         put_if_absent/3,
         get_put_if_absent/3,
         replace/3,
         replace_if_equals/4,
         clear/1,
         clear_key/2,
         clear_keys/2,
         remove_key/2,
         remove_if_equals/4,
         get_size/2,
         remove_keys/2,
         remove_all/1]).

-include("type_spec.hrl").
-include("operation.hrl").
-include("type_binary_spec.hrl").

get(Cache, Key) ->
    {?OP_CACHE_GET, undefined, write_key(Cache, Key)}.

get_all(Cache, Keys) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>,
    Content2 = 
    lists:foldl(fun(Key, ContentAcc) -> ignite_encoder:write(Key, ContentAcc) end, 
                Content, 
                Keys),
    {?OP_CACHE_GET_ALL, undefined, Content2}.

put(Cache, Key, Value) ->
    {?OP_CACHE_PUT, undefined, write_key_value(Cache, Key, Value)}.

put_all(Cache, Pairs) ->
    Len = erlang:length(Pairs),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun({K, V}, DataAcc) ->
                                   DataAcc2 = ignite_encoder:write(K, DataAcc),
                                   ignite_encoder:write(V, DataAcc2)
                           end, 
                           Content,
                           Pairs),
    {?OP_CACHE_PUT_ALL, undefined, Content2}.

contains_key(Cache, Key) ->
    {?OP_CACHE_CONTAINS_KEY, undefined, write_key(Cache, Key)}.

contains_keys(Cache, Keys) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc) end,
                           Content,
                           Keys),
    {?OP_CACHE_CONTAINS_KEYS, undefined, Content2}.

get_and_put(Cache, Key, Value) ->
    {?OP_CACHE_GET_AND_PUT, undefined, write_key_value(Cache, Key, Value)}.

get_and_replace(Cache, Key, Value) ->
    {?OP_CACHE_GET_AND_REPLACE, undefined, write_key_value(Cache, Key, Value)}.

get_and_remove(Cache, Key) ->
    {?OP_CACHE_GET_AND_REMOVE, undefined, write_key(Cache, Key)}.

put_if_absent(Cache, Key, Value) ->
    {?OP_CACHE_PUT_IF_ABSENT, undefined, write_key_value(Cache, Key, Value)}.

get_put_if_absent(Cache, Key, Value) ->
    {?OP_CACHE_GET_AND_PUT_IF_ABSENT, undefined, write_key_value(Cache, Key, Value)}.

replace(Cache, Key, Value) ->
    {?OP_CACHE_REPLACE, undefined, write_key_value(Cache, Key, Value)}.

replace_if_equals(Cache, Key, Compare, Value) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content),
    Content3 = ignite_encoder:write(Compare, Content2),
    Content4 = ignite_encoder:write(Value, Content3),
    {?OP_CACHE_REPLACE_IF_EQUALS, undefined, Content4}.

clear(Cache) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    {?OP_CACHE_CLEAR, undefined, Content}.

clear_key(Cache, Key) ->
    {?OP_CACHE_CLEAR_KEY, undefined, write_key(Cache, Key)}.

clear_keys(Cache, Keys) ->
    Len = elrang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc) end,
                           Content,
                           Keys),
    {?OP_CACHE_CLEAR_KEYS, undefined, Content2}.

get_size(Cache, Mode) ->
    ModeValue = case Mode of
                    all -> ?CACHEPEEKMODE_ALL;
                    near -> ?CACHEPEEKMODE_NEAR;
                    primary -> ?CACHEPEEKMODE_PRIMARY;
                    backup -> ?CACHEPEEKMODE_BACKUP;
                    onheap -> ?CACHEPEEKMODE_ONHEAP;
                    offheap -> ?CACHEPEEKMODE_OFFHEAP
                end,
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, ModeValue:?sint_spec, ModeValue:?sbyte_spec>>, 
    {?OP_CACHE_GET_SIZE, undefined, Content}.

remove_key(Cache, Key) ->
    {?OP_CACHE_REMOVE_KEY, undefined, write_key(Cache, Key)}.

remove_if_equals(Cache, Key, Compare, Value) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content),
    Content3 = ignite_encoder:write(Compare, Content2),
    Content4 = ignite_encoder:write(Value, Content3),
    {?OP_CACHE_REMOVE_IF_EQUALS, undefined, Content4}.

remove_keys(Cache, Keys) ->
    Len = elrang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc) end,
                           Content,
                           Keys),
    {?OP_CACHE_REMOVE_KEYS, undefined, Content2}.

remove_all(Cache) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    {?OP_CACHE_REMOVE_ALL, undefined, Content}.

write_key(Cache, Key) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    ignite_encoder:write(Key, Content).
 
write_key_value(Cache, Key, Value) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content),
    ignite_encoder:write(Value, Content2).
