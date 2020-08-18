-module(ignite_kv_query).
-export([get/3, 
         get_all/3,
         put/4,
         put_all/3,
         contains_key/3,
         contains_keys/3,
         get_and_put/4,
         get_and_replace/4,
         get_and_remove/3,
         put_if_absent/4,
         get_put_if_absent/4,
         replace/4,
         replace_if_equals/5,
         clear/1,
         clear_key/3,
         clear_keys/3,
         remove_key/3,
         remove_if_equals/4,
         get_size/2,
         remove_keys/3,
         remove_all/1]).

-include("type_spec.hrl").
-include("operation.hrl").
-include("type_binary_spec.hrl").

get(Cache, Key, Option) ->
    {?OP_CACHE_GET, write_key(Cache, Key, Option)}.

get_all(Cache, Keys, Option) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>,
    Content2 = 
    lists:foldl(fun(Key, ContentAcc) -> ignite_encoder:write(Key, ContentAcc, Option) end, 
                Content, 
                Keys),
    {?OP_CACHE_GET_ALL, Content2}.

put(Cache, Key, Value, Option) ->
    {?OP_CACHE_PUT, write_key_value(Cache, Key, Value, Option)}.

put_all(Cache, Pairs, Option) ->
    Len = erlang:length(Pairs),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun({K, V}, DataAcc) ->
                                   DataAcc2 = ignite_encoder:write(K, DataAcc, Option),
                                   ignite_encoder:write(V, DataAcc2, Option)
                           end, 
                           Content,
                           Pairs),
    {?OP_CACHE_PUT_ALL, Content2}.

contains_key(Cache, Key, Option) ->
    {?OP_CACHE_CONTAINS_KEY, write_key(Cache, Key, Option)}.

contains_keys(Cache, Keys, Option) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc, Option) end,
                           Content,
                           Keys),
    {?OP_CACHE_CONTAINS_KEYS, Content2}.

get_and_put(Cache, Key, Value, Option) ->
    {?OP_CACHE_GET_AND_PUT, write_key_value(Cache, Key, Value, Option)}.

get_and_replace(Cache, Key, Value, Option) ->
    {?OP_CACHE_GET_AND_REPLACE, write_key_value(Cache, Key, Value, Option)}.

get_and_remove(Cache, Key, Option) ->
    {?OP_CACHE_GET_AND_REMOVE, write_key(Cache, Key, Option)}.

put_if_absent(Cache, Key, Value, Option) ->
    {?OP_CACHE_PUT_IF_ABSENT, write_key_value(Cache, Key, Value, Option)}.

get_put_if_absent(Cache, Key, Value, Option) ->
    {?OP_CACHE_GET_AND_PUT_IF_ABSENT,  write_key_value(Cache, Key, Value, Option)}.

replace(Cache, Key, Value, Option) ->
    {?OP_CACHE_REPLACE, write_key_value(Cache, Key, Value, Option)}.

replace_if_equals(Cache, Key, Compare, Value, Option) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content, Option),
    Content3 = ignite_encoder:write(Compare, Content2, Option),
    Content4 = ignite_encoder:write(Value, Content3, Option),
    {?OP_CACHE_REPLACE_IF_EQUALS, Content4}.

clear(Cache) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    {?OP_CACHE_CLEAR, Content}.

clear_key(Cache, Key, Option) ->
    {?OP_CACHE_CLEAR_KEY, write_key(Cache, Key, Option)}.

clear_keys(Cache, Keys, Option) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc, Option) end,
                           Content,
                           Keys),
    {?OP_CACHE_CLEAR_KEYS, Content2}.

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
    {?OP_CACHE_GET_SIZE, Content}.

remove_key(Cache, Key, Option) ->
    {?OP_CACHE_REMOVE_KEY, write_key(Cache, Key, Option)}.

remove_if_equals(Cache, Key, Compare, Option) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content, Option),
    Content3 = ignite_encoder:write(Compare, Content2, Option),
    {?OP_CACHE_REMOVE_IF_EQUALS, Content3}.

remove_keys(Cache, Keys, Option) ->
    Len = erlang:length(Keys),
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec, Len:?sint_spec>>, 
    Content2 = lists:foldl(fun(Key, DataAcc) -> ignite_encoder:write(Key, DataAcc, Option) end,
                           Content,
                           Keys),
    {?OP_CACHE_REMOVE_KEYS, Content2}.

remove_all(Cache) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    {?OP_CACHE_REMOVE_ALL, Content}.

write_key(Cache, Key, Option) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    ignite_encoder:write(Key, Content, Option).
 
write_key_value(Cache, Key, Value, Option) ->
    Content = <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>, 
    Content2 = ignite_encoder:write(Key, Content, Option),
    ignite_encoder:write(Value, Content2, Option).
