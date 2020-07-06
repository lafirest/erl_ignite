-module(ignite_kv_query).
-export([get/2]).

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
    ignite_query:make_request(erlang:byte_size(Content), ?OP_CACHE_GET, Content).

get_all(Cache, Len) ->
    Content = ignite_encoder:write(Len, <<(get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>),
    ignite_query:make_request(erlang:byte_size(Content), ?OP_CACHE_GET_ALL, Content).

get_cache_id(CacheId) when is_integer(CacheId) -> CacheId;
get_cache_id(CacheName) -> utils:hash_data(CacheName).
