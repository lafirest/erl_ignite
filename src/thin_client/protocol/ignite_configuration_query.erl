-module(ignite_configuration_query).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([create_cache/1, 
         get_or_create_cache/1, 
         get_cache_names/0,
         get_configuration/1,
         destroy_cache/0,
         create_with_configuration/1,
         get_or_create_with_configuration/1]).

create_cache(Name) ->
    {?OP_CACHE_CREATE_WITH_NAME, undefined, ignite_encoder:write({string, Name}, <<>>)}.

get_or_create_cache(Name) ->
    {?OP_CACHE_GET_OR_CREATE_WITH_NAME, undefined, ignite_encoder:write({string, Name}, <<>>)}.

get_cache_names() ->
    {?OP_CACHE_GET_NAMES, undefined, <<>>}.

get_configuration(Cache) ->
    {?OP_CACHE_GET_CONFIGURATION, undefined, <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>}.

create_with_configuration(Configuration) ->
    Bin = configuration:write(Configuration),
    Len = erlang:byte_size(Bin),
    {?OP_CACHE_CREATE_WITH_CONFIGURATION, undefined, <<Len:?sint_spec, Bin/binary>>}.

get_or_create_with_configuration(Configuration) ->
    Bin = configuration:write(Configuration),
    Len = erlang:byte_size(Bin),
    {?OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION, undefined, <<Len:?sint_spec, Bin/binary>>}.

destroy_cache() ->
    {?OP_CACHE_DESTROY, undefined, <<>>}.
