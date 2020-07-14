-module(ignite_configuration_query).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([create_cache/2, 
         get_or_create_cache/2, 
         get_cache_names/0,
         get_configuration/1,
         destroy_cache/1,
         create_with_configuration/1,
         get_or_create_with_configuration/1]).

create_cache(Name, Option) ->
    {?OP_CACHE_CREATE_WITH_NAME, ignite_encoder:write({string, Name}, <<>>, Option)}.

get_or_create_cache(Name, Option) ->
    {?OP_CACHE_GET_OR_CREATE_WITH_NAME, ignite_encoder:write({string, Name}, <<>>, Option)}.

get_cache_names() ->
    {?OP_CACHE_GET_NAMES, <<>>}.

get_configuration(Cache) ->
    {?OP_CACHE_GET_CONFIGURATION, <<(utils:get_cache_id(Cache)):?sint_spec, 0:?sbyte_spec>>}.

create_with_configuration(Configuration) ->
    Bin = configuration:write(Configuration),
    Len = erlang:byte_size(Bin),
    {?OP_CACHE_CREATE_WITH_CONFIGURATION, <<Len:?sint_spec, Bin/binary>>}.

get_or_create_with_configuration(Configuration) ->
    Bin = configuration:write(Configuration),
    Len = erlang:byte_size(Bin),
    {?OP_CACHE_GET_OR_CREATE_WITH_CONFIGURATION, <<Len:?sint_spec, Bin/binary>>}.

destroy_cache(Cache) ->
    {?OP_CACHE_DESTROY, <<(utils:get_cache_id(Cache)):?sint_spec>>}. 
