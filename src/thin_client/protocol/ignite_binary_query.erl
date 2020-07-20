-module(ignite_binary_query).

-include("operation.hrl").
-include("type_binary_spec.hrl").

-export([get_name/2, register_name/4, get_type/1, put_type/2]).

-type platform() :: java | dotnet.

get_name(Platform, HashCode) ->
    RawPlatform = get_raw_platfomr(Platform),
    {?OP_GET_BINARY_TYPE_NAME, <<RawPlatform:?sbyte_spec, HashCode:?sint_spec>>}.

register_name(Platform, HashCode, Name, Option) ->
    RawPlatform = get_raw_platfomr(Platform),
    Content = ignite_encoder:write({string, Name}, <<RawPlatform:?sbyte_spec, HashCode:?sint_spec>>, Option),
    {?OP_REGISTER_BINARY_TYPE_NAME, Content}.

get_type(HashCode) ->
    {?OP_GET_BINARY_TYPE, <<HashCode:?sint_spec>>}.

put_type(Type, Option) ->
    {?OP_PUT_BINARY_TYPE, schema:write(Type, Option)}.

get_raw_platfomr(java) -> 0;
get_raw_platfomr(dotnet) -> 1.
