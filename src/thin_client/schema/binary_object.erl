%% TODO now, just for fast field read
-module(binary_object).

-export([read_field/2, read_field/3]).

-include("type_spec.hrl").

read_field(Name, Bin) -> read_field(Name, Bin, []).

read_field(Name, #binary_object{body = Body, schemas = Schemas}, Options) ->
    NameId = utils:hash_name(Name),
    case maps:get(NameId, Schemas, undefined) of
        undefined -> undefined;
        Offset ->
            <<_:Offset/binary, Rest/binary>> = Body,
            ignite_decoder:read_value(Rest, Options)
    end.

