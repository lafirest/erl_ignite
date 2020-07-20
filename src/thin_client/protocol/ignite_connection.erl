-module(ignite_connection).
-export([hand_shake/5, on_response/1]).

-include("type_binary_spec.hrl").
-define(handshake_code, 1).
-define(client_code, 2).

hand_shake(Major, Minor, Patch, Username, Password) ->
    Content = 
        <<?handshake_code:?sbyte_spec, 
          Major:?sshort_spec, 
          Minor:?sshort_spec, 
          Patch:?sshort_spec, 
          ?client_code:?sbyte_spec>>,
    Content1 = ignite_encoder:write({string, Username}, Content),
    Content2 = ignite_encoder:write({string, Password}, Content1),
    TotalLen = erlang:byte_size(Content2),
    <<TotalLen:?sint_spec, Content2/binary>>.

on_response(<<1:?sint_spec, 1:?sbyte_spec>>) -> ok;
on_response(<<_:?sint_spec, 
              Status:?sbyte_spec,
              Major:?sshort_spec,
              Minor:?sshort_spec,
              Patch:?sshort_spec,
              ErrMsg/binary>>) ->
    {false, Status, Major, Minor, Patch, ErrMsg}.
