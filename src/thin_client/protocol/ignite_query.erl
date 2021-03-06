-module(ignite_query).
-export([make_request/3, on_response/1]).

-include("type_binary_spec.hrl").

make_request(ReqId, Op, Content) ->
    Len = erlang:byte_size(Content) + 10,
    <<Len:?sint_spec, Op:?sshort_spec, ReqId:?slong_spec, Content/binary>>.

on_response(<<ReqId:?slong_spec, Status:?sint_spec, Bin/binary>>) ->
    case Status of
        0 -> {on_query_success, ReqId, Bin};
        _ ->
            ErrMsg = ignite_decoder:read_value(Bin),
            {on_query_failed, ReqId,  ErrMsg}
    end.
