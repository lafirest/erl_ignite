-module(ignite_query).
-export([make_request/3, on_response/1]).

-include("type_binary_spec.hrl").

make_request(ContentLen, Op, Content) ->
    Len = ContentLen + 10,
    ReqId = erlang:unique_integer(),
    {ReqId, <<Len:?sint_spec, Op:?sshort_spec, ReqId:?slong_spec, Content/binary>>}.

on_response(<<_:?sint_spec, ReqId:?slong_spec, Status:?sint_spec, Bin/binary>>) ->
    case Status of
        0 -> {on_query_success, ReqId, Bin};
        _ ->
            {ErrMsg, _} = ignite_decoder:read(Bin),
            {on_query_failed, ReqId,  ErrMsg}
    end.
