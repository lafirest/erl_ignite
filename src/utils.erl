-module(utils).

-export([jhash/1]).

jhash(Bin) when is_binary(Bin) ->
    Len = erlang:byte_size(Bin),
    jhash_bin(Bin, 0, Len, 0);

jhash(Str) ->
    jhash_str(Str, 0).

jhash_bin(_, Len, Len, Acc) -> check_result(Acc);
jhash_bin(Bin, Pos, Len, Acc) ->
    Byte = binary:at(Bin, Pos),
    Acc2 = 31 * Acc + Byte,
    jhash_bin(Bin, Pos + 1, Len, check_size(Acc2)).

jhash_str([], Acc) -> check_result(Acc);

jhash_str([H|T], Acc) ->
    Acc2 = 31 * Acc + H,
    jhash_str(T, check_size(Acc2)).

check_size(Value) -> Value band 16#FFFFFFFF.
check_result(Value) ->
    if Value =< 16#7FFFFFFF -> Value;
       true -> bnot (bnot Value band 16#7FFFFFFF)
    end.



