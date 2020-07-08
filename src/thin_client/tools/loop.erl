-module(loop).

-export([dotimes/3, while/2]).

-spec dotimes((fun((term()) -> term())),
              non_neg_integer(), 
              term()) -> term().
dotimes(_, 0, Result) -> Result;
dotimes(Func, N, Result) ->
    dotimes(Func, N - 1, Func(Result)).

-spec while((fun((term()) -> {true, term()} | term())),
            term()) -> term().
while(Func, Result) ->
    case Func(Result) of
        {true, Result2} ->
            while(Func, Result2);
        Result2 -> Result2
    end.

