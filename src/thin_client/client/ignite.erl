-module(ignite).

%%----Sync API ----------------------------------------------------------
get(Cache, Key) ->
    Ref = erlang:make_ref(),
    From = self(),
    Query = ignite_ke_query:get(Cache, Key),
    wpool:cast(ignite, {query, From, Ref, Query}, random_worker),
    receive {on_query_response, Ref, Result} ->
                xxx;
    after xxx -> ok
    end.
