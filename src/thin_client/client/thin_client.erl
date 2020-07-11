%%%-------------------------------------------------------------------
%%% @author firest
%%% @copyright (C) 2020, firest
%%% @doc
%%%
%%% @end
%%% Created : 2020-07-07 11:39:06.050255
%%%-------------------------------------------------------------------
-module(thin_client).

-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-compile(export_all).

-include("schema.hrl").
-include("type_binary_spec.hrl").
-define(SERVER, ?MODULE).

-type request_id() :: non_neg_integer().

-record(request,
        {op_code :: request_id(),
         ref :: reference(),
         from :: pid(),
         state :: term()}).

-type requests() :: #{request_id() => #request{}}.

-record(client, 
        {alloc_id :: request_id(),
         requests :: requests(),
         socket :: gen_tcp:socket()
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, 
                          [Args], 
                          []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init({Host, Port, {Major, Minor, Patch}, Username, Password}) ->
    erlang:process_flag(trap_exit, true),
    case gen_tcp:connect(Host, Port, [inet, {active, false}, binary, {nodelay, true}, {keepalive, true}]) of
        {ok, Socket} ->
            HandShake = ignite_connection:hand_shake(Major, Minor, Patch, Username, Password),
            ok = gen_tcp:send(Socket, HandShake),
            case gen_tcp:recv(Socket, 0) of
                {ok, Packet} ->
                    case ignite_connection:on_response(Packet) of
                        ok ->
                            {ok, Recver} = thin_client_recver:start_link(self(), Socket),
                            erlang:monitor(process, Recver),
                            gen_tcp:controlling_process(Socket, Recver),
                            erlang:send(Recver, recv),
                            {ok, #client{alloc_id = 1,
                                         requests = #{},
                                         socket = Socket}};
                        Reason ->
                            {stop, io_lib:format("hand shake failed, Reason:~p~n", [Reason])}
                    end;
                {error, Reason} ->
                    {stop, io_lib:format("hand shake failed, Reason:~p~n", [Reason])}
            end;
        {error, Reason} ->
            {stop, io_lib:format("connect to ~p:~p failed, Reason~p~n", [Host, Port, Reason])}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    try
        do_handle_cast(Msg, State)
    catch Type:Reason:Trace ->
            logger:error("handle cast:~p error~ntype:~p~nreason:~p~ntace:~p~n", [Msg, Type, Reason, Trace]),
              {noreply, State}
    end.

do_handle_cast({query, From, Ref, {Op, State, Content}}, 
            #client{alloc_id = AllocId,
                    requests = Requests,
                    socket = Socket} = Client) ->
    ReqData = ignite_query:make_request(AllocId, Op, Content),
    Request = #request{op_code = Op, 
                       ref = Ref,
                       from = From,
                       state = State},
    ok = gen_tcp:send(Socket, ReqData),
    {noreply, Client#client{alloc_id = AllocId + 1,
                            requests = Requests#{AllocId => Request}}};

do_handle_cast(Msg, State) ->
    logger:error("un handle cast:~p~n", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
    try
        do_handle_info(Msg, State)
    catch Type:Reason:Trace ->
            logger:error("handle info:~p error~ntype:~p~nreason:~p~ntace:~p~n", [Msg, Type, Reason, Trace]),
              {noreply, State}
    end.

do_handle_info({tcp, Data}, #client{requests = Requests} = Client) ->
    {Status, ReqId, Content} = ignite_query:on_response(Data),
    case maps:get(ReqId, Requests, undefined) of
        undefined ->
            {noreply, Client};
        #request{ref = Ref, from = From, op_code = OpCdoe, state = State} ->
            case Status of
                on_query_success ->
                    try
                        Value = ignite_op_response_handler:on_response(OpCdoe, State, Content),
                        erlang:send(From, {on_query_success, Ref, Value})
                    catch
                        Type:Reason ->
                            logger:error("parse error:~p ~p~n", [Type, Reason])
                    end,
                    {noreply, Client#client{requests = maps:remove(ReqId, Requests)}};
                _ ->
                    {noreply, Client}
            end
    end;

do_handle_info({'DOWN', _, process, _, Reason}, State) ->
    logger:error("recver down, this client will auto close~nReason:~p~n", [Reason]),
    {stop, {recver_down, Reason}, State};

do_handle_info(Info, State) ->
    logger:error("un handle info:~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%%===================================================================
%%% API functions
%%%===================================================================
get(Cache, Key) ->
    Ref = erlang:make_ref(),
    From = self(),
    Query = ignite_kv_query:get(Cache, Key),
    wpool:cast(ignite, {query, From, Ref, Query}, random_worker),
    receive {on_query_success, Ref, Value} ->
        Value
    after 10000 -> ok
    end.

put(Cache, Key, Value) ->
    Ref = erlang:make_ref(),
    From = self(),
    Query = ignite_kv_query:put(Cache, Key, Value),
    gen_server:cast(thin_client, {query, From, Ref, Query}),
    receive {on_query_success, Ref, _} ->
                done
    after 10000 -> ok
    end.

query(Cache, Table, Sql, Arg, Opt) ->
    Ref = erlang:make_ref(),
    From = self(),
    Query = ignite_sql_query:query_fields(Cache, Table, Sql, Arg, Opt),
    gen_server:cast(thin_client, {query, From, Ref, Query}),
    receive {on_query_success, Ref, Result} ->
                Result
    after 10000 -> ok
    end.

query_next_page(Id) ->
    Ref = erlang:make_ref(),
    From = self(),
    Query = ignite_sql_query:query_fields_next_page(Id, [], 1),
    gen_server:cast(thin_client, {query, From, Ref, Query}),
    receive {on_query_success, Ref, Result} ->
                Result
    after 10000 -> ok
    end.

register_type() ->
    Player = #type_register{type_name = "Player",
                            type_type = tuple,
                            type_tag = player,
                            version = 1,
                            schema_format = compact,
                            fields = [
                                      #field{name = "Age", type = short},
                                      #field{name = "Sex", type = bool},
                                      #field{name = "Level", type = short},
                                      #field{name = "Gold", type = int}
                                     ],
                            on_upgrades = []},
    Monster = #type_register{type_name = "Monster",
                             type_type = map,
                             type_tag = undefined,
                             version = 1,
                             schema_format = compact,
                             fields = [
                                       #field{name = "Attack", type = int, key = atk},
                                       #field{name = "Defience", type = int, key = def},
                                       #field{name = "Reward", type = int, key = reward},
                                       #field{name = "City", type = {complex_object, "City"}, key = city}
                                      ],
                             on_upgrades = []},
    City = #type_register{type_name = "City",
                          type_type = map,
                          type_tag = undefined,
                          version = 1,
                          schema_format = compact,
                          fields = [
                                    #field{name = "Name", type = bin_string, key = name}
                                   ],
                          on_upgrades = []},

    Person = #type_register{type_name = "org.apache.ignite.examples.Person",
                            type_type = tuple,
                            type_tag = person,
                            version = 1,
                            schema_format = compact,
                            fields = [
                                      #field{name = "id", type = long},
                                      #field{name = "orgId", type = long},
                                      #field{name = "firstName", type = string},
                                      #field{name = "lastName", type = string},
                                      #field{name = "resume", type = string},
                                      #field{name = "salary", type = double}
                                     ],
                            on_upgrades = []},

    schema_manager:register_type(Player),
    schema_manager:register_type(City),
    schema_manager:register_type(Person),
    schema_manager:register_type(Monster).
