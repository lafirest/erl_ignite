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
-export([start_link/7]).

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
         handler :: atom(),
         ref :: reference(),
         from :: pid()}).

-type requests() :: #{request_id() => #request{}}.

-record(client, 
        {alloc_id :: request_id(),
         requests :: requests(),
         socket :: gen_tcp:socket(),
         buffer :: binary()
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
start_link(Host, Port, Major, Minor, Patch, Username, Password) ->
    gen_server:start_link({local, ?SERVER}, 
                          ?MODULE, 
                          [Host, Port, Major, Minor, Patch, Username, Password], 
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
init([Host, Port, Major, Minor, Patch, Username, Password]) ->
    case gen_tcp:connect(Host, Port, [inet, {active, false}, binary, {nodelay, true}, {keepalive, true}]) of
        {ok, Socket} ->
            HandShake = ignite_connection:hand_shake(Major, Minor, Patch, Username, Password),
            ok = gen_tcp:send(Socket, HandShake),
            case gen_tcp:recv(Socket, 0) of
                {ok, Packet} ->
                    io:format("on con :~p~n", [Packet]),
                    case ignite_connection:on_response(Packet) of
                        ok ->
                            erlang:send_after(10, self(), recv),
                            {ok, #client{alloc_id = 1,
                                         requests = #{},
                                         socket = Socket,
                                         buffer = <<>>}};
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
handle_cast({query, From, Ref, {Handler, Op, Content}}, 
            #client{alloc_id = AllocId,
                    requests = Requests,
                    socket = Socket} = State) ->
    logger:error("query ~p ~p ~p~n", [Handler, Op, Content]),
    ReqData = ignite_query:make_request(AllocId, Op, Content),
    Request = #request{op_code = Op, 
                       handler = Handler,
                       ref = Ref,
                       from = From},
    logger:error("send :~p~n", [ReqData]),
    ok = gen_tcp:send(Socket, ReqData),
    {noreply, State#client{alloc_id = AllocId + 1,
                           requests = Requests#{AllocId => Request}}};

handle_cast(Msg, State) ->
    logger:error("un handle message:~p~n", [Msg]),
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
handle_info({tcp, Data}, #client{requests = Requests} = State) ->
    logger:error("receive :~p~n", [Data]),
    {Status, ReqId, Content} = ignite_query:on_response(Data),
    case maps:get(ReqId, Requests, undefined) of
        undefined ->
            {noreply, State};
        #request{handler = Handler, ref = Ref, from = From, op_code = OpCdoe} ->
            case Status of
                on_query_success ->
                    try
                        Value = Handler:on_response(OpCdoe, Content),
                        erlang:send(From, {on_query_success, Ref, Value})
                    catch
                        Type:Reason ->
                            logger:error("parse error:~p ~p~n", [Type, Reason])
                    end,
                    {noreply, State#client{requests = maps:remove(ReqId, Requests)}};
                _ ->
                    {noreply, State}
            end
    end;

handle_info(recv, #client{socket = Socket, buffer = Buffer} = State) ->
    erlang:send_after(10, self(), recv),
    case gen_tcp:recv(Socket, 0, 10) of
        {error, _} ->
            {noreply, State};
        {ok, Packet} ->
            logger:error("recv data:~p~n", [Packet]),
            Buffer2 = <<Buffer/binary, Packet/binary>>,
            Len = erlang:byte_size(Buffer2),
            if Len < 4 ->
                   {noreply, State#client{buffer = Buffer2}};
               true ->
                   <<MsgLen:?sint_spec, Body/binary>> = Buffer2,
                   if MsgLen > Len - 4 ->
                        {noreply, State#client{buffer = Buffer2}};
                      true ->
                          <<Msg:MsgLen/binary, BufferRest/binary>> = Body,
                          erlang:send(self(), {tcp, Msg}),
                          {noreply, State#client{buffer = BufferRest}}
                   end
            end
    end;

handle_info(Info, State) ->
    logger:error("un handle message:~p~n", [Info]),
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
    gen_server:cast(thin_client, {query, From, Ref, Query}),
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

get_name(Cache, Name) ->
    Ref = erlang:make_ref(),
    From = self(),
    Id = utils:hash_name(Name),
    Query = {ignite_kv_query, 3000, <<0:?sbyte_spec, Id:?sint_spec>>},
    gen_server:cast(thin_client, {query, From, Ref, Query}),
    receive {on_query_success, Ref, _} ->
                done
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
