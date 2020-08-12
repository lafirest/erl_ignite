%%%-------------------------------------------------------------------
%%% @author firest
%%% @copyright (C) 2020, firest
%%% @doc
%%%
%%% @end
%%% Created : 2020-07-11 17:01:16.101067
%%%-------------------------------------------------------------------
-module(thin_client_recver).

-behaviour(gen_server).

%% API
-export([start_link/2,
         begin_recv_tick/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("schema.hrl").
-include("type_binary_spec.hrl").

-define(SERVER, ?MODULE).
-define(DATALENSIZE, 4).

-record(recver, 
        {parent :: pid(),
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
start_link(Parent, Socket) ->
    gen_server:start_link(?MODULE, [Parent, Socket], []).

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
init([Parent, Socket]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #recver{parent = Parent,
                 socket = Socket,
                 buffer = <<>>}}.

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
handle_cast(_Msg, State) ->
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

do_handle_info(recv, #recver{parent = Parent, socket = Socket, buffer = Buffer} = State) ->
    case socket:recv(Socket) of
        {error, _} = Error ->
            {stop, Error, State};
        {ok, Packet} ->
            tick_recv(),
            Buffer2 = <<Buffer/binary, Packet/binary>>,
            {noreply, State#recver{buffer = dispatch_packate(Parent, Buffer2)}}
    end;

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
tick_recv() -> 
    erlang:send(self(), recv).

dispatch_packate(Parent, Bin) ->
    BufferSize = erlang:byte_size(Bin),
    if BufferSize < ?DATALENSIZE -> Bin;
       true ->
           <<MsgLen:?sint_spec, Body/binary>> = Bin,
           if MsgLen =< BufferSize - ?DATALENSIZE ->
                  <<Msg:MsgLen/binary, BufferRest/binary>> = Body,
                  erlang:send(Parent, {tcp, Msg}),
                  dispatch_packate(Parent, BufferRest);
              true -> Bin
           end
    end.

%%%===================================================================
%%% API functions
%%%===================================================================
begin_recv_tick(Pid) -> 
    erlang:send(Pid, recv).





