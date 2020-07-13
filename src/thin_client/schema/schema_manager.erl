%%%-------------------------------------------------------------------
%%% @author firest
%%% @copyright (C) 2020, firest
%%% @doc
%%%
%%% @end
%%% Created : 2020-07-07 19:51:38.941884
%%%-------------------------------------------------------------------
-module(schema_manager).

-behaviour(gen_server).

-include("schema.hrl").
-define(SERVER, ?MODULE).

%% API
-export([start_link/0,
         register_type/1, 
         get_type/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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
init([]) ->
    ets:new(?MODULE, [set, protected, named_table, {keypos, #type_schema.type_id}, {read_concurrency, true}]),
    lists:foreach(fun(Type) -> inner_register_type(Type) end, default_types()),
    {ok, SchemaDir} = application:get_env(erl_ignite, schema),
    filelib:fold_files(SchemaDir,
                       ".*.cfg", 
                       true,
                       fun(File, _) -> 
                               logger:error("find file:~p~n", [File]),
                               {ok, Types} = file:consult(File),
                               lists:foreach(fun(Type) -> inner_register_type(Type) end, Types)
                       end,
                       undefined),
    {ok, #state{}}.

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
handle_cast({register_type, Type}, State) ->
    inner_register_type(Type),
    {noreply, State};
    
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
handle_info(_Info, State) ->
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
-spec default_types() -> list(type_register()).
default_types() ->
    Term = #{name              => "ErlangTerm",
             type              => tuple,
             type_tag          => term,
             version           => 1,
             schema_format     => compact,
             fields            => [
                                    #{name => "Value", type => byte_array}
                                  ],
             constructor       => fun([Bin]) -> erlang:binary_to_term(Bin) end},
    [Term].

inner_register_type(#{name := TypeName,
                      type := TypeType,
                      version := Version,
                      schema_format := SchemaFormat,
                      fields  := FieldDefs} = Register) ->
    TypeTag = maps:get(type_tag, Register, undefined),
    Constructor = maps:get(constructor, Register, undefined),
    OnUpgrades = maps:get(on_upgrades, Register, []),
    TypeId = utils:hash_name(TypeName),
    SchemaId = utils:calculate_schemaId([Name || #{name := Name} <- FieldDefs]),
    case TypeType of
        tuple ->
            FieldDataR = lists:foldl(fun(#{name := Name, type := Type}, DataAcc) ->
                                             FieldType = Type,
                                             FieldId = utils:hash_name(Name),
                                             [{FieldType, FieldId} | DataAcc]
                                     end,
                                     [],
                                     FieldDefs),
            {FieldTypes, FieldIdOrder} = lists:unzip(lists:reverse(FieldDataR)),
            FieldKeys = undefined;
        _ -> 
            FieldDataR = lists:foldl(fun(#{name := Name, type := Type, key := Key}, DataAcc) ->
                                             FieldType = Type,
                                             FieldId = utils:hash_name(Name),
                                             [{FieldType, FieldId, Key} | DataAcc]
                                     end,
                                     [],
                                     FieldDefs),
            {FieldTypes, FieldIdOrder, FieldKeys} = lists:unzip3(lists:reverse(FieldDataR))

    end,
    Schema = #type_schema{type_id = TypeId,
                          type_name = TypeName,
                          type_type = TypeType,
                          type_tag = TypeTag,
                          schema_id = SchemaId,
                          version = Version,
                          field_types = FieldTypes,
                          field_id_order = FieldIdOrder,
                          field_keys = FieldKeys,
                          schema_format = SchemaFormat,
                          constructor = Constructor,
                          on_upgrades = OnUpgrades},
    ets:insert(?MODULE, Schema);

inner_register_type(#{name := TypeName, enums := Enums}) ->
    TypeId = utils:hash_name(TypeName),
    Schema = #enum_schema{type_id = TypeId,
                          type_name = TypeName,
                          values = Enums},
    ets:insert(?MODULE, Schema).

%%%===================================================================
%%% API functions
%%%===================================================================
register_type(Type) ->
    gen_server:cast(?MODULE, {register_type, Type}).

get_type(TypeName) when is_list(TypeName) ->
    TypeId = utils:hash_name(TypeName),
    get_type(TypeId);

get_type(TypeId) ->
    case ets:lookup(?MODULE, TypeId) of
        [Schema] -> Schema;
        _ -> undefined
    end.
