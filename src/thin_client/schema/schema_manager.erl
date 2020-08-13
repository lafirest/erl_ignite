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
-include("type_binary_spec.hrl").
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
    lists:foreach(fun(Type) -> try_register_type(Type) end, default_types()),
    case application:get_env(erl_ignite, schema, undefined) of
        undefined -> ok;
        SchemaDir ->
            filelib:fold_files(SchemaDir,
                               ".*.cfg", 
                               true,
                               fun(File, _) -> 
                                       {ok, Types} = file:consult(File),
                                       lists:foreach(fun(Type) -> try_register_type(Type) end, Types)
                               end,
                               undefined)
    end,
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
    try_register_type(Type),
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

try_register_type(Register) ->
    try
        inner_register_type(Register)
    catch _:Reason:Trace ->
        logger:error("register failed, data :~p~nReason:~p~nTrace:~p~n", [Register, Reason, Trace])
    end.

inner_register_type(#{name := TypeName,
                      type := TypeType,
                      version := Version,
                      schema_format := SchemaFormat,
                      fields  := FieldDefs} = Register) ->
    TypeTag = maps:get(type_tag, Register, undefined),
    Constructor = maps:get(constructor, Register, undefined),
    OnUpgrades = maps:get(on_upgrades, Register, []),
    TypeId = utils:hash_string(TypeName),
    AffinityKey = maps:get(affinity_key, Register, undefined),
    SchemaId = utils:calculate_schemaId([Name || #{name := Name} <- FieldDefs]),
    RegisterMeta = application:get_env(erl_ignite, register_schema, false),
    if TypeType =:= tuple orelse TypeType =:= record ->
            FieldDataR = lists:foldl(fun(#{name := Name, type := Type}, DataAcc) ->
                                             FieldType = Type,
                                             FieldId = utils:hash_string(Name),
                                             [{FieldType, FieldId} | DataAcc]
                                     end,
                                     [],
                                     FieldDefs),
            {FieldTypes, FieldIdOrder} = lists:unzip(lists:reverse(FieldDataR)),
            FieldKeys = undefined;
        true -> 
            FieldDataR = lists:foldl(fun(#{name := Name, type := Type, key := Key}, DataAcc) ->
                                             FieldType = Type,
                                             FieldId = utils:hash_string(Name),
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
    ets:insert(?MODULE, Schema),

    if RegisterMeta ->
           Meta = #{affinity_key => AffinityKey,
                    type_name => TypeName,
                    type_id => TypeId,
                    schemas => [#{schema_id => SchemaId, fields => FieldIdOrder}],
                    fields => lists:foldl(fun(Def, Acc) -> 
                                                  Name = maps:get(name, Def),
                                                  Type = maps:get(type, Def),
                                                  FieldId = utils:hash_string(Name),
                                                  TypeCode = type_atom2code(Type),
                                                  [#{name => Name,
                                                     type_id => FieldId,
                                                     type_code => TypeCode} | Acc]
                                          end,
                                          [],
                                          FieldDefs)},
           ignite:put_type(Meta);
       true -> ok
    end;

inner_register_type(#{name := TypeName, enums := Enums} = Register) ->
    TypeId = utils:hash_string(TypeName),
    Schema = #enum_schema{type_id = TypeId,
                          type_name = TypeName,
                          values = Enums},
    RegisterMeta = maps:get(register_meta, Register, false),
    ets:insert(?MODULE, Schema),
    if RegisterMeta -> ignite:put_type(Schema);
       true -> ok
    end;

inner_register_type(#{caches := Caches}) ->
    lists:foreach(fun(Cache) -> utils:register_cache(Cache) end, Caches).

%% type atom to type code
type_atom2code(byte) -> ?byte_code;
type_atom2code(short) -> ?short_code;
type_atom2code(int) -> ?int_code;
type_atom2code(long) -> ?long_code;
type_atom2code(float) -> ?float_code;
type_atom2code(double) -> ?double_code;
type_atom2code(char) -> ?char_code;
type_atom2code(bool) -> ?bool_code;
type_atom2code(undefined) -> ?null_code;
type_atom2code(bin_string) -> ?string_code;
type_atom2code(string) -> ?string_code;
type_atom2code(uuid) -> ?uuid_code;
type_atom2code(timestamp) -> ?timestamp_code;
type_atom2code(data) -> ?date_code;
type_atom2code(time) -> ?time_code;
type_atom2code({enum, _}) -> ?enum_code;
type_atom2code(byte_array) -> ?byte_array_code;
type_atom2code(short_array) -> ?short_array_code;
type_atom2code(int_array) -> ?int_array_code;
type_atom2code(long_array) -> ?long_array_code;
type_atom2code(float_array) -> ?float_array_code;
type_atom2code(double_array) -> ?double_array_code;
type_atom2code(char_array) -> ?char_array_code;
type_atom2code(bool_array) -> ?bool_array_code;
type_atom2code(bin_string_array) -> ?string_array_code;
type_atom2code(string_array) -> ?string_array_code;
type_atom2code(uuid_array) -> ?uuid_array_code;
type_atom2code(timestamp_array) -> ?timestamp_array_code;
type_atom2code(date_array) -> ?date_array_code;
type_atom2code(time_array) -> ?time_array_code;
type_atom2code({object_array, _}) -> ?complex_object_code;
type_atom2code({collection, _, _}) -> ?collection_code;
type_atom2code({map, _, _}) -> ?map_code;
type_atom2code({orddict, _, _}) -> ?map_code;
type_atom2code({enum_array, _}) -> ?enum_array_code;
type_atom2code({complex_object, _}) -> ?complex_object_code;
type_atom2code({wrapped, _}) -> ?wrapped_data_code;
type_atom2code({binary_enum, _}) -> ?binary_enum_code;
type_atom2code(fast_term) -> ?byte_array_code;
type_atom2code(term) -> ?complex_object_code.


%%%===================================================================
%%% API functions
%%%===================================================================
register_type(Type) ->
    gen_server:cast(?MODULE, {register_type, Type}).

get_type(TypeName) when is_list(TypeName) ->
    TypeId = utils:hash_string(TypeName),
    get_type(TypeId);

get_type(TypeId) ->
    case ets:lookup(?MODULE, TypeId) of
        [Schema] -> Schema;
        _ -> undefined
    end.
