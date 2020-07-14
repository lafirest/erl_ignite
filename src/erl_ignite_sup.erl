%%%-------------------------------------------------------------------
%% @doc erl_ignite top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(erl_ignite_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(workerSpec(Mod), {Mod, {Mod, start_link, []},
                          transient, timer:minutes(1), worker, [Mod]}).

-define(superSpec(Mod), {Mod, {Mod, start_link, []},
                         transient, infinity, supervisor, [Mod]}).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    {ok, Connect} = application:get_env(erl_ignite, connect),
    {ok, PoolCfgT} = application:get_env(erl_ignite, pool),
    PoolCfg = [{worker, {thin_client, Connect}} | PoolCfgT],
    wpool:start_pool(ignite, PoolCfg),
    {ok, {{one_for_one, 6, 3600}, [?workerSpec(schema_manager)]}}.

%%====================================================================
%% Internal functions
%%====================================================================
