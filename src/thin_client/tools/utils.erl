-module(utils).
-export([hash_string/1, 
         hash_raw_string/1, 
         hash_data/1,
         calculate_schemaId/1,
         get_cache_id/1,
         register_cache/1,
         to_raw_bool/1,
         from_raw_bool/1,
         parse_read_options/1,
         parse_write_options/1,
         parse_query_options/1,
         parse_sql_options/1]).

-on_load(init/0).

-include("type_spec.hrl").
-define(FNV1_OFFSET_BASIS, 16#811C9DC5).
-define(FNV1_PRIME, 16#01000193).
-define(CACHE_KEY_TAG, ignite_cache).
-define(APPNAME, erl_ignite).
-define(LIBNAME, utils_nif).

init() ->
    SoName = case code:priv_dir(?APPNAME) of
                 {error, bad_name} ->
                     case filelib:is_dir(filename:join(["..", priv])) of
                         true ->
                             filename:join(["..", priv, ?LIBNAME]);
                         _ ->
                             filename:join([priv, ?LIBNAME])
                     end;
                 Dir ->
                     filename:join(Dir, ?LIBNAME)
             end,
    erlang:load_nif(SoName, 0).

%% use for field name
hash_string(Bin) ->
    case erlang:is_binary(Bin) of
        true ->
            lower_hash(Bin);
        _ ->
            lower_hash(erlang:list_to_binary(Bin))
    end.

%% use for cache id
hash_raw_string(Bin) -> 
    case erlang:is_binary(Bin) of
        true ->
            data_hash(Bin, 0);
        _ ->
            data_hash(erlang:list_to_binary(Bin), 0)
    end.

%% use for complex object hash
hash_data(Bin) ->
    data_hash(Bin, 1).

data_hash(_, _) ->
    not_loaded(?LINE).

lower_hash(_) ->
    not_loaded(?LINE).

calculate_schemaId(_) ->
    not_loaded(?LINE).

get_cache_id(CacheId) ->
    if is_atom(CacheId) -> persistent_term:get({?CACHE_KEY_TAG, CacheId});
       is_integer(CacheId) -> CacheId;
       true -> hash_raw_string(CacheId)
    end.

register_cache({Atom, Name}) ->
    CacheId = hash_raw_string(Name),
    persistent_term:put({?CACHE_KEY_TAG, Atom}, CacheId);

register_cache(Atom) ->
    Name = erlang:atom_to_binary(Atom),
    CacheId = hash_raw_string(Name),
    persistent_term:put({?CACHE_KEY_TAG, Atom}, CacheId).

to_raw_bool(true) -> 1;
to_raw_bool(false) -> 0.

from_raw_bool(1) -> true;
from_raw_bool(0) -> false.

parse_read_options(Options) -> 
    lists:foldl(fun(E, Acc) -> i_parse_read_options(E, Acc) end, #read_option{}, Options).

i_parse_read_options(fast_term, Option) -> Option#read_option{fast_term = true};
i_parse_read_options(keep_wrapped, Option) -> Option#read_option{keep_wrapped = true};
i_parse_read_options(keep_binary_object, Option) -> Option#read_option{keep_binary_object = true};
i_parse_read_options({timeout, Timeout}, Option) -> Option#read_option{timeout = Timeout}.

parse_write_options(Options) ->
    lists:foldl(fun(E, Acc) -> i_parse_write_options(E, Acc) end, #write_option{}, Options).

i_parse_write_options(fast_term, Option) -> Option#write_option{fast_term = true}.

parse_query_options(Options) -> 
    WriteOptions = maps:get(write, Options, []),
    ReadOptions = maps:get(read, Options, []),
    AsyncCallback = maps:get(async, Options, undefined),
    {AsyncCallback, parse_write_options(WriteOptions), parse_read_options(ReadOptions)}.

parse_sql_options(Options) ->
    WriteOptions = maps:get(write, Options, []),
    ReadOptions = maps:get(read, Options, []),
    AsyncCallback = maps:get(async, Options, undefined),
    SqlOptions = maps:get(sql, Options, []),
    {AsyncCallback, SqlOptions, parse_write_options(WriteOptions), parse_read_options(ReadOptions)}.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
