%%% @doc Erlang bindings for LMDB (Lightning Memory-Mapped Database) via a C NIF.
%%%
%%% This is the lean surface used by HyperBEAM's `hb_store_lmdb' backend:
%%% open an environment + database, buffered `put', point `get', prefix `list'
%%% of immediate children, and `match' over hierarchical key/value patterns.
%%%
%%% Writes are buffered and flushed automatically before any read and when the
%%% environment is closed, so no explicit flush call is needed. Reads run fully
%%% concurrently (a shared read-lock guards env lifetime, not the reads).
-module(elmdb).

%% Environment + database lifecycle.
-export([env_open/2, env_close_by_name/1, db_open/2]).

%% Key/value operations.
-export([put/3, get/2, list/2, match/2]).

%% NIF loading.
-export([init/0]).
-on_load(init/0).

%%%===================================================================
%%% NIF loading
%%%===================================================================

%% @doc Load the C NIF library from the application's `priv' directory.
-spec init() -> ok | {error, term()}.
init() ->
    PrivDir =
        case code:priv_dir(?MODULE) of
            {error, bad_name} ->
                case filelib:is_dir(filename:join(["..", priv])) of
                    true -> filename:join(["..", priv]);
                    _ -> priv
                end;
            Dir ->
                Dir
        end,
    load_nif_from_list(PrivDir, ["libelmdb_nif", "elmdb_nif"]).

load_nif_from_list(_PrivDir, []) ->
    {error, {load_failed, "elmdb NIF library not found"}};
load_nif_from_list(PrivDir, [LibName | Rest]) ->
    case erlang:load_nif(filename:join([PrivDir, LibName]), 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, _Reason} -> load_nif_from_list(PrivDir, Rest)
    end.

%%%===================================================================
%%% Environment + database lifecycle
%%%===================================================================

%% @doc Open (or reuse) the LMDB environment rooted at `Path'.
%%
%% Environments are singletons per path; a second open of the same path returns
%% the same handle. Supported options:
%%   `{map_size, Bytes}'    maximum database size (default 1 GiB)
%%   `{max_readers, N}'     reader slot count
%%   `{batch_size, N}'      buffered writes flushed per transaction (default 1000)
%%   `no_mem_init'          skip zeroing freshly malloc'd pages
%%   `no_sync'              don't fsync on commit
%%   `no_lock'              skip the LMDB reader lock table
%%   `write_map'            use a writeable memory map
%%   `no_readahead'         disable OS readahead
-spec env_open(Path :: binary() | string(), Options :: list()) ->
    {ok, term()} | {error, term()}.
env_open(_Path, _Options) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Close the environment for `Path' by name (handles stay valid and reopen
%% lazily on next use). Returns `ok', or `{error, not_found}' if unknown.
-spec env_close_by_name(Path :: binary() | string()) -> ok | {error, not_found}.
env_close_by_name(_Path) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Open the (unnamed) database within an environment. Pass `create' to
%% create it if missing.
-spec db_open(Env :: term(), Options :: list()) -> {ok, term()} | {error, term()}.
db_open(_Env, _Options) ->
    erlang:nif_error(nif_not_loaded).

%%%===================================================================
%%% Key/value operations
%%%===================================================================

%% @doc Buffer a key/value write. Flushed automatically before the next read or
%% when the buffer reaches the environment's batch size.
-spec put(DB :: term(), Key :: binary(), Value :: binary()) ->
    ok | {error, term(), binary()}.
put(_DB, _Key, _Value) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Read the value for `Key'. Returns `{ok, Value}' or `not_found'.
-spec get(DB :: term(), Key :: binary()) -> {ok, binary()} | not_found.
get(_DB, _Key) ->
    erlang:nif_error(nif_not_loaded).

%% @doc List the distinct immediate children under a key prefix.
%%
%% For prefix `<<"colors/">>' over keys `colors/red', `colors/blue/navy', this
%% returns `[<<"red">>, <<"blue">>]' (segments up to the next `/'), in sorted
%% order. Returns `not_found' when the prefix has no children.
-spec list(DB :: term(), Prefix :: binary()) -> {ok, [binary()]} | not_found.
list(_DB, _Prefix) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Return the entity ids whose `Suffix => Value' fields match ALL patterns.
%%
%% Keys are treated as `<<"id/suffix">>'; an entity matches a pattern when it has
%% a key ending in `/Suffix' whose value equals `Value' exactly. Returns
%% `{ok, [Id]}' for entities matching every pattern, or `not_found'.
-spec match(DB :: term(), Patterns :: [{binary(), binary()}]) ->
    {ok, [binary()]} | not_found | {error, term(), binary()}.
match(DB, Patterns) ->
    match_pattern(DB, Patterns).

match_pattern(_DB, _Patterns) ->
    erlang:nif_error(nif_not_loaded).
