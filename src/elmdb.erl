%%%-------------------------------------------------------------------
%%% @doc
%%% LMDB NIF bindings for Erlang
%%% 
%%% This module provides Erlang bindings for LMDB (Lightning Memory-Mapped Database)
%%% through a Rust NIF implementation.
%%% @end
%%%-------------------------------------------------------------------
-module(elmdb).

%% Environment management
-export([env_open/2, env_sync/1, env_close/1, env_close_by_name/1, env_status/1]).

%% Database operations
-export([db_open/2, db_close/1]).

%% Key-value operations
-export([put/3, put_batch/2, put_batch_direct/2, put_batch_append/2, get/2,
         flush/1]).

%% Diagnostics
-export([overlay_count/1]).

%% Iterator operations
-export([iterator/1, iterator_next/2, foreach/2, fold/3, map/2]).

%% List operations
-export([list/2, read_prefix/2, read_dups/3]).

%% Pattern matching operations
-export([match/2]).


%% NIF loading
-export([init/0]).

-on_load(init/0).

%%%===================================================================
%%% NIF Loading
%%%===================================================================

%% @doc Initialize and load the NIF library
-spec init() -> ok | {error, term()}.
init() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            case filelib:is_dir(filename:join(["..", priv])) of
                true -> filename:join(["..", priv]);
                _ -> priv
            end;
        Dir ->
            Dir
    end,
    % Try different library names based on OS
    LibNames = case os:type() of
        {unix, darwin} -> ["libelmdb_nif", "elmdb_nif"];
        {unix, _} -> ["libelmdb_nif", "elmdb_nif"];
        _ -> ["elmdb_nif"]
    end,
    load_nif_from_list(PrivDir, LibNames).

load_nif_from_list(_PrivDir, []) ->
    {error, {load_failed, "Failed to load NIF library: no suitable library found"}};
load_nif_from_list(PrivDir, [LibName | Rest]) ->
    SoName = filename:join([PrivDir, LibName]),
    case erlang:load_nif(SoName, 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, _Reason} -> 
            load_nif_from_list(PrivDir, Rest)
    end.

%%%===================================================================
%%% Environment Management
%%%===================================================================

%% @doc Create or open an LMDB environment
%% @param Path Directory path for the database files
%% @param Options Configuration options:
%%   - {map_size, integer()}: Maximum database size in bytes
%%   - {max_readers, integer()}: Maximum number of reader slots (default: 126)
%%   - {page_size, integer()}: Database page size in bytes, a power of two
%%     between 512 and 65536. Applies when the data file is created; an
%%     existing file keeps the page size it was created with.
%%   - no_mem_init: Don't initialize malloc'd memory before writing to disk
%%   - no_sync: Don't flush system buffers to disk when committing
%%   - write_map: Use a writeable memory map for better performance
%%   - read_only: Open the environment read-only
%%   - no_subdir: Path names the data file itself rather than a directory
%% @returns {ok, Env} where Env is an opaque environment handle
%%          {error, directory_not_found} if the directory doesn't exist
%%          {error, permission_denied} if lacking permissions
%%          {error, already_open} if environment is already open
%%          {error, no_space} if disk is full
%%          {error, corrupted} if database is corrupted
%%          {error, ErrorAtom} for other errors
-spec env_open(Path :: binary() | string(), Options :: list()) -> 
    {ok, term()} | {error, term()}.
env_open(_Path, _Options) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Flushes LMDB buffers to disk 
%% @param Env Environment handle from env_open
%% @returns ok
-spec env_sync(Env :: term()) -> ok.
env_sync(_Env) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Close an LMDB environment and release resources
%% @param Env Environment handle from env_open
%% @returns ok
-spec env_close(Env :: term()) -> ok.
env_close(_Env) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Close an environment by its directory path (fallback method)
%% @param Path Directory path of the database
%% @returns ok
-spec env_close_by_name(Path :: binary() | string()) -> ok.
env_close_by_name(_Path) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Get status information about an environment
%% @param Env Environment handle
%% @returns {ok, Closed, RefCount, Path} where Closed is boolean, RefCount is integer
-spec env_status(Env :: term()) -> {ok, boolean(), integer(), string()}.
env_status(_Env) ->
    erlang:nif_error(nif_not_loaded).


%%%===================================================================
%%% Database Operations
%%%===================================================================

%% @doc Open a database within an environment
%% @param Env Environment handle
%% @param Options Configuration options:
%%   - create: Create the database if it doesn't exist
%%   - dupsort: Keys may carry multiple values, stored in sorted order
%%   - dupfixed: All values of a key have the same size (implies dupsort)
%% The dup options must match the database on disk: opening a non-empty
%% database in a different dup mode returns an error.
%% @returns {ok, DBInstance} where DBInstance is an opaque database handle
-spec db_open(Env :: term(), Options :: list()) -> 
    {ok, term()} | {error, term()}.
db_open(_Env, _Options) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Close a database handle and decrement environment reference count
%% @param DBInstance Database handle from db_open
%% @returns ok
-spec db_close(DBInstance :: term()) -> ok | {error, term(), string()}.
db_close(_DBInstance) ->
    erlang:nif_error(nif_not_loaded).

%%%===================================================================
%%% Key-Value Operations
%%%===================================================================

%% @doc Write a key-value pair to the database
%% @param DBInstance Database handle
%% @param Key The key to write (binary)
%% @param Value The value to store (binary)
%% @returns ok on success
%% @throws {error, Type, Description} on failure
-spec put(DBInstance :: term(), Key :: binary(), Value :: binary()) -> 
    ok | {error, term(), binary()}.
put(_DBInstance, _Key, _Value) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Write multiple key-value pairs to the database in a single transaction
%% @param DBInstance Database handle
%% @param KeyValuePairs List of {Key, Value} tuples where Key and Value are binaries
%% @returns ok on success, or {ok, SuccessCount, Errors} if some writes failed
%% @throws {error, Type, Description} on failure
-spec put_batch(DBInstance :: term(), KeyValuePairs :: [{binary(), binary()}]) -> 
    ok | {ok, integer(), list()} | {error, term(), binary()}.
put_batch(_DBInstance, _KeyValuePairs) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Write multiple key-value pairs directly to LMDB in one transaction.
%%      The buffered overlay is flushed first so older queued writes cannot
%%      later overwrite this batch.
-spec put_batch_direct(DBInstance :: term(), KeyValuePairs :: [{binary(), binary()}]) ->
    ok | {error, term(), binary()}.
put_batch_direct(_DBInstance, _KeyValuePairs) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Append key-value pairs at the end of the database in one transaction.
%%      Writes with MDB_APPEND (and MDB_APPENDDUP on dup databases), skipping
%%      the page-split search, so the batch must be in strictly ascending
%%      order -- by key, or by {Key, Value} pair on a dup database -- and must
%%      sort after everything already stored. Out-of-order input within the
%%      batch returns a validation_error; input that does not extend the
%%      database tail returns key_exist. The buffered overlay is flushed
%%      first so older queued writes cannot later overwrite this batch.
-spec put_batch_append(DBInstance :: term(), KeyValuePairs :: [{binary(), binary()}]) ->
    ok | {error, term(), binary()}.
put_batch_append(_DBInstance, _KeyValuePairs) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Read a value by key from the database
%% @param DBInstance Database handle
%% @param Key The key to read (binary)
%% @returns {ok, Value} where Value is a binary, or not_found if key doesn't exist.
%%          On a dup database the first (smallest) duplicate is returned.
-spec get(DBInstance :: term(), Key :: binary()) ->
    {ok, binary()} | not_found.
get(_DBInstance, _Key) ->
    erlang:nif_error(nif_not_loaded).

%%%===================================================================
%%% Iterator Operations
%%%===================================================================

%% @doc Create an iterator cursor token for a database scan.
%% @param DBInstance Database handle
%% @returns Cursor token that can be passed to iterator_next/2
-spec iterator(DBInstance :: term()) -> term() | {error, term(), binary()}.
iterator(_DBInstance) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Fetch the next {Key, Value} pair and continuation cursor.
%% @param DBInstance Database handle
%% @param Cursor Iterator token returned by iterator/1 or iterator_next/2
%% @returns {ok, Key, Value, NextCursor} or undefined when exhausted
-spec iterator_next(DBInstance :: term(), Cursor :: term()) ->
    {ok, binary(), binary(), term()} | undefined | {error, term(), binary()}.
iterator_next(_DBInstance, _Cursor) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Execute over the full keyspace with an arity-2 callback.
%%      Callback is called as Fun(Key, Value). Stops when iterator_next returns
%%      undefined.
-spec foreach(DBInstance :: term(), Fun :: fun((binary(), binary()) -> term())) ->
    ok | {error, term(), binary()}.
foreach(DBInstance, Fun) when is_function(Fun, 2) ->
    case iterator(DBInstance) of
        {error, _, _} = Error -> Error;
        Cursor -> fold_loop(DBInstance, Cursor, Fun)
    end.

%% @doc Fold over the full keyspace with an accumulator callback.
%%      Callback is called as Fun(Key, Value, AccIn) and returns AccOut.
-spec fold(DBInstance :: term(), Fun :: fun((binary(), binary(), term()) -> term()), Acc0 :: term()) ->
    {ok, term()} | {error, term(), binary()}.
fold(DBInstance, Fun, Acc0) when is_function(Fun, 3) ->
    case iterator(DBInstance) of
        {error, _, _} = Error -> Error;
        Cursor -> fold_loop_acc(DBInstance, Cursor, Fun, Acc0)
    end.

fold_loop(DBInstance, Cursor, Fun) ->
    case iterator_next(DBInstance, Cursor) of
        {ok, Key, Value, NextCursor} ->
            _ = Fun(Key, Value),
            fold_loop(DBInstance, NextCursor, Fun);
        undefined ->
            ok;
        {error, _, _} = Error ->
            Error
    end.

fold_loop_acc(DBInstance, Cursor, Fun, Acc) ->
    case iterator_next(DBInstance, Cursor) of
        {ok, Key, Value, NextCursor} ->
            NextAcc = Fun(Key, Value, Acc),
            fold_loop_acc(DBInstance, NextCursor, Fun, NextAcc);
        undefined ->
            {ok, Acc};
        {error, _, _} = Error ->
            Error
    end.

%% @doc Map over all key-value pairs and return an Erlang map of Key => Fun(Key, Value).
-spec map(DBInstance :: term(), Fun :: fun((binary(), binary()) -> term())) ->
    {ok, map()} | {error, term(), binary()}.
map(DBInstance, Fun) when is_function(Fun, 2) ->
    fold(DBInstance, fun(Key, Value, Acc) ->
        Acc#{Key => Fun(Key, Value)}
    end, #{}).

%%%===================================================================
%%% List Operations
%%%===================================================================

%% @doc List all direct children of a group using prefix matching
%% @param DBInstance Database handle
%% @param Key The key prefix to search for (binary)
%% @returns {ok, Children} where Children is a list of binaries, or not_found
-spec list(DBInstance :: term(), Key :: binary()) ->
    {ok, [binary()]} | not_found.
list(_DBInstance, _Key) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Read all raw row entries under a prefix.
%% @param DBInstance Database handle
%% @param Key The key prefix to search for (binary)
%% @returns {ok, Entries} where Entries is [{FullKey, Value}], or not_found.
-spec read_prefix(DBInstance :: term(), Key :: binary()) ->
    {ok, [{binary(), binary()}]} | not_found.
read_prefix(DBInstance, Key) ->
    read_prefix_rows(DBInstance, Key).

read_prefix_rows(_DBInstance, _Key) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Read values from one key's duplicate set on a dup database.
%% @param DBInstance Database handle (opened with dupsort or dupfixed)
%% @param Key The key whose duplicate set is read (binary)
%% @param Options Selection options:
%%   - {from, binary()}: Start at the first value >= from (forward) or the
%%     last value =< from (backward)
%%   - {prefix, binary()}: Only values carrying this byte prefix
%%   - {limit, integer()}: Maximum number of values to return (0 = all)
%%   - {direction, forward | backward}: Walk order (default: forward)
%% @returns {ok, Values} in walk order -- descending for backward reads --
%%          where an empty list means the key exists but no value matched;
%%          not_found when the key is absent.
-spec read_dups(DBInstance :: term(), Key :: binary(), Options :: list()) ->
    {ok, [binary()]} | not_found | {error, term(), binary()}.
read_dups(_DBInstance, _Key, _Options) ->
    erlang:nif_error(nif_not_loaded).

%%%===================================================================
%%% Pattern Matching Operations
%%%===================================================================

%% @doc Match database entries against a set of key-value patterns
%% @param DBInstance Database handle
%% @param Patterns List of {KeySuffix, Value} tuples to match against
%%        KeySuffix is the part after the last '/' in hierarchical keys
%%        Value must match exactly for a successful match
%% @returns {ok, [MatchingIDs]} where MatchingIDs is a list of binary IDs
%%          not_found if no matches exist
%%          {error, ErrorType, Description} on error
%% @example
%%   Patterns = [{<<"name">>, <<"Alice">>}, {<<"email">>, <<"alice@example.com">>}],
%%   {ok, IDs} = elmdb:match(DB, Patterns).
%%   %% Returns IDs where all patterns match, e.g., {ok, [<<"users/alice">>]}
-spec match(DBInstance :: term(), Patterns :: [{binary(), binary()}]) -> 
    {ok, [binary()]} | not_found | {error, term(), binary()}.
match(DBInstance, Patterns) ->
    match_pattern(DBInstance, Patterns).

match_pattern(_DBInstance, _Patterns) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Explicitly flush any buffered writes to disk
%% @param DBInstance Database handle
%% @returns ok on success
-spec flush(DBInstance :: term()) -> ok | {error, term(), binary()}.
flush(_DBInstance) ->
    erlang:nif_error(nif_not_loaded).

%% @doc Return the number of entries in the write overlay (diagnostic)
-spec overlay_count(DBInstance :: term()) -> non_neg_integer().
overlay_count(_DBInstance) ->
    erlang:nif_error(nif_not_loaded).
