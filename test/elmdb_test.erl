%%% @doc Test suite for the elmdb C NIF, covering exactly the live API that
%%% HyperBEAM's `hb_store_lmdb' uses: env_open, db_open, put, put_batch,
%%% get, list, read_prefix, match, env_close_by_name -- plus the write-buffer
%%% flush-on-read behaviour and the concurrent read / close-race safety the
%%% NIF guarantees.
-module(elmdb_test).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Fixtures
%%%===================================================================

setup() ->
    Dir = test_dir(),
    file:del_dir_r(Dir),
    ok = filelib:ensure_dir(Dir ++ "/"),
    {ok, Env} = elmdb:env_open(Dir, [{map_size, 64 * 1024 * 1024}, {batch_size, 1000}]),
    {ok, DB} = elmdb:db_open(Env, [create]),
    {Dir, Env, DB}.

cleanup({Dir, _Env, _DB}) ->
    catch elmdb:env_close_by_name(Dir),
    file:del_dir_r(Dir).

test_dir() ->
    Unique = erlang:unique_integer([positive]),
    filename:join(["/tmp", "elmdb_test_" ++ integer_to_list(Unique)]).

%%%===================================================================
%%% put / get
%%%===================================================================

put_get_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     ok = elmdb:put(DB, <<"hello">>, <<"world">>),
                     ?assertEqual({ok, <<"world">>}, elmdb:get(DB, <<"hello">>))
                 end),
          ?_assertEqual(not_found, elmdb:get(DB, <<"absent">>)),
          ?_test(begin
                     %% Last write wins; empty values round-trip.
                     ok = elmdb:put(DB, <<"k">>, <<"v1">>),
                     ok = elmdb:put(DB, <<"k">>, <<"v2">>),
                     ?assertEqual({ok, <<"v2">>}, elmdb:get(DB, <<"k">>)),
                     ok = elmdb:put(DB, <<"empty">>, <<>>),
                     ?assertEqual({ok, <<>>}, elmdb:get(DB, <<"empty">>))
                 end),
          %% Invalid keys are rejected, not stored.
          ?_assertMatch({error, _, _}, elmdb:put(DB, <<>>, <<"x">>)),
          ?_assertMatch({error, _, _}, elmdb:put(DB, binary:copy(<<"a">>, 512), <<"x">>))
         ]
     end}.

%% A read flushes the write buffer first, so values written but not yet flushed
%% are still observed -- HyperBEAM relies on this instead of calling flush.
flush_on_read_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         ?_test(begin
                    N = 500,
                    [ ok = elmdb:put(DB, key(I), val(I)) || I <- lists:seq(1, N) ],
                    ?assert(lists:all(
                        fun(I) -> elmdb:get(DB, key(I)) =:= {ok, val(I)} end,
                        lists:seq(1, N)))
                end)
	     end}.

put_batch_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         ?_test(begin
                    ok = elmdb:put_batch(
                        DB,
                        [
                            {<<"batch/a">>, <<"1">>},
                            {<<"batch/b">>, <<"2">>}
                        ]
                    ),
                    ?assertEqual({ok, <<"1">>}, elmdb:get(DB, <<"batch/a">>)),
                    ?assertEqual({ok, <<"2">>}, elmdb:get(DB, <<"batch/b">>))
                end)
     end}.

%%%===================================================================
%%% list (immediate children)
%%%===================================================================

list_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     ok = elmdb:put(DB, <<"colors/red">>, <<"1">>),
                     ok = elmdb:put(DB, <<"colors/blue">>, <<"2">>),
                     ok = elmdb:put(DB, <<"colors/green">>, <<"3">>),
                     ok = elmdb:put(DB, <<"colors/shades/dark">>, <<"4">>),
                     ok = elmdb:put(DB, <<"colors/shades/light">>, <<"5">>),
                     %% Immediate children only; nested keys collapse to their
                     %% first segment ("shades"); distinct + sorted.
                     {ok, Children} = elmdb:list(DB, <<"colors/">>),
                     ?assertEqual(
                        [<<"blue">>, <<"green">>, <<"red">>, <<"shades">>],
                        lists:sort(Children)),
                     {ok, Shades} = elmdb:list(DB, <<"colors/shades/">>),
                     ?assertEqual([<<"dark">>, <<"light">>], lists:sort(Shades))
                 end),
          ?_assertEqual(not_found, elmdb:list(DB, <<"nonexistent/">>))
         ]
	     end}.

read_prefix_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         ?_test(begin
                    ok = elmdb:put_batch(
                        DB,
                        [
                            {<<"root">>, <<"group">>},
                            {<<"root/a">>, <<"1">>},
                            {<<"root/b">>, <<"2">>},
                            {<<"root/b/c">>, <<"3">>}
                        ]
                    ),
                    ?assertEqual(
                        {ok,
                            [
                                {<<"root">>, <<"group">>},
                                {<<"root/a">>, <<"1">>},
                                {<<"root/b">>, <<"2">>},
                                {<<"root/b/c">>, <<"3">>}
                            ]
                        },
                        elmdb:read_prefix(DB, <<"root">>)
                    ),
                    ?assertEqual(not_found, elmdb:read_prefix(DB, <<"absent">>))
                end)
     end}.

%%%===================================================================
%%% match (entities matching all key/value patterns)
%%%===================================================================

match_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         ?_test(begin
                    ok = elmdb:put(DB, <<"alice/name">>, <<"Alice">>),
                    ok = elmdb:put(DB, <<"alice/email">>, <<"alice@x.com">>),
                    ok = elmdb:put(DB, <<"bob/name">>, <<"Bob">>),
                    ok = elmdb:put(DB, <<"bob/email">>, <<"bob@x.com">>),
                    %% Single pattern.
                    ?assertEqual({ok, [<<"alice">>]},
                                 elmdb:match(DB, [{<<"name">>, <<"Alice">>}])),
                    %% All patterns must match the same entity.
                    ?assertEqual({ok, [<<"alice">>]},
                                 elmdb:match(DB,
                                     [{<<"name">>, <<"Alice">>},
                                      {<<"email">>, <<"alice@x.com">>}])),
                    %% No single entity satisfies a cross-entity combination.
                    ?assertEqual(not_found,
                                 elmdb:match(DB,
                                     [{<<"name">>, <<"Alice">>},
                                      {<<"email">>, <<"bob@x.com">>}])),
                    %% Value must match exactly.
                    ?assertEqual(not_found,
                                 elmdb:match(DB, [{<<"name">>, <<"alice">>}]))
                end)
     end}.

%%%===================================================================
%%% Environment lifecycle
%%%===================================================================

reopen_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun({Dir, _Env, DB}) ->
         ?_test(begin
                    ok = elmdb:put(DB, <<"persist">>, <<"value">>),
                    ?assertEqual({ok, <<"value">>}, elmdb:get(DB, <<"persist">>)),
                    %% Soft close by name flushes; the handle reopens lazily and
                    %% still sees the committed data.
                    ok = elmdb:env_close_by_name(Dir),
                    ?assertEqual({ok, <<"value">>}, elmdb:get(DB, <<"persist">>)),
                    ?assertEqual({error, not_found}, elmdb:env_close_by_name(<<"/tmp/nope-elmdb">>))
                end)
     end}.

%%%===================================================================
%%% Concurrency: parallel reads + close-vs-read race
%%%===================================================================

%% Many processes read while another repeatedly closes the env by name. Reads
%% must stay correct (and never crash the VM) as the env drains and reopens.
concurrent_read_close_test_() ->
    {timeout, 60,
     {setup, fun setup/0, fun cleanup/1,
      fun({Dir, _Env, DB}) ->
          ?_test(begin
                     N = 500,
                     [ ok = elmdb:put(DB, key(I), val(I)) || I <- lists:seq(1, N) ],
                     %% force a flush so all keys are committed
                     _ = elmdb:get(DB, key(1)),
                     Stop = erlang:monotonic_time(millisecond) + 3000,
                     Parent = self(),
                     Readers =
                         [ spawn_link(fun() -> reader(DB, N, Stop, 0, Parent) end)
                           || _ <- lists:seq(1, 8) ],
                     Churn = spawn_link(fun() -> churn(Dir, Stop, Parent) end),
                     Bad = lists:sum([ receive {reader, P, B} -> B end || P <- Readers ]),
                     receive {churn, Churn, _Closes} -> ok end,
                     ?assertEqual(0, Bad)
                 end)
      end}}.

reader(DB, N, Stop, Bad, Parent) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true -> Parent ! {reader, self(), Bad};
        false ->
            PassBad =
                lists:foldl(
                    fun(I, B) ->
                        %% Found -> must be the right value. not_found is
                        %% tolerated only during a close/reopen window.
                        case elmdb:get(DB, key(I)) of
                            {ok, V} when V =:= <<I:64>> -> B;
                            {ok, _Wrong} -> B + 1;
                            not_found -> B;
                            {error, _, _} -> B;
                            _ -> B + 1
                        end
                    end, 0, lists:seq(1, N)),
            reader(DB, N, Stop, Bad + PassBad, Parent)
    end.

churn(Dir, Stop, Parent) -> churn(Dir, Stop, Parent, 0).
churn(Dir, Stop, Parent, C) ->
    case erlang:monotonic_time(millisecond) >= Stop of
        true -> Parent ! {churn, self(), C};
        false ->
            catch elmdb:env_close_by_name(Dir),
            churn(Dir, Stop, Parent, C + 1)
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

key(I) -> <<"k/", (integer_to_binary(I))/binary>>.
val(I) -> <<I:64>>.
