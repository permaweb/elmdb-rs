%%%-------------------------------------------------------------------
%%% @doc
%%% Focused serial read benchmark for elmdb:get/2.
%%%
%%% This bench avoids the earlier bad harness shape where Erlang-side loop and
%%% key-selection overhead dominated the result. It uses:
%%%
%%% - one fixed hit key
%%% - one fixed miss key
%%% - an unrolled 8-key hot-set loop with prebuilt binaries
%%%
%%% Usage:
%%%   erl -pa _build/default/lib/*/ebin -noshell -s elmdb_serial_bench run -s init stop
%%%-------------------------------------------------------------------
-module(elmdb_serial_bench).

-export([run/0, run/1, run_large/0]).

-define(DEFAULT_BLOCKS, 625000).
-define(DEFAULT_KEYS, 2048).

run() ->
    run(#{}).

run_large() ->
    run(#{
        key_count => 1000000,
        blocks => 125000
    }).

run(Overrides) ->
    Dir = maps:get(dir, Overrides, "/tmp/elmdb_serial_bench"),
    Blocks = maps:get(blocks, Overrides, ?DEFAULT_BLOCKS),
    KeyCount = maps:get(key_count, Overrides, ?DEFAULT_KEYS),
    ok = reset_dir(Dir),
    ok = file:make_dir(Dir),
    {ok, Env} = elmdb:env_open(Dir, [{map_size, 1073741824}]),
    {ok, DB} = elmdb:db_open(Env, [create]),
    try
        Keys = seed(DB, KeyCount),
        ok = elmdb:flush(DB),
        FixedHit = element(1, Keys),
        FixedMiss = <<"missing_key_for_serial_bench">>,
        {Hot1, Hot2, Hot3, Hot4, Hot5, Hot6, Hot7, Hot8} = first8(Keys),
        bench_fixed_hit(DB, FixedHit, Blocks),
        bench_fixed_miss(DB, FixedMiss, Blocks),
        bench_hot8(DB, Hot1, Hot2, Hot3, Hot4, Hot5, Hot6, Hot7, Hot8, Blocks)
    after
        ok = elmdb:db_close(DB),
        ok = elmdb:env_close(Env),
        ok = reset_dir(Dir)
    end.

seed(DB, KeyCount) ->
    Keys =
        list_to_tuple(
            [<<"key", I:32/unsigned>> || I <- lists:seq(1, KeyCount)]
        ),
    lists:foreach(
        fun(I) ->
            Key = element(I, Keys),
            Val = <<I:64/unsigned, 0:64/unsigned, 0:64/unsigned, 0:64/unsigned>>,
            ok = elmdb:put(DB, Key, Val)
        end,
        lists:seq(1, KeyCount)
    ),
    Keys.

first8(Keys) ->
    {
        element(1, Keys),
        element(2, Keys),
        element(3, Keys),
        element(4, Keys),
        element(5, Keys),
        element(6, Keys),
        element(7, Keys),
        element(8, Keys)
    }.

bench_fixed_hit(DB, Key, Blocks) ->
    bench("serial_get_fixed_hit", Blocks * 8, fun() -> fixed_hit_loop(DB, Key, Blocks) end).

bench_fixed_miss(DB, Key, Blocks) ->
    bench("serial_get_fixed_miss", Blocks * 8, fun() -> fixed_miss_loop(DB, Key, Blocks) end).

bench_hot8(DB, K1, K2, K3, K4, K5, K6, K7, K8, Blocks) ->
    bench(
        "serial_get_hot8",
        Blocks * 8,
        fun() -> hot8_loop(DB, K1, K2, K3, K4, K5, K6, K7, K8, Blocks) end
    ).

bench(Label, Ops, Fun) ->
    Start = erlang:monotonic_time(microsecond),
    ok = Fun(),
    ElapsedUs = erlang:monotonic_time(microsecond) - Start,
    Rate = Ops * 1000000 / ElapsedUs,
    NsPerOp = ElapsedUs * 1000 / Ops,
    io:format("~s ~.2f ops/s (~.1f ns/op)~n", [Label, Rate, NsPerOp]).

fixed_hit_loop(_DB, _Key, 0) ->
    ok;
fixed_hit_loop(DB, Key, N) ->
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    {ok, _} = elmdb:get(DB, Key),
    fixed_hit_loop(DB, Key, N - 1).

fixed_miss_loop(_DB, _Key, 0) ->
    ok;
fixed_miss_loop(DB, Key, N) ->
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    not_found = elmdb:get(DB, Key),
    fixed_miss_loop(DB, Key, N - 1).

hot8_loop(_DB, _K1, _K2, _K3, _K4, _K5, _K6, _K7, _K8, 0) ->
    ok;
hot8_loop(DB, K1, K2, K3, K4, K5, K6, K7, K8, N) ->
    {ok, _} = elmdb:get(DB, K1),
    {ok, _} = elmdb:get(DB, K2),
    {ok, _} = elmdb:get(DB, K3),
    {ok, _} = elmdb:get(DB, K4),
    {ok, _} = elmdb:get(DB, K5),
    {ok, _} = elmdb:get(DB, K6),
    {ok, _} = elmdb:get(DB, K7),
    {ok, _} = elmdb:get(DB, K8),
    hot8_loop(DB, K1, K2, K3, K4, K5, K6, K7, K8, N - 1).

reset_dir(Dir) ->
    _ = file:del_dir_r(Dir),
    ok.
