%% Comprehensive stress benchmark for elmdb-rs.
%%
%% Driven entirely by environment variables. Designed to be run via
%% scripts/stress/stress.sh — never as part of any automatic test target.
%%
%% Public elmdb API only (env_open/2, db_open/2, put/3, get/2, list/2,
%% flush/1, overlay_count/1, env_close/1). No NIF-internal calls. That lets
%% the same harness run unchanged against historical versions.
-module(elmdb_stress).

-export([run/0, bench_generator/0]).

%% =====================================================================
%% Entry
%% =====================================================================

run() ->
    try
        do_run()
    catch
        throw:{abort, Reason, Detail} ->
            io:format(standard_error,
                      "STRESS_ABORT reason=~p detail=~p~n",
                      [Reason, Detail]),
            erlang:halt(2);
        Class:Reason2:Stk ->
            io:format(standard_error,
                      "STRESS_CRASH ~p:~p~n~p~n",
                      [Class, Reason2, Stk]),
            erlang:halt(3)
    end.

do_run() ->
    Cfg = config(),
    log("config: ~p", [Cfg]),
    Spec = parse_size(maps:get(size, Cfg)),
    ok = ensure_size_sane(Spec),
    case run_generator_check() of
        ok -> ok;
        {error, slow, R} ->
            throw({abort, slow_generator,
                   [{rate_gib_per_s, R / 1073741824}]})
    end,
    Mix = parse_mix(maps:get(read_mix, Cfg)),
    Pool = make_pool(Spec),
    DbDir = maps:get(db_dir, Cfg),
    ok = filelib:ensure_dir(filename:join(DbDir, "x")),
    {ok, Env} = elmdb:env_open(DbDir, build_env_opts(Cfg)),
    {ok, DB} = elmdb:db_open(Env, []),
    %% Shared atomics:
    %%   slot 1            = total k+v bytes accepted so far
    %%   slot 2            = boolean (0/1): all writers should stop
    %%   slot 3..3+W-1     = per-writer counter (how many keys written)
    NumWriters = maps:get(writers, Cfg),
    NumReaders = maps:get(readers, Cfg),
    Counters = atomics:new(2 + NumWriters, [{signed, false}]),
    Coord = self(),
    {ok, Sampler} = proc_sampler:start_link(100),
    OverlayPid = spawn_link(
        fun() -> overlay_sampler_loop(DB, 100, []) end),
    MemWatch = spawn_link(
        fun() -> mem_watch_loop(Coord, maps:get(mem_cap_mib, Cfg)) end),
    DiskWatch = spawn_link(
        fun() -> disk_watch_loop(Coord, DbDir) end),
    TimeWatch = erlang:send_after(
        maps:get(duration_s_max, Cfg) * 1000, Coord, time_limit),
    T0 = erlang:monotonic_time(microsecond),
    StartSample = proc_sampler:snapshot(),
    MaxBytes = maps:get(max_bytes, Cfg),
    _Writers = [spawn_writer(Coord, DB, Pool, Spec, Counters, WI, MaxBytes)
                || WI <- lists:seq(0, NumWriters - 1)],
    Readers = [spawn_reader(DB, Pool, Spec, Counters, NumWriters, Mix,
                            maps:get(range_size, Cfg))
               || _ <- lists:seq(1, NumReaders)],
    {WriteSummary, AbortInfo} =
        await_writers(NumWriters, T0, [], Counters),
    T_overlay_done = erlang:monotonic_time(microsecond),
    erlang:cancel_timer(TimeWatch),
    %% On abort: tell writers to stop and drain any remaining writer_done
    %% messages so we don't lose their histograms.
    case AbortInfo of
        undefined -> ok;
        _ ->
            atomics:put(Counters, 2, 1),
            ok
    end,
    %% Flush only if the run completed cleanly. Otherwise skip — flushing
    %% under a tripped catastrophic gate can prolong the abort.
    {Outcome, FlushUs, AbortDetail} =
        case AbortInfo of
            undefined ->
                log("writers done; flushing"),
                Tf0 = erlang:monotonic_time(microsecond),
                _ = (catch elmdb:flush(DB)),
                Tf1 = erlang:monotonic_time(microsecond),
                log("flushed in ~p us", [Tf1 - Tf0]),
                {ok, Tf1 - Tf0, null};
            {abort, AbR, AbD} ->
                {catastrophic, 0, format_term({AbR, AbD})}
        end,
    T_total_done = erlang:monotonic_time(microsecond),
    %% Stop the under-load readers and capture their stats. They were
    %% running while writers were saturating the system, so these are
    %% the "noisy reads under writer pressure" numbers.
    ReaderResults = stop_readers(Readers, 2000),
    %% Optional post-load phase: spawn fresh readers with NO concurrent
    %% writers. This is the clean-room measurement — many more samples,
    %% no writer interference, deterministic-ish percentiles. Set
    %% MEASURE_AFTER_WRITES_S > 0 to enable.
    MeasureAfterS = maps:get(measure_after_writes_s, Cfg),
    {PostReaderResults, T_post_done} = case MeasureAfterS of
        0 ->
            {[], T_total_done};
        S ->
            log("measure-after-writes phase: ~p s with no writers, "
                "~p reader(s)", [S, NumReaders]),
            T_post_start = erlang:monotonic_time(microsecond),
            PostReaders = [spawn_reader(DB, Pool, Spec, Counters,
                                        NumWriters, Mix,
                                        maps:get(range_size, Cfg))
                           || _ <- lists:seq(1, NumReaders)],
            timer:sleep(S * 1000),
            PostResults = stop_readers(PostReaders, 2000),
            T_post_end = erlang:monotonic_time(microsecond),
            log("post-load phase done in ~p ms",
                [(T_post_end - T_post_start) div 1000]),
            {PostResults, T_post_end}
    end,
    [catch unlink(P) || P <- [MemWatch, DiskWatch, OverlayPid]],
    [exit(P, shutdown) || P <- [MemWatch, DiskWatch]],
    EndSample = proc_sampler:snapshot(),
    Samples = proc_sampler:get_samples(Sampler),
    proc_sampler:stop(Sampler),
    OverlayCounts = capture_overlay(OverlayPid),
    exit(OverlayPid, shutdown),
    catch elmdb:env_close(Env),
    Result = build_result(Cfg, Spec, Mix,
                          T0, T_overlay_done, T_total_done, T_post_done,
                          FlushUs, Outcome, AbortDetail,
                          WriteSummary, ReaderResults, PostReaderResults,
                          StartSample, EndSample, Samples,
                          OverlayCounts),
    write_result(Cfg, Result),
    print_summary(Result),
    case Outcome of
        ok -> ok;
        _ -> erlang:halt(2)
    end.

ensure_size_sane(Spec) ->
    case min_k(Spec) of
        K when K < 1 -> throw({abort, bad_size, {k_too_small, K}});
        _ -> ok
    end.

%% =====================================================================
%% Config
%% =====================================================================

config() ->
    #{
        db_dir => env("DB_DIR", "/home/user/mnt/stress"),
        results_dir => env("RESULTS_DIR", "/home/user/mnt/stress-results"),
        max_bytes => env_int("MAX_BYTES", 15 * 1024 * 1024 * 1024),
        mem_cap_mib => env_int("MEM_CAP_MIB", 2048),
        writers => env_int("WRITERS", 8),
        readers => env_int("READERS", 4),
        size => env("SIZE", "avg"),
        read_mix => env("READ_MIX",
                        "point_random:40,zipfian_point:30,"
                        "range_random:20,zipfian_range:10"),
        range_size => env_int("RANGE_SIZE", 64),
        map_size => env_int("MAP_SIZE", 50 * 1024 * 1024 * 1024),
        batch_size => env_int_opt("BATCH_SIZE"),
        flush_bytes => env_int_opt("FLUSH_BYTES"),
        flush_idle_seconds => env_int_opt("FLUSH_IDLE_SECONDS"),
        no_sync => env_bool("NO_SYNC"),
        write_map => env_bool("WRITE_MAP"),
        run_label => env("RUN_LABEL", "unlabeled"),
        duration_s_max => env_int("DURATION_S_MAX", 1800),
        measure_after_writes_s => env_int("MEASURE_AFTER_WRITES_S", 0)
    }.

env(K, D) ->
    case os:getenv(K) of
        false -> D;
        "" -> D;
        V -> V
    end.

env_int(K, D) ->
    list_to_integer(env(K, integer_to_list(D))).

env_int_opt(K) ->
    case os:getenv(K) of
        false -> undefined;
        "" -> undefined;
        V -> list_to_integer(V)
    end.

env_bool(K) ->
    case os:getenv(K) of
        "1" -> true;
        "true" -> true;
        _ -> false
    end.

build_env_opts(Cfg) ->
    Base = [{map_size, maps:get(map_size, Cfg)}],
    L1 = maybe_add(batch_size, Cfg, Base),
    L2 = maybe_add(flush_bytes, Cfg, L1),
    L3 = case maps:get(flush_idle_seconds, Cfg) of
        undefined -> L2;
        V -> [{flush_idle_timeout_seconds, V} | L2]
    end,
    L4 = case maps:get(no_sync, Cfg) of
        true -> [no_sync | L3];
        false -> L3
    end,
    case maps:get(write_map, Cfg) of
        true -> [write_map | L4];
        false -> L4
    end.

maybe_add(Key, Cfg, L) ->
    case maps:get(Key, Cfg) of
        undefined -> L;
        V -> [{Key, V} | L]
    end.

%% =====================================================================
%% Size presets
%% =====================================================================

parse_size("xsmall") -> {fixed, 5, 5};
parse_size("avg") -> {fixed, 50, 512};
parse_size("large") -> {fixed, 400, 1048576};
parse_size("custom:" ++ Rest) ->
    [K, V] = string:split(Rest, ",", all),
    {fixed, list_to_integer(K), list_to_integer(V)};
parse_size("random:" ++ Rest) ->
    [KR, VR] = string:split(Rest, ",", all),
    [KMin, KMax] = string:split(KR, "-", all),
    [VMin, VMax] = string:split(VR, "-", all),
    {ranged,
     list_to_integer(KMin), list_to_integer(KMax),
     list_to_integer(VMin), list_to_integer(VMax)};
parse_size(Other) -> throw({abort, bad_size_spec, Other}).

sizes_for({fixed, K, V}, _, _) -> {K, V};
sizes_for({ranged, KMin, KMax, VMin, VMax}, WI, Ctr) ->
    H = erlang:phash2({WI, Ctr}, 16#FFFFFFFF),
    K = KMin + (H rem (KMax - KMin + 1)),
    V = VMin + ((H bsr 16) rem (VMax - VMin + 1)),
    {K, V}.

max_v({fixed, _, V}) -> V;
max_v({ranged, _, _, _, VMax}) -> VMax.

min_k({fixed, K, _}) -> K;
min_k({ranged, KMin, _, _, _}) -> KMin.

avg_bytes_per_record({fixed, K, V}) -> K + V;
avg_bytes_per_record({ranged, KMin, KMax, VMin, VMax}) ->
    (KMin + KMax) div 2 + (VMin + VMax) div 2.

%% =====================================================================
%% Noise pool + generator
%% =====================================================================

-define(POOL_DEFAULT, 16 * 1024 * 1024).

make_pool(Spec) ->
    Need = max(?POOL_DEFAULT, 4 * max_v(Spec) + 4 * 4096),
    crypto:strong_rand_bytes(Need).

gen_key(Pool, Spec, WI, Ctr) ->
    {K, _} = sizes_for(Spec, WI, Ctr),
    gen_key_k(Pool, K, WI, Ctr).

gen_key_k(_, 1, WI, _) -> <<WI:8>>;
gen_key_k(_, 2, WI, Ctr) -> <<WI:8, (Ctr band 16#FF):8>>;
gen_key_k(_, 3, WI, Ctr) -> <<WI:8, (Ctr band 16#FFFF):16>>;
gen_key_k(_, 4, WI, Ctr) -> <<WI:8, (Ctr band 16#FFFFFF):24>>;
gen_key_k(_, 5, WI, Ctr) -> <<WI:8, (Ctr band 16#FFFFFFFF):32>>;
gen_key_k(Pool, K, WI, Ctr) ->
    Head = <<WI:8, (Ctr band 16#FFFFFFFF):32>>,
    Tail = K - 5,
    Off = (Ctr * 31 + WI * 7) rem (byte_size(Pool) - Tail),
    <<Head/binary, (binary_part(Pool, Off, Tail))/binary>>.

gen_val(Pool, Spec, WI, Ctr) ->
    {_, V} = sizes_for(Spec, WI, Ctr),
    case V of
        0 -> <<>>;
        _ ->
            Off = (Ctr * 131 + WI * 17) rem (byte_size(Pool) - V),
            binary_part(Pool, Off, V)
    end.

bench_generator() ->
    Spec = {fixed, 50, 512},
    Pool = make_pool(Spec),
    Dur = 200000,
    T0 = erlang:monotonic_time(microsecond),
    Bytes = bgen(Pool, Spec, 0, 0, T0, Dur),
    T1 = erlang:monotonic_time(microsecond),
    Rate = Bytes * 1000000 / max(1, T1 - T0),
    io:format("Generator: ~.2f GiB/s (~p bytes in ~p us)~n",
              [Rate / 1073741824, Bytes, T1 - T0]),
    Rate.

bgen(Pool, Spec, Ctr, Bytes, T0, Dur) ->
    case erlang:monotonic_time(microsecond) - T0 >= Dur of
        true -> Bytes;
        false ->
            K = gen_key(Pool, Spec, 0, Ctr),
            V = gen_val(Pool, Spec, 0, Ctr),
            bgen(Pool, Spec, Ctr + 1,
                 Bytes + byte_size(K) + byte_size(V), T0, Dur)
    end.

%% Threshold of 2 GiB/s — comfortably above any realistic disk throughput
%% (NVMe tops out around 1 GiB/s sequential-write), so writers will still
%% outrun the disk by 2x+ and keep backpressure hot.
run_generator_check() ->
    case bench_generator() of
        R when R >= 2 * 1073741824 -> ok;
        R -> {error, slow, R}
    end.

%% =====================================================================
%% Read mix
%% =====================================================================

parse_mix(Str) ->
    Parts = string:split(Str, ",", all),
    L = [parse_mix_part(P) || P <- Parts],
    Total = lists:sum([W || {_, W} <- L]),
    case Total of
        0 -> throw({abort, bad_read_mix, Str});
        _ -> {L, Total}
    end.

parse_mix_part(P) ->
    [N, W] = string:split(P, ":", all),
    {list_to_atom(string:trim(N)), list_to_integer(string:trim(W))}.

pick_op({L, Total}) ->
    N = rand:uniform(Total),
    pick_op_loop(L, N, 0).
pick_op_loop([{Op, W} | Rest], N, Acc) ->
    case Acc + W >= N of
        true -> Op;
        false -> pick_op_loop(Rest, N, Acc + W)
    end;
pick_op_loop([], _, _) -> point_random.

%% =====================================================================
%% Writers
%% =====================================================================

spawn_writer(Coord, DB, Pool, Spec, Counters, WI, MaxBytes) ->
    spawn_link(fun() ->
        H = histogram:new(),
        writer_loop(Coord, DB, Pool, Spec, Counters, WI, MaxBytes, H,
                    0, 0, 0)
    end).

writer_loop(Coord, DB, Pool, Spec, Counters, WI, MaxBytes, H, Ctr, Errs, Slow) ->
    case atomics:get(Counters, 2) of
        1 ->
            writer_finish(Coord, WI, H, Ctr, Errs, Slow);
        _ ->
            K = gen_key(Pool, Spec, WI, Ctr),
            V = gen_val(Pool, Spec, WI, Ctr),
            Sz = byte_size(K) + byte_size(V),
            T0 = erlang:monotonic_time(microsecond),
            R = (catch elmdb:put(DB, K, V)),
            T1 = erlang:monotonic_time(microsecond),
            Lat = T1 - T0,
            histogram:record(H, Lat),
            case R of
                ok ->
                    atomics:add(Counters, 3 + WI, 1),
                    Total = atomics:add_get(Counters, 1, Sz),
                    Slow2 = case Lat > 1000 of
                        true -> Slow + 1;
                        false -> Slow
                    end,
                    case Total >= MaxBytes of
                        true ->
                            atomics:put(Counters, 2, 1),
                            writer_finish(Coord, WI, H, Ctr + 1, Errs, Slow2);
                        false ->
                            writer_loop(Coord, DB, Pool, Spec, Counters,
                                        WI, MaxBytes, H, Ctr + 1, Errs, Slow2)
                    end;
                _ ->
                    writer_loop(Coord, DB, Pool, Spec, Counters, WI,
                                MaxBytes, H, Ctr + 1, Errs + 1, Slow)
            end
    end.

writer_finish(Coord, WI, H, Ctr, Errs, Slow) ->
    Coord ! {writer_done, WI, #{
        writes => Ctr,
        errors => Errs,
        slow_puts => Slow,
        hist => histogram:to_list(H)
    }}.

%% =====================================================================
%% Readers
%% =====================================================================

spawn_reader(DB, Pool, Spec, Counters, NumWriters, Mix, RangeSize) ->
    spawn_link(fun() ->
        Hist = #{point => histogram:new(),
                 range => histogram:new(),
                 match => histogram:new()},
        Stats = #{point_hit => 0, point_miss => 0,
                  range_ops => 0, range_keys => 0,
                  match_ops => 0, match_results => 0,
                  errors => 0},
        reader_loop(DB, Pool, Spec, Counters, NumWriters, Mix, RangeSize,
                    Hist, Stats)
    end).

reader_loop(DB, Pool, Spec, Counters, NumWriters, Mix, RangeSize, Hist, Stats) ->
    receive
        {coord_stop, From, Ref} ->
            From ! {reader_done, Ref, Stats#{
                hist => #{
                    point => histogram:to_list(maps:get(point, Hist)),
                    range => histogram:to_list(maps:get(range, Hist)),
                    match => histogram:to_list(maps:get(match, Hist))
                }
            }}
    after 0 ->
        WI = rand:uniform(NumWriters) - 1,
        WCount = atomics:get(Counters, 3 + WI),
        case WCount of
            0 ->
                receive
                    {coord_stop, From, Ref} ->
                        From ! {reader_done, Ref, Stats#{
                            hist => #{
                                point => histogram:to_list(maps:get(point, Hist)),
                                range => histogram:to_list(maps:get(range, Hist)),
                                match => histogram:to_list(maps:get(match, Hist))
                            }
                        }}
                after 5 -> ok
                end,
                reader_loop(DB, Pool, Spec, Counters, NumWriters, Mix,
                            RangeSize, Hist, Stats);
            _ ->
                Op = pick_op(Mix),
                {Stats2, Hist2} = run_read_op(Op, DB, Pool, Spec, WI, WCount,
                                              RangeSize, Hist, Stats),
                reader_loop(DB, Pool, Spec, Counters, NumWriters, Mix,
                            RangeSize, Hist2, Stats2)
        end
    end.

run_read_op(point_random, DB, Pool, Spec, WI, WCount, _RS, Hist, Stats) ->
    Ctr = rand:uniform(WCount) - 1,
    do_point(DB, Pool, Spec, WI, Ctr, Hist, Stats);
run_read_op(zipfian_point, DB, Pool, Spec, WI, WCount, _RS, Hist, Stats) ->
    Ctr = zipf(WCount, 2.0),
    do_point(DB, Pool, Spec, WI, Ctr, Hist, Stats);
run_read_op(range_random, DB, _Pool, _Spec, WI, _WCount, _RS, Hist, Stats) ->
    Prefix = <<WI:8, (rand:uniform(16#1000000) - 1):24>>,
    do_range(DB, Prefix, Hist, Stats);
run_read_op(zipfian_range, DB, _Pool, _Spec, WI, _WCount, _RS, Hist, Stats) ->
    Prefix = <<WI:8, (zipf(16#1000000, 1.5)):24>>,
    do_range(DB, Prefix, Hist, Stats);
run_read_op(match_random, DB, _Pool, _Spec, _WI, _WCount, _RS, Hist, Stats) ->
    %% match/2 with patterns we know won't match anything — measures the
    %% pure full-table-scan cost without confounding from "matches found".
    %% The bench's keys have no '/' so all keys hash to (full_key, empty
    %% suffix); pick a random suffix-key the scanner will never see.
    Patterns = [{rand:bytes(8), rand:bytes(8)}],
    do_match(DB, Patterns, Hist, Stats);
run_read_op(_, DB, Pool, Spec, WI, WCount, RS, Hist, Stats) ->
    run_read_op(point_random, DB, Pool, Spec, WI, WCount, RS, Hist, Stats).

do_point(DB, Pool, Spec, WI, Ctr, Hist, Stats) ->
    K = gen_key(Pool, Spec, WI, Ctr),
    T0 = erlang:monotonic_time(microsecond),
    R = (catch elmdb:get(DB, K)),
    T1 = erlang:monotonic_time(microsecond),
    histogram:record(maps:get(point, Hist), T1 - T0),
    case R of
        {ok, _} ->
            {Stats#{point_hit := maps:get(point_hit, Stats) + 1}, Hist};
        not_found ->
            {Stats#{point_miss := maps:get(point_miss, Stats) + 1}, Hist};
        _ ->
            {Stats#{errors := maps:get(errors, Stats) + 1}, Hist}
    end.

do_range(DB, Prefix, Hist, Stats) ->
    T0 = erlang:monotonic_time(microsecond),
    R = (catch elmdb:list(DB, Prefix)),
    T1 = erlang:monotonic_time(microsecond),
    histogram:record(maps:get(range, Hist), T1 - T0),
    N = case R of
        {ok, L} when is_list(L) -> length(L);
        L when is_list(L) -> length(L);
        not_found -> 0;
        _ -> -1
    end,
    case N of
        -1 ->
            {Stats#{errors := maps:get(errors, Stats) + 1}, Hist};
        _ ->
            {Stats#{range_ops := maps:get(range_ops, Stats) + 1,
                    range_keys := maps:get(range_keys, Stats) + N}, Hist}
    end.

do_match(DB, Patterns, Hist, Stats) ->
    T0 = erlang:monotonic_time(microsecond),
    R = (catch elmdb:match(DB, Patterns)),
    T1 = erlang:monotonic_time(microsecond),
    histogram:record(maps:get(match, Hist), T1 - T0),
    N = case R of
        {ok, L} when is_list(L) -> length(L);
        not_found -> 0;
        _ -> -1
    end,
    case N of
        -1 ->
            {Stats#{errors := maps:get(errors, Stats) + 1}, Hist};
        _ ->
            {Stats#{match_ops := maps:get(match_ops, Stats) + 1,
                    match_results := maps:get(match_results, Stats) + N}, Hist}
    end.

zipf(N, S) when N > 0 ->
    U = rand:uniform_real(),
    Idx = trunc(N * math:pow(U, S)),
    if Idx >= N -> N - 1;
       Idx < 0  -> 0;
       true     -> Idx
    end;
zipf(_, _) -> 0.

stop_readers(Pids, TimeoutMs) ->
    Ref = make_ref(),
    [P ! {coord_stop, self(), Ref} || P <- Pids],
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    Results = collect_reader_replies(length(Pids), Ref, Deadline, []),
    %% Force-kill anything still alive.
    [exit(P, kill) || P <- Pids, erlang:is_process_alive(P)],
    Results.

collect_reader_replies(0, _Ref, _Dl, Acc) -> Acc;
collect_reader_replies(N, Ref, Deadline, Acc) ->
    Now = erlang:monotonic_time(millisecond),
    case Deadline - Now of
        Left when Left > 0 ->
            receive
                {reader_done, Ref, R} ->
                    collect_reader_replies(N - 1, Ref, Deadline, [R | Acc])
            after Left ->
                Acc
            end;
        _ -> Acc
    end.

%% =====================================================================
%% Watchdogs
%% =====================================================================

%% Gate on anonymous RSS, not total RSS. File-backed RSS (LMDB mmap working
%% set) is reclaimable for free under any real memory pressure, so failing
%% on it gives false positives: Linux happily keeps clean file-backed pages
%% resident when there's no pressure, and the bench would abort while the
%% application has actually allocated nothing. Anon RSS = BEAM heap + Rust
%% overlay + native allocations, which is what "memory the application is
%% holding" actually means.
mem_watch_loop(Coord, CapMib) ->
    Cap = CapMib * 1024 * 1024,
    timer:sleep(100),
    {Anon, Rss} = current_anon_and_rss(),
    case Anon > Cap of
        true ->
            Coord ! {abort, mem_cap_exceeded,
                     [{anon_bytes, Anon},
                      {rss_bytes, Rss},
                      {cap_bytes, Cap},
                      {gated_on, anon_rss}]};
        false ->
            mem_watch_loop(Coord, CapMib)
    end.

current_anon_and_rss() ->
    Smaps = case file:read_file("/proc/self/smaps_rollup") of
        {ok, S} -> S;
        _ -> <<>>
    end,
    Anon = parse_proc_kb(Smaps, <<"Anonymous:">>),
    Rss = case parse_proc_kb(Smaps, <<"Rss:">>) of
        0 -> current_rss_fallback();
        R -> R
    end,
    {Anon, Rss}.

current_rss_fallback() ->
    case file:read_file("/proc/self/status") of
        {ok, B} -> parse_proc_kb(B, <<"VmRSS:">>);
        _ -> 0
    end.

parse_proc_kb(Bin, Field) ->
    case binary:match(Bin, Field) of
        nomatch -> 0;
        {Pos, Len} ->
            Tail = binary:part(Bin, Pos + Len, byte_size(Bin) - Pos - Len),
            [Line | _] = binary:split(Tail, <<"\n">>),
            case string:to_integer(string:trim(Line)) of
                {Int, _} when is_integer(Int) -> Int * 1024;
                _ -> 0
            end
    end.

disk_watch_loop(Coord, Path) ->
    timer:sleep(1000),
    case free_bytes(Path) of
        F when is_integer(F), F >= 0, F < 1073741824 ->
            Coord ! {abort, disk_full,
                     [{free_bytes, F}, {min_required, 1073741824}]};
        _ ->
            disk_watch_loop(Coord, Path)
    end.

free_bytes(Path) ->
    Cmd = "df -B1 --output=avail " ++ shell_quote(Path) ++ " 2>/dev/null | tail -n1",
    Out = os:cmd(Cmd),
    case string:to_integer(string:trim(Out)) of
        {Int, _} when is_integer(Int) -> Int;
        _ -> -1
    end.

shell_quote(S) ->
    "'" ++ lists:flatten([case C of $' -> "'\\''"; _ -> C end || C <- S]) ++ "'".

%% =====================================================================
%% Overlay sampler
%% =====================================================================

overlay_sampler_loop(DB, IntervalMs, Acc) ->
    receive
        {get, From, Ref} ->
            From ! {overlay, Ref, lists:reverse(Acc)},
            overlay_sampler_loop(DB, IntervalMs, Acc);
        stop -> ok
    after IntervalMs ->
        Ts = erlang:monotonic_time(millisecond),
        N = case (catch elmdb:overlay_count(DB)) of
            X when is_integer(X) -> X;
            _ -> 0
        end,
        overlay_sampler_loop(DB, IntervalMs, [{Ts, N} | Acc])
    end.

capture_overlay(Pid) ->
    Ref = make_ref(),
    Pid ! {get, self(), Ref},
    receive
        {overlay, Ref, L} -> L
    after 2000 -> []
    end.

%% =====================================================================
%% Coordinator: writer collection + watchdog mux
%% =====================================================================

await_writers(0, _T0, Acc, _C) ->
    {finalize_writer_results(Acc), undefined};
await_writers(Remaining, T0, Acc, Counters) ->
    receive
        {writer_done, _WI, R} ->
            await_writers(Remaining - 1, T0, [R | Acc], Counters);
        {abort, Reason, Detail} ->
            %% Stop writers and drain in-flight writer_done with a deadline.
            atomics:put(Counters, 2, 1),
            Drained = drain_writers(Remaining, Acc,
                erlang:monotonic_time(millisecond) + 2000),
            {finalize_writer_results(Drained), {abort, Reason, Detail}};
        time_limit ->
            atomics:put(Counters, 2, 1),
            Drained = drain_writers(Remaining, Acc,
                erlang:monotonic_time(millisecond) + 2000),
            {finalize_writer_results(Drained), {abort, time_limit, []}}
    end.

drain_writers(0, Acc, _Dl) -> Acc;
drain_writers(N, Acc, Dl) ->
    Now = erlang:monotonic_time(millisecond),
    case Dl - Now of
        Left when Left > 0 ->
            receive
                {writer_done, _WI, R} ->
                    drain_writers(N - 1, [R | Acc], Dl)
            after Left -> Acc
            end;
        _ -> Acc
    end.

finalize_writer_results(Rs) ->
    Hists = [maps:get(hist, R) || R <- Rs],
    Merged = case Hists of
        [] -> [];
        [H | Rest] -> lists:foldl(fun histogram:merge_list/2, H, Rest)
    end,
    Writes = lists:sum([maps:get(writes, R) || R <- Rs]),
    Errors = lists:sum([maps:get(errors, R) || R <- Rs]),
    Slow = lists:sum([maps:get(slow_puts, R) || R <- Rs]),
    #{
        per_writer_writes => [maps:get(writes, R) || R <- Rs],
        total_writes => Writes,
        total_errors => Errors,
        slow_puts => Slow,
        hist_summary => case Merged of
            [] -> #{count => 0};
            _ -> histogram:summary_from_list(Merged)
        end
    }.

%% =====================================================================
%% Result building + JSON dump
%% =====================================================================

build_result(Cfg, Spec, Mix, T0, Td, Tt, Tp,
             FlushUs, Outcome, AbortDetail,
             WriteSummary, ReaderResults, PostReaderResults,
             StartSample, EndSample, Samples, OverlayCounts) ->
    OverlaySecs = (Td - T0) / 1000000,
    TotalSecs = (Tt - T0) / 1000000,
    PostSecs = case Tp > Tt of
        true -> (Tp - Tt) / 1000000;
        false -> 0
    end,
    AvgBytes = avg_bytes_per_record(Spec),
    Writes = maps:get(total_writes, WriteSummary),
    AcceptedBytes = AvgBytes * Writes,
    Base = #{
        run_label => maps:get(run_label, Cfg),
        outcome => Outcome,
        abort_detail => AbortDetail,
        config => Cfg,
        size_spec => format_term(Spec),
        read_mix => format_term(Mix),
        timing => #{
            overlay_seconds => OverlaySecs,
            total_seconds => TotalSecs,
            post_load_seconds => PostSecs,
            flush_us => FlushUs
        },
        throughput => #{
            target_bytes => maps:get(max_bytes, Cfg),
            estimated_bytes_accepted => AcceptedBytes,
            overlay_bytes_per_s => safe_div(AcceptedBytes, OverlaySecs),
            total_bytes_per_s => safe_div(AcceptedBytes, TotalSecs),
            writes_per_s => safe_div(Writes, OverlaySecs)
        },
        writes => WriteSummary,
        reads => summarize_readers(ReaderResults),
        memory_start => StartSample,
        memory_end => EndSample,
        memory_summary => summarize_memory(Samples),
        memory_samples_count => length(Samples),
        overlay_counts => summarize_overlay(OverlayCounts)
    },
    case PostReaderResults of
        [] -> Base;
        _ -> Base#{reads_post_load => summarize_readers(PostReaderResults)}
    end.

safe_div(_, B) when B =< 0 -> 0;
safe_div(A, B) -> A / B.

summarize_readers(Rs) ->
    Sum = fun(K) -> lists:sum([maps:get(K, R, 0) || R <- Rs]) end,
    PointHists = [maps:get(point, maps:get(hist, R, #{}), [])
                  || R <- Rs, maps:is_key(hist, R)],
    RangeHists = [maps:get(range, maps:get(hist, R, #{}), [])
                  || R <- Rs, maps:is_key(hist, R)],
    MatchHists = [maps:get(match, maps:get(hist, R, #{}), [])
                  || R <- Rs, maps:is_key(hist, R)],
    #{
        point_hit => Sum(point_hit),
        point_miss => Sum(point_miss),
        range_ops => Sum(range_ops),
        range_keys => Sum(range_keys),
        match_ops => Sum(match_ops),
        match_results => Sum(match_results),
        errors => Sum(errors),
        point_hist => merge_and_summarize(PointHists),
        range_hist => merge_and_summarize(RangeHists),
        match_hist => merge_and_summarize(MatchHists)
    }.

merge_and_summarize(Hists) ->
    case lists:filter(fun(H) -> H =/= [] end, Hists) of
        [] -> #{count => 0};
        [H | Rest] ->
            Merged = lists:foldl(fun histogram:merge_list/2, H, Rest),
            histogram:summary_from_list(Merged)
    end.

summarize_memory(Samples) ->
    Pairs = lists:flatten([extract_pairs(S) || S <- Samples]),
    Series = lists:foldl(
        fun({K, V}, Acc) ->
            maps:update_with(K, fun(Old) -> [V | Old] end, [V], Acc)
        end, #{}, Pairs),
    maps:map(fun(_, Vs) -> series_summary(Vs) end, Series).

extract_pairs(S) ->
    L = [
        {beam_total,      get_in(S, [beam, total])},
        {beam_processes,  get_in(S, [beam, processes])},
        {beam_binary,     get_in(S, [beam, binary])},
        {beam_ets,        get_in(S, [beam, ets])},
        {proc_rss,        get_in(S, [proc, rss])},
        {proc_pss,        get_in(S, [proc, pss])},
        {proc_anon,       get_in(S, [proc, anon])},
        {proc_file,       get_in(S, [proc, file])},
        {proc_vm_rss,     get_in(S, [proc, vm_rss])},
        {proc_vm_hwm,     get_in(S, [proc, vm_hwm])},
        {cgroup_current,  get_in(S, [cgroup, current])},
        {cgroup_peak,     get_in(S, [cgroup, peak])},
        {cgroup_anon,     get_in(S, [cgroup, anon])},
        {cgroup_file,     get_in(S, [cgroup, file])}
    ],
    [{K, V} || {K, V} <- L, V =/= undefined].

get_in(undefined, _) -> undefined;
get_in(M, []) -> M;
get_in(M, [K | Rest]) when is_map(M) ->
    case maps:find(K, M) of
        {ok, V} -> get_in(V, Rest);
        error -> undefined
    end;
get_in(_, _) -> undefined.

series_summary(Values) ->
    Vs = [V || V <- Values, is_integer(V) orelse is_float(V)],
    case Vs of
        [] -> #{count => 0};
        _ ->
            Sorted = lists:sort(Vs),
            N = length(Sorted),
            #{
                count => N,
                min => lists:nth(1, Sorted),
                p50 => lists:nth(max(1, N div 2), Sorted),
                p95 => lists:nth(max(1, (N * 95) div 100), Sorted),
                p99 => lists:nth(max(1, (N * 99) div 100), Sorted),
                max => lists:last(Sorted),
                avg => lists:sum(Sorted) / N
            }
    end.

summarize_overlay([]) -> #{count => 0};
summarize_overlay(L) ->
    series_summary([N || {_T, N} <- L]).

format_term(T) ->
    iolist_to_binary(io_lib:format("~p", [T])).

%% Stdout summary printed at end of every run (catastrophic or ok).
%% Pure formatting from the same Result that goes to JSON.
print_summary(R) ->
    Outcome = maps:get(outcome, R),
    Tim = maps:get(timing, R),
    Th = maps:get(throughput, R),
    W = maps:get(writes, R),
    Reads = maps:get(reads, R),
    Mem = maps:get(memory_summary, R, #{}),
    Ov = maps:get(overlay_counts, R, #{count => 0}),
    WHist = maps:get(hist_summary, W, #{count => 0}),
    Line = lists:duplicate(72, $=),
    io:format("~n~s~n", [Line]),
    io:format("STRESS SUMMARY  label=~s  outcome=~s~n",
              [b(maps:get(run_label, R, <<"unlabeled">>)),
               atom_or_str(Outcome)]),
    case Outcome of
        ok -> ok;
        _ ->
            io:format("  abort_detail: ~s~n",
                      [b(maps:get(abort_detail, R, <<"unknown">>))])
    end,
    io:format("~s~n", [Line]),
    io:format("Timing       overlay=~.2fs  total=~.2fs  flush=~.3fms~n",
              [maps:get(overlay_seconds, Tim, 0.0),
               maps:get(total_seconds, Tim, 0.0),
               maps:get(flush_us, Tim, 0) / 1000.0]),
    io:format("Throughput   overlay=~s/s  total=~s/s  writes=~s/s~n",
              [hbytes(maps:get(overlay_bytes_per_s, Th, 0)),
               hbytes(maps:get(total_bytes_per_s, Th, 0)),
               hint(maps:get(writes_per_s, Th, 0))]),
    io:format("             accepted=~s of target=~s~n",
              [hbytes(maps:get(estimated_bytes_accepted, Th, 0)),
               hbytes(maps:get(target_bytes, Th, 0))]),
    io:format("Writes       count=~s  errors=~p  slow_puts(>1ms)=~p~n",
              [hint(maps:get(total_writes, W, 0)),
               maps:get(total_errors, W, 0),
               maps:get(slow_puts, W, 0)]),
    print_hist_line("  latency    ", WHist),
    print_read_block("Reads (under load)", Reads),
    case maps:get(reads_post_load, R, undefined) of
        undefined -> ok;
        Post ->
            io:format("Post-load phase: ~.2fs (no concurrent writers)~n",
                      [maps:get(post_load_seconds,
                                maps:get(timing, R, #{}),
                                0.0)]),
            print_read_block("Reads (clean)", Post)
    end,
    io:format("Memory       rss_max=~s  rss_p99=~s  rss_p50=~s~n",
              [hbytes(get_mem(Mem, proc_rss, max)),
               hbytes(get_mem(Mem, proc_rss, p99)),
               hbytes(get_mem(Mem, proc_rss, p50))]),
    io:format("             anon_max=~s  file_max=~s  beam_total_max=~s~n",
              [hbytes(get_mem(Mem, proc_anon, max)),
               hbytes(get_mem(Mem, proc_file, max)),
               hbytes(get_mem(Mem, beam_total, max))]),
    io:format("             cgroup_peak=~s~n",
              [hbytes(get_mem(Mem, cgroup_peak, max))]),
    io:format("Overlay      count_max=~s  p99=~s  p50=~s~n",
              [hint(maps:get(max, Ov, 0)),
               hint(maps:get(p99, Ov, 0)),
               hint(maps:get(p50, Ov, 0))]),
    io:format("~s~n~n", [Line]).

print_hist_line(_, #{count := 0}) ->
    ok;
print_hist_line(Label, H) ->
    io:format("~s  p50=~s  p95=~s  p99=~s  p999=~s  max=~s  avg=~s~n",
              [Label,
               us(maps:get(p50_us, H, 0)),
               us(maps:get(p95_us, H, 0)),
               us(maps:get(p99_us, H, 0)),
               us(maps:get(p999_us, H, 0)),
               us(maps:get(max_us, H, 0)),
               us(maps:get(avg_us, H, 0))]).

print_read_block(Label, Reads) ->
    Ph = maps:get(point_hit, Reads, 0),
    Pm = maps:get(point_miss, Reads, 0),
    Ro = maps:get(range_ops, Reads, 0),
    Rk = maps:get(range_keys, Reads, 0),
    Mo = maps:get(match_ops, Reads, 0),
    Mr = maps:get(match_results, Reads, 0),
    Re = maps:get(errors, Reads, 0),
    io:format("~s  point_hit=~p  point_miss=~p  "
              "range_ops=~p  range_keys=~p  "
              "match_ops=~p  match_results=~p  errors=~p~n",
              [pad(Label, 20), Ph, Pm, Ro, Rk, Mo, Mr, Re]),
    PHist = maps:get(point_hist, Reads, #{count => 0}),
    RHist = maps:get(range_hist, Reads, #{count => 0}),
    MHist = maps:get(match_hist, Reads, #{count => 0}),
    case maps:get(count, PHist, 0) of
        0 -> ok;
        _ -> print_hist_line("  point     ", PHist)
    end,
    case maps:get(count, RHist, 0) of
        0 -> ok;
        _ -> print_hist_line("  range     ", RHist)
    end,
    case maps:get(count, MHist, 0) of
        0 -> ok;
        _ -> print_hist_line("  match     ", MHist)
    end.

get_mem(Mem, Series, Key) ->
    case maps:get(Series, Mem, undefined) of
        undefined -> 0;
        M -> maps:get(Key, M, 0)
    end.

hbytes(N) when is_integer(N); is_float(N) ->
    Abs = abs(N),
    if
        Abs >= 1099511627776.0 -> io_lib:format("~.2f TiB", [N / 1099511627776.0]);
        Abs >= 1073741824.0    -> io_lib:format("~.2f GiB", [N / 1073741824.0]);
        Abs >= 1048576.0       -> io_lib:format("~.2f MiB", [N / 1048576.0]);
        Abs >= 1024.0          -> io_lib:format("~.2f KiB", [N / 1024.0]);
        true                   -> io_lib:format("~p B", [trunc(N)])
    end;
hbytes(_) -> "?".

hint(N) when is_integer(N); is_float(N) ->
    Abs = abs(N),
    if
        Abs >= 1.0e9 -> io_lib:format("~.2fG", [N / 1.0e9]);
        Abs >= 1.0e6 -> io_lib:format("~.2fM", [N / 1.0e6]);
        Abs >= 1.0e3 -> io_lib:format("~.2fk", [N / 1.0e3]);
        true         -> io_lib:format("~p", [trunc(N)])
    end;
hint(_) -> "?".

us(N) when is_integer(N); is_float(N) ->
    Abs = abs(N),
    if
        Abs >= 1.0e6 -> io_lib:format("~.2fs", [N / 1.0e6]);
        Abs >= 1.0e3 -> io_lib:format("~.2fms", [N / 1.0e3]);
        true         -> io_lib:format("~pus", [trunc(N)])
    end;
us(_) -> "?".

b(B) when is_binary(B) -> binary_to_list(B);
b(L) when is_list(L) -> L;
b(A) when is_atom(A) -> atom_to_list(A);
b(X) -> io_lib:format("~p", [X]).

atom_or_str(A) when is_atom(A) -> atom_to_list(A);
atom_or_str(X) -> b(X).

pad(S, Width) ->
    L = lists:flatten(io_lib:format("~s", [S])),
    case length(L) >= Width of
        true -> L;
        false -> L ++ lists:duplicate(Width - length(L), $\s)
    end.

write_result(Cfg, Result) ->
    ResultsDir = maps:get(results_dir, Cfg),
    ok = filelib:ensure_dir(filename:join(ResultsDir, "x")),
    Stamp = stamp(),
    Label = maps:get(run_label, Cfg),
    Size = maps:get(size, Cfg),
    W = integer_to_list(maps:get(writers, Cfg)),
    R = integer_to_list(maps:get(readers, Cfg)),
    Name = lists:flatten(io_lib:format("run_~s_~s_~sw_~sr_~s.json",
                                       [Label, Size, W, R, Stamp])),
    Path = filename:join(ResultsDir, Name),
    Json = to_json(Result),
    ok = file:write_file(Path, Json),
    io:format("Result written: ~s~n", [Path]).

stamp() ->
    {{Y, Mo, D}, {H, Mi, S}} = calendar:universal_time(),
    lists:flatten(io_lib:format("~4..0w~2..0w~2..0wT~2..0w~2..0w~2..0wZ",
                                [Y, Mo, D, H, Mi, S])).

log(Fmt) -> io:format("[stress] " ++ Fmt ++ "~n").
log(Fmt, Args) -> io:format("[stress] " ++ Fmt ++ "~n", Args).

%% =====================================================================
%% Tiny JSON encoder
%% =====================================================================

to_json(V) -> iolist_to_binary(j(V)).

j(M) when is_map(M) ->
    Pairs = [[$", esc_k(K), $", $:, j(V)] || {K, V} <- maps:to_list(M)],
    [${, lists:join($,, Pairs), $}];
j(B) when is_binary(B) ->
    [$", esc_s(B), $"];
j(true) -> "true";
j(false) -> "false";
j(null) -> "null";
j(undefined) -> "null";
j(A) when is_atom(A) ->
    [$", atom_to_list(A), $"];
j(I) when is_integer(I) -> integer_to_list(I);
j(F) when is_float(F) -> float_to_list(F, [{decimals, 6}, compact]);
j(T) when is_tuple(T) ->
    j(format_term(T));
j(L) when is_list(L) ->
    [$[, lists:join($,, [j(E) || E <- L]), $]].

esc_k(A) when is_atom(A) -> atom_to_list(A);
esc_k(B) when is_binary(B) -> esc_s(B);
esc_k(L) when is_list(L) -> L.

esc_s(B) when is_binary(B) ->
    [esc_c(C) || <<C>> <= B];
esc_s(L) when is_list(L) ->
    [esc_c(C) || C <- L].

esc_c($\\) -> "\\\\";
esc_c($") -> "\\\"";
esc_c($\n) -> "\\n";
esc_c($\r) -> "\\r";
esc_c($\t) -> "\\t";
esc_c(C) when C < 16#20 ->
    io_lib:format("\\u~4.16.0b", [C]);
esc_c(C) -> C.
