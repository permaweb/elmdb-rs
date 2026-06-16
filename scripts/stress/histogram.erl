%% Log-bucket latency histogram, microsecond scale.
%% 64 sub-buckets per power-of-2, 30 powers -> 1920 buckets total.
%% Backed by a counters/2 array so add/2 is lock-free per writer process.
-module(histogram).

-export([new/0, record/2, to_list/1, merge_list/2, summary_from_list/1]).

-define(SUB, 64).
-define(POWS, 30).
-define(N, (?SUB * ?POWS)).

new() ->
    counters:new(?N, [write_concurrency]).

record(H, Us) when is_integer(Us), Us =< 0 ->
    counters:add(H, 1, 1);
record(H, Us) when is_integer(Us) ->
    Idx = bucket(Us) + 1,
    counters:add(H, Idx, 1).

bucket(Us) ->
    P0 = floor_log2(Us),
    Pow = if P0 < 0 -> 0; P0 >= ?POWS -> ?POWS - 1; true -> P0 end,
    Base = 1 bsl Pow,
    Sub0 = (Us - Base) * ?SUB div Base,
    Sub = if Sub0 < 0 -> 0; Sub0 >= ?SUB -> ?SUB - 1; true -> Sub0 end,
    Pow * ?SUB + Sub.

floor_log2(N) when N < 1 -> -1;
floor_log2(N) -> floor_log2(N, 0).
floor_log2(N, A) when N < 2 -> A;
floor_log2(N, A) -> floor_log2(N bsr 1, A + 1).

to_list(H) ->
    [counters:get(H, I) || I <- lists:seq(1, ?N)].

merge_list(L1, L2) ->
    lists:zipwith(fun(A, B) -> A + B end, L1, L2).

summary_from_list(Counts) ->
    Total = lists:sum(Counts),
    case Total of
        0 -> #{count => 0};
        _ ->
            Sum = weighted_sum(Counts, 0, 0),
            #{
                count => Total,
                min_us => p_us(Counts, 0),
                p50_us => p_us(Counts, Total div 2),
                p95_us => p_us(Counts, (Total * 95) div 100),
                p99_us => p_us(Counts, (Total * 99) div 100),
                p999_us => p_us(Counts, (Total * 999) div 1000),
                max_us => p_us(Counts, Total - 1),
                avg_us => Sum / Total
            }
    end.

p_us(Counts, Rank) ->
    p_us_loop(Counts, Rank, 0, 0).
p_us_loop([], _, _, _) -> 0;
p_us_loop([C | Rest], Rank, Acc, Idx) ->
    case Acc + C > Rank of
        true -> bucket_lower_us(Idx);
        false -> p_us_loop(Rest, Rank, Acc + C, Idx + 1)
    end.

bucket_lower_us(Idx) ->
    Pow = Idx div ?SUB,
    Sub = Idx rem ?SUB,
    Base = 1 bsl Pow,
    Base + (Sub * Base) div ?SUB.

weighted_sum([], _, Acc) -> Acc;
weighted_sum([C | Rest], Idx, Acc) ->
    weighted_sum(Rest, Idx + 1, Acc + C * bucket_lower_us(Idx)).
