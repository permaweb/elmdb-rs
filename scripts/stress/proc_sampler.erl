%% Per-100ms memory sampler. Captures BEAM, OS-process (/proc), and cgroup
%% (cgroup v2) memory views so a single timeline can be sliced by layer.
%%
%% Started by the coordinator; returns its samples on get_samples/1 and is
%% killed at run end. Not designed to be a long-lived service.
-module(proc_sampler).

-export([start_link/1, stop/1, get_samples/1, snapshot/0]).

start_link(IntervalMs) ->
    Pid = spawn_link(fun() -> init(IntervalMs) end),
    {ok, Pid}.

stop(Pid) ->
    Pid ! stop,
    ok.

get_samples(Pid) ->
    Ref = make_ref(),
    Pid ! {get, self(), Ref},
    receive
        {samples, Ref, S} -> S
    after 5000 ->
        []
    end.

init(IntervalMs) ->
    erlang:send_after(IntervalMs, self(), tick),
    loop(IntervalMs, []).

loop(IntervalMs, Acc) ->
    receive
        tick ->
            erlang:send_after(IntervalMs, self(), tick),
            loop(IntervalMs, [snapshot() | Acc]);
        {get, From, Ref} ->
            From ! {samples, Ref, lists:reverse(Acc)},
            loop(IntervalMs, Acc);
        stop ->
            ok
    end.

%% Captured even when no sampler is running (used at run start/end for
%% bookkeeping snapshots).
snapshot() ->
    #{
        ts_ms => erlang:monotonic_time(millisecond),
        beam => beam_mem(),
        proc => proc_mem(),
        cgroup => cgroup_mem()
    }.

beam_mem() ->
    M = erlang:memory(),
    #{
        total => proplists:get_value(total, M, 0),
        processes => proplists:get_value(processes, M, 0),
        binary => proplists:get_value(binary, M, 0),
        ets => proplists:get_value(ets, M, 0),
        atom => proplists:get_value(atom, M, 0),
        code => proplists:get_value(code, M, 0)
    }.

proc_mem() ->
    Status = read_file_safe("/proc/self/status"),
    Smaps = read_file_safe("/proc/self/smaps_rollup"),
    Rss = parse_kb(Smaps, <<"Rss:">>),
    Anon = parse_kb(Smaps, <<"Anonymous:">>),
    File = max(0, Rss - Anon),
    #{
        vm_rss => parse_kb(Status, <<"VmRSS:">>),
        vm_hwm => parse_kb(Status, <<"VmHWM:">>),
        vm_size => parse_kb(Status, <<"VmSize:">>),
        rss => Rss,
        pss => parse_kb(Smaps, <<"Pss:">>),
        anon => Anon,
        file => File
    }.

cgroup_mem() ->
    case cgroup_dir() of
        undefined -> #{available => false};
        Path ->
            #{
                available => true,
                path => Path,
                current => read_int(filename:join(Path, "memory.current")),
                peak => read_int(filename:join(Path, "memory.peak")),
                anon => stat_field(filename:join(Path, "memory.stat"), <<"anon">>),
                file => stat_field(filename:join(Path, "memory.stat"), <<"file">>),
                kernel => stat_field(filename:join(Path, "memory.stat"), <<"kernel">>)
            }
    end.

cgroup_dir() ->
    case file:read_file("/proc/self/cgroup") of
        {ok, Bin} ->
            case binary:split(string:trim(Bin), <<"::">>) of
                [_, Rest] ->
                    [Sub | _] = binary:split(Rest, <<"\n">>),
                    SubStr = binary_to_list(string:trim(Sub)),
                    Path = "/sys/fs/cgroup" ++ SubStr,
                    case filelib:is_dir(Path) of
                        true -> Path;
                        false -> undefined
                    end;
                _ -> undefined
            end;
        _ -> undefined
    end.

read_file_safe(Path) ->
    case file:read_file(Path) of
        {ok, B} -> B;
        _ -> <<>>
    end.

read_int(Path) ->
    case file:read_file(Path) of
        {ok, B} ->
            T = string:trim(B),
            case T of
                <<"max">> -> -1;
                _ ->
                    try binary_to_integer(T) catch _:_ -> 0 end
            end;
        _ -> 0
    end.

%% memory.stat is a flat <field> <int> per line file.
stat_field(Path, Field) ->
    case file:read_file(Path) of
        {ok, B} -> scan_stat(B, Field);
        _ -> 0
    end.

scan_stat(B, Field) ->
    Lines = binary:split(B, <<"\n">>, [global]),
    scan_stat_lines(Lines, Field).
scan_stat_lines([], _) -> 0;
scan_stat_lines([Line | Rest], Field) ->
    case binary:split(Line, <<" ">>) of
        [Field, Val] ->
            try binary_to_integer(string:trim(Val)) catch _:_ -> 0 end;
        _ ->
            scan_stat_lines(Rest, Field)
    end.

%% Parse the integer following a label in a kB-suffixed /proc field.
parse_kb(Bin, Field) ->
    case binary:match(Bin, Field) of
        nomatch -> 0;
        {Pos, Len} ->
            Tail = binary:part(Bin, Pos + Len, byte_size(Bin) - Pos - Len),
            [Line | _] = binary:split(Tail, <<"\n">>),
            Stripped = string:trim(Line),
            %% Stripped looks like "12345 kB"
            case string:to_integer(Stripped) of
                {Int, _} when is_integer(Int) -> Int * 1024;
                _ -> 0
            end
    end.
