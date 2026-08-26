%%%-------------------------------------------------------------------
%%% @doc
%%% Test suite for duplicate-set support: dupsort/dupfixed databases,
%%% page_size/read_only/no_subdir environments, sorted appends and
%%% positioned duplicate reads.
%%% @end
%%%-------------------------------------------------------------------
-module(elmdb_dup_test).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test fixtures
%%%===================================================================

setup(EnvOpts, DbOpts) ->
    Dir = test_dir(),
    file:del_dir_r(Dir),
    filelib:ensure_dir(Dir ++ "/"),
    {ok, Env} = elmdb:env_open(Dir, [{map_size, 67108864} | EnvOpts]),
    {ok, DB} = elmdb:db_open(Env, DbOpts),
    {Dir, Env, DB}.

cleanup({Dir, Env, DB}) ->
    _ = elmdb:db_close(DB),
    _ = elmdb:env_close(Env),
    file:del_dir_r(Dir).

test_dir() ->
    Unique = erlang:unique_integer([positive]),
    filename:join(["/tmp", "elmdb_dup_test_" ++ integer_to_list(Unique)]).

%% 17-byte fixed-width item, big-endian so memcmp order is numeric.
item(N) ->
    <<N:136/big>>.

%% Parse one LMDB meta page at byte Offset of the data file. Layout: the
%% 24-byte page header, then MDB_meta: magic, version, address, mapsize,
%% mm_dbs[FREE] (whose md_pad holds the page size), mm_dbs[MAIN],
%% last_pg, txnid.
parse_meta(Bin, Offset) ->
    <<_:Offset/binary, _PageHdr:24/binary, Magic:32/little, Version:32/little,
      _Address:8/binary, _MapSize:8/binary, PSize:32/little,
      _FreeRest:44/binary, _MainPad:32/little, MainFlags:16/little,
      _MainDepth:16/little, _MainPages:24/binary, MainEntries:64/little,
      _MainRoot:64/little, _LastPg:64/little, TxnId:64/little,
      _/binary>> = Bin,
    #{magic => Magic, version => Version, psize => PSize,
      main_flags => MainFlags, main_entries => MainEntries, txnid => TxnId}.

%% Read the live meta page of a data file: pages 0 and 1 alternate per
%% commit, so take the one with the higher txnid.
meta(DataFile) ->
    {ok, Bin} = file:read_file(DataFile),
    Meta0 = parse_meta(Bin, 0),
    Meta1 = parse_meta(Bin, maps:get(psize, Meta0)),
    case maps:get(txnid, Meta1) > maps:get(txnid, Meta0) of
        true -> Meta1;
        false -> Meta0
    end.

%%%===================================================================
%%% Dup round-trip tests
%%%===================================================================

dup_round_trip_test_() ->
    {setup,
     fun() -> setup([], [create, dupsort]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     % Buffered puts of duplicates arrive unsorted; the set
                     % reads back sorted, and get returns the first (least)
                     % duplicate even while entries are pending.
                     ok = elmdb:put(DB, <<"set">>, <<"delta">>),
                     ok = elmdb:put(DB, <<"set">>, <<"alpha">>),
                     ok = elmdb:put(DB, <<"set">>, <<"charlie">>),
                     ?assertEqual({ok, <<"alpha">>}, elmdb:get(DB, <<"set">>)),
                     ?assertEqual(
                         {ok, [<<"alpha">>, <<"charlie">>, <<"delta">>]},
                         elmdb:read_dups(DB, <<"set">>, [])),

                     % Same values again: exact pairs are stored once.
                     ok = elmdb:put(DB, <<"set">>, <<"alpha">>),
                     ok = elmdb:flush(DB),
                     ?assertEqual(
                         {ok, [<<"alpha">>, <<"charlie">>, <<"delta">>]},
                         elmdb:read_dups(DB, <<"set">>, [])),
                     ?assertEqual({ok, <<"alpha">>}, elmdb:get(DB, <<"set">>))
                 end),
          ?_test(begin
                     % Two keys keep separate duplicate sets.
                     ok = elmdb:put_batch(DB, [
                         {<<"other">>, <<"one">>},
                         {<<"other">>, <<"two">>}
                     ]),
                     ?assertEqual(
                         {ok, [<<"one">>, <<"two">>]},
                         elmdb:read_dups(DB, <<"other">>, [])),
                     ?assertEqual(not_found, elmdb:read_dups(DB, <<"absent">>, []))
                 end),
          ?_test(begin
                     % Empty values cannot live in a dup set.
                     ?assertMatch({error, bad_val_size, _},
                                  elmdb:put(DB, <<"set">>, <<>>)),
                     ?assertMatch({error, validation_error, _},
                                  elmdb:put_batch(DB, [{<<"set">>, <<>>}]))
                 end)
         ]
     end}.

%%%===================================================================
%%% Append tests
%%%===================================================================

append_test_() ->
    {setup,
     fun() -> setup([{page_size, 65536}], [create, dupfixed]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     % A second append whose first pair continues the tail
                     % key's duplicate set must extend it in place.
                     ok = elmdb:put_batch_append(
                         DB, [{<<0>>, item(N)} || N <- lists:seq(1, 50)]),
                     ok = elmdb:put_batch_append(
                         DB, [{<<0>>, item(N)} || N <- lists:seq(51, 100)]),
                     {ok, All} = elmdb:read_dups(DB, <<0>>, []),
                     ?assertEqual([item(N) || N <- lists:seq(1, 100)], All)
                 end),
          ?_test(begin
                     % Out-of-order and duplicated pairs are refused before
                     % any write happens.
                     ?assertMatch({error, validation_error, _},
                                  elmdb:put_batch_append(
                                      DB, [{<<0>>, item(300)}, {<<0>>, item(200)}])),
                     ?assertMatch({error, validation_error, _},
                                  elmdb:put_batch_append(
                                      DB, [{<<0>>, item(300)}, {<<0>>, item(300)}])),
                     % A batch sorting below the database tail is refused by
                     % LMDB's append check.
                     ?assertMatch({error, key_exist, _},
                                  elmdb:put_batch_append(DB, [{<<0>>, item(7)}])),
                     % The refused batches left the set untouched.
                     {ok, Tail} = elmdb:read_dups(DB, <<0>>,
                                                  [{from, item(99)}]),
                     ?assertEqual([item(99), item(100)], Tail)
                 end)
         ]
     end}.

append_plain_db_test_() ->
    {setup,
     fun() -> setup([], [create]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     % Appends on a plain database require strictly ascending
                     % keys within the batch and after the existing tail.
                     ok = elmdb:put_batch_append(
                         DB, [{<<"a">>, <<"1">>}, {<<"b">>, <<"2">>}]),
                     ?assertEqual({ok, <<"1">>}, elmdb:get(DB, <<"a">>)),
                     ?assertMatch({error, validation_error, _},
                                  elmdb:put_batch_append(
                                      DB, [{<<"d">>, <<"3">>}, {<<"c">>, <<"4">>}])),
                     ?assertMatch({error, validation_error, _},
                                  elmdb:put_batch_append(
                                      DB, [{<<"d">>, <<"3">>}, {<<"d">>, <<"4">>}])),
                     ?assertMatch({error, key_exist, _},
                                  elmdb:put_batch_append(DB, [{<<"b">>, <<"5">>}])),
                     ok = elmdb:put_batch_append(DB, [{<<"c">>, <<"6">>}]),
                     ?assertEqual({ok, <<"6">>}, elmdb:get(DB, <<"c">>))
                 end)
         ]
     end}.

%%%===================================================================
%%% Positioned read tests
%%%===================================================================

positioned_read_test_() ->
    {setup,
     fun() ->
         State = {_, _, DB} = setup([], [create, dupfixed]),
         ok = elmdb:put_batch_append(
             DB, [{<<"k">>, item(N)} || N <- [2, 4, 6, 8, 10]]),
         State
     end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          % Forward: from lands on an exact member, between members, before
          % the first and past the last duplicate.
          ?_assertEqual({ok, [item(4), item(6), item(8), item(10)]},
                        elmdb:read_dups(DB, <<"k">>, [{from, item(4)}])),
          ?_assertEqual({ok, [item(6), item(8), item(10)]},
                        elmdb:read_dups(DB, <<"k">>, [{from, item(5)}])),
          ?_assertEqual({ok, [item(2), item(4), item(6), item(8), item(10)]},
                        elmdb:read_dups(DB, <<"k">>, [{from, item(0)}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"k">>, [{from, item(11)}])),
          % Limits bound the walk in both directions.
          ?_assertEqual({ok, [item(6), item(8)]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{from, item(5)}, {limit, 2}])),
          ?_assertEqual({ok, [item(2)]},
                        elmdb:read_dups(DB, <<"k">>, [{limit, 1}])),
          % Backward: values come in descending order, starting at the last
          % duplicate at or below from.
          ?_assertEqual({ok, [item(10), item(8), item(6), item(4), item(2)]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{direction, backward}])),
          ?_assertEqual({ok, [item(4), item(2)]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{from, item(5)}, {direction, backward}])),
          ?_assertEqual({ok, [item(4), item(2)]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{from, item(4)}, {direction, backward}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{from, item(1)}, {direction, backward}])),
          ?_assertEqual({ok, [item(10), item(8)]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{from, item(99)}, {direction, backward},
                                         {limit, 2}])),
          % Key presence separates an empty selection from a missing set.
          ?_assertEqual(not_found, elmdb:read_dups(DB, <<"missing">>, [])),
          % Malformed options are refused.
          ?_assertError(badarg, elmdb:read_dups(DB, <<"k">>, [{limit, wrong}])),
          ?_assertError(badarg, elmdb:read_dups(DB, <<"k">>,
                                                [{direction, sideways}]))
         ]
     end}.

single_dup_test_() ->
    {setup,
     fun() ->
         State = {_, _, DB} = setup([], [create, dupsort]),
         % A key with exactly one value stores it inline in the leaf node
         % rather than in a sub-page, exercising GET_BOTH_RANGE's
         % single-value comparison path.
         ok = elmdb:put(DB, <<"one">>, <<"m">>),
         ok = elmdb:flush(DB),
         State
     end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_assertEqual({ok, [<<"m">>]}, elmdb:read_dups(DB, <<"one">>, [])),
          ?_assertEqual({ok, [<<"m">>]},
                        elmdb:read_dups(DB, <<"one">>, [{from, <<"a">>}])),
          ?_assertEqual({ok, [<<"m">>]},
                        elmdb:read_dups(DB, <<"one">>, [{from, <<"m">>}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"one">>, [{from, <<"z">>}])),
          ?_assertEqual({ok, [<<"m">>]},
                        elmdb:read_dups(DB, <<"one">>,
                                        [{from, <<"z">>}, {direction, backward}])),
          ?_assertEqual({ok, [<<"m">>]},
                        elmdb:read_dups(DB, <<"one">>,
                                        [{from, <<"m">>}, {direction, backward}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"one">>,
                                        [{from, <<"a">>}, {direction, backward}])),
          ?_assertEqual({ok, [<<"m">>]},
                        elmdb:read_dups(DB, <<"one">>, [{prefix, <<"m">>}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"one">>, [{prefix, <<"n">>}]))
         ]
     end}.

prefix_read_test_() ->
    {setup,
     fun() ->
         State = {_, _, DB} = setup([], [create, dupfixed]),
         Values = [<<"a", 1>>, <<"a", 2>>, <<"b", 1>>, <<"b", 2>>, <<"c", 1>>,
                   <<255, 1>>, <<255, 2>>],
         ok = elmdb:put_batch_append(DB, [{<<"k">>, V} || V <- Values]),
         State
     end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_assertEqual({ok, [<<"b", 1>>, <<"b", 2>>]},
                        elmdb:read_dups(DB, <<"k">>, [{prefix, <<"b">>}])),
          ?_assertEqual({ok, [<<"b", 2>>, <<"b", 1>>]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<"b">>},
                                         {direction, backward}])),
          % from and prefix combine: the tighter bound wins in either
          % direction.
          ?_assertEqual({ok, [<<"b", 2>>]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<"b">>}, {from, <<"b", 2>>}])),
          ?_assertEqual({ok, [<<"b", 1>>]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<"b">>}, {from, <<"b", 1>>},
                                         {direction, backward}])),
          ?_assertEqual({ok, [<<"b", 2>>, <<"b", 1>>]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<"b">>}, {from, <<"z">>},
                                         {direction, backward}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"k">>, [{prefix, <<"d">>}])),
          ?_assertEqual({ok, []},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<"d">>},
                                         {direction, backward}])),
          % An all-0xff prefix has no successor: the backward walk starts at
          % the set's last duplicate.
          ?_assertEqual({ok, [<<255, 1>>, <<255, 2>>]},
                        elmdb:read_dups(DB, <<"k">>, [{prefix, <<255>>}])),
          ?_assertEqual({ok, [<<255, 2>>, <<255, 1>>]},
                        elmdb:read_dups(DB, <<"k">>,
                                        [{prefix, <<255>>},
                                         {direction, backward}]))
         ]
     end}.

%%%===================================================================
%%% Sub-page/sub-database boundary tests
%%%===================================================================

subpage_promotion_test_() ->
    {setup,
     fun() -> setup([{page_size, 512}], [create, dupfixed]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         Fifteen = fun(N) -> <<N:120/big>> end,
         [
          ?_test(begin
                     % Three 15-byte items stay in a sub-page inside the leaf
                     % node; positioned reads must work before promotion.
                     ok = elmdb:put_batch_append(
                         DB, [{<<0>>, Fifteen(N)} || N <- [1, 2, 3]]),
                     ?assertEqual({ok, [Fifteen(2), Fifteen(3)]},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(2)}])),
                     ?assertEqual({ok, [Fifteen(2), Fifteen(1)]},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(2)},
                                                   {direction, backward}]))
                 end),
          ?_test(begin
                     % Growing the set to 3000 items promotes it to a
                     % sub-database of LEAF2 pages holding (512-24) div 15 =
                     % 32 items each; reads must be seamless across leaf
                     % boundaries.
                     ok = elmdb:put_batch_append(
                         DB, [{<<0>>, Fifteen(N)} || N <- lists:seq(4, 3000)]),
                     ?assertEqual({ok, [Fifteen(N) || N <- lists:seq(1, 3000)]},
                                  elmdb:read_dups(DB, <<0>>, [])),
                     % Positioned reads around the first leaf boundary.
                     ?assertEqual({ok, [Fifteen(32), Fifteen(33), Fifteen(34)]},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(32)},
                                                   {limit, 3}])),
                     ?assertEqual({ok, [Fifteen(33), Fifteen(32), Fifteen(31)]},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(33)},
                                                   {direction, backward},
                                                   {limit, 3}])),
                     ?assertEqual({ok, [Fifteen(3000)]},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(3000)}])),
                     ?assertEqual({ok, []},
                                  elmdb:read_dups(DB, <<0>>,
                                                  [{from, Fifteen(3001)}]))
                 end)
         ]
     end}.

leaf_exact_fit_test_() ->
    {setup,
     fun() -> setup([{page_size, 512}], [create, dupfixed]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         Fifteen = fun(N) -> <<N:120/big>> end,
         [
          ?_test(begin
                     % 32 items exactly fill one 512-byte LEAF2 page; the
                     % 33rd forces a split. Reads at both sizes must agree
                     % with the model.
                     ok = elmdb:put_batch_append(
                         DB, [{<<1>>, Fifteen(N)} || N <- lists:seq(1, 32)]),
                     ?assertEqual({ok, [Fifteen(N) || N <- lists:seq(1, 32)]},
                                  elmdb:read_dups(DB, <<1>>, [])),
                     ?assertEqual({ok, [Fifteen(32)]},
                                  elmdb:read_dups(DB, <<1>>,
                                                  [{from, Fifteen(32)}])),
                     ok = elmdb:put_batch_append(DB, [{<<1>>, Fifteen(33)}]),
                     ?assertEqual({ok, [Fifteen(N) || N <- lists:seq(1, 33)]},
                                  elmdb:read_dups(DB, <<1>>, [])),
                     ?assertEqual({ok, [Fifteen(33), Fifteen(32)]},
                                  elmdb:read_dups(DB, <<1>>,
                                                  [{direction, backward},
                                                   {limit, 2}]))
                 end)
         ]
     end}.

%%%===================================================================
%%% Environment format tests
%%%===================================================================

page_size_meta_test_() ->
    [
     ?_test(begin
                {Dir, Env, DB} = setup([{page_size, 65536}], [create, dupfixed]),
                ok = elmdb:put_batch_append(
                    DB, [{<<0>>, item(N)} || N <- [1, 2, 3]]),
                ok = elmdb:env_close(Env),
                % The meta page proves the format: LMDB magic, data version
                % 3 (LMDB 1.0), the configured page size, DUPSORT|DUPFIXED
                % (0x14) on the main DB and one md_entries per duplicate.
                Meta = meta(filename:join(Dir, "data.mdb")),
                ?assertEqual(16#BEEFC0DE, maps:get(magic, Meta)),
                ?assertEqual(3, maps:get(version, Meta)),
                ?assertEqual(65536, maps:get(psize, Meta)),
                ?assertEqual(16#14, maps:get(main_flags, Meta) band 16#14),
                ?assertEqual(3, maps:get(main_entries, Meta)),
                file:del_dir_r(Dir)
            end),
     ?_test(begin
                {Dir, Env, DB} = setup([{page_size, 512}], [create]),
                ok = elmdb:put(DB, <<"k">>, <<"v">>),
                ok = elmdb:flush(DB),
                ok = elmdb:env_close(Env),
                Meta = meta(filename:join(Dir, "data.mdb")),
                ?assertEqual(3, maps:get(version, Meta)),
                ?assertEqual(512, maps:get(psize, Meta)),
                ?assertEqual(0, maps:get(main_flags, Meta) band 16#14),
                file:del_dir_r(Dir)
            end),
     % Invalid page sizes are refused outright.
     ?_assertError(badarg, elmdb:env_open(test_dir(), [{page_size, 1000}])),
     ?_assertError(badarg, elmdb:env_open(test_dir(), [{page_size, 256}])),
     ?_assertError(badarg, elmdb:env_open(test_dir(), [{page_size, 131072}]))
    ].

read_only_env_test_() ->
    [
     ?_test(begin
                Dir = test_dir(),
                file:del_dir_r(Dir),
                filelib:ensure_dir(Dir ++ "/"),
                {ok, Env} = elmdb:env_open(Dir, [{map_size, 67108864}]),
                {ok, DB} = elmdb:db_open(Env, [create, dupsort]),
                ok = elmdb:put(DB, <<"k">>, <<"v1">>),
                ok = elmdb:put(DB, <<"k">>, <<"v2">>),
                ok = elmdb:flush(DB),
                ok = elmdb:env_close(Env),
                % Reopen read-only: reads work, writes are refused.
                {ok, Env2} = elmdb:env_open(Dir, [{map_size, 67108864},
                                                  read_only]),
                {ok, DB2} = elmdb:db_open(Env2, [dupsort]),
                ?assertEqual({ok, <<"v1">>}, elmdb:get(DB2, <<"k">>)),
                ?assertEqual({ok, [<<"v1">>, <<"v2">>]},
                             elmdb:read_dups(DB2, <<"k">>, [])),
                ?assertMatch({error, _, _},
                             elmdb:put_batch_append(DB2, [{<<"z">>, <<"1">>}])),
                ok = elmdb:env_close(Env2),
                file:del_dir_r(Dir)
            end)
    ].

no_subdir_env_test_() ->
    [
     ?_test(begin
                Dir = test_dir(),
                file:del_dir_r(Dir),
                filelib:ensure_dir(Dir ++ "/"),
                % With no_subdir the path names the data file itself.
                DataFile = filename:join(Dir, "index.lmdb"),
                {ok, Env} = elmdb:env_open(DataFile,
                                           [{map_size, 67108864},
                                            {page_size, 65536}, no_subdir]),
                {ok, DB} = elmdb:db_open(Env, [create, dupfixed]),
                ok = elmdb:put_batch_append(
                    DB, [{<<0>>, item(N)} || N <- [1, 2]]),
                ok = elmdb:env_close(Env),
                ?assert(filelib:is_regular(DataFile)),
                Meta = meta(DataFile),
                ?assertEqual(3, maps:get(version, Meta)),
                ?assertEqual(65536, maps:get(psize, Meta)),
                % The file reopens read-only through the same path.
                {ok, Env2} = elmdb:env_open(DataFile,
                                            [{map_size, 67108864}, no_subdir,
                                             read_only]),
                {ok, DB2} = elmdb:db_open(Env2, [dupfixed]),
                ?assertEqual({ok, [item(1), item(2)]},
                             elmdb:read_dups(DB2, <<0>>, [])),
                ok = elmdb:env_close(Env2),
                file:del_dir_r(Dir)
            end)
    ].

%%%===================================================================
%%% Mode-compatibility tests
%%%===================================================================

incompatible_mode_test_() ->
    {setup,
     fun() -> setup([], [create, dupsort]) end,
     fun cleanup/1,
     fun({_Dir, Env, DB}) ->
         [
          ?_test(begin
                     ok = elmdb:put(DB, <<"k">>, <<"v">>),
                     ok = elmdb:flush(DB),
                     % A database stays in the mode it was opened with.
                     ?assertMatch({error, incompatible, _},
                                  elmdb:db_open(Env, [create])),
                     ?assertMatch({error, incompatible, _},
                                  elmdb:db_open(Env, [create, dupfixed]))
                 end)
         ]
     end}.

non_dup_read_dups_test_() ->
    {setup,
     fun() -> setup([], [create]) end,
     fun cleanup/1,
     fun({_Dir, _Env, DB}) ->
         [
          ?_test(begin
                     ok = elmdb:put(DB, <<"k">>, <<"v">>),
                     ?assertMatch({error, incompatible, _},
                                  elmdb:read_dups(DB, <<"k">>, []))
                 end)
         ]
     end}.

%%%===================================================================
%%% Mixed workload: plain databases are unaffected by dup support
%%%===================================================================

mixed_workload_test_() ->
    {setup,
     fun() ->
         Plain = setup([], [create]),
         Dup = setup([], [create, dupsort]),
         {Plain, Dup}
     end,
     fun({Plain, Dup}) ->
         cleanup(Plain),
         cleanup(Dup)
     end,
     fun({{_, _, PlainDB}, {_, _, DupDB}}) ->
         [
          ?_test(begin
                     % The plain database keeps replace semantics while the
                     % dup database accumulates, through the same code paths.
                     ok = elmdb:put(PlainDB, <<"k">>, <<"v1">>),
                     ok = elmdb:put(DupDB, <<"k">>, <<"v1">>),
                     ok = elmdb:put(PlainDB, <<"k">>, <<"v2">>),
                     ok = elmdb:put(DupDB, <<"k">>, <<"v2">>),
                     ok = elmdb:flush(PlainDB),
                     ok = elmdb:flush(DupDB),
                     ?assertEqual({ok, <<"v2">>}, elmdb:get(PlainDB, <<"k">>)),
                     ?assertEqual({ok, [<<"v1">>, <<"v2">>]},
                                  elmdb:read_dups(DupDB, <<"k">>, [])),

                     % Hierarchical listing and prefix reads still work on
                     % the plain database.
                     ok = elmdb:put(PlainDB, <<"group/a">>, <<"1">>),
                     ok = elmdb:put(PlainDB, <<"group/b">>, <<"2">>),
                     ok = elmdb:flush(PlainDB),
                     ?assertEqual({ok, [<<"a">>, <<"b">>]},
                                  elmdb:list(PlainDB, <<"group/">>)),
                     ?assertEqual(
                         {ok, [{<<"group/a">>, <<"1">>},
                               {<<"group/b">>, <<"2">>}]},
                         elmdb:read_prefix(PlainDB, <<"group/">>)),

                     % put_batch_direct keeps working on both modes.
                     ok = elmdb:put_batch_direct(PlainDB, [{<<"x">>, <<"1">>}]),
                     ok = elmdb:put_batch_direct(DupDB, [{<<"k">>, <<"v0">>}]),
                     ?assertEqual({ok, <<"1">>}, elmdb:get(PlainDB, <<"x">>)),
                     ?assertEqual({ok, [<<"v0">>, <<"v1">>, <<"v2">>]},
                                  elmdb:read_dups(DupDB, <<"k">>, []))
                 end)
         ]
     end}.

%%%===================================================================
%%% Model-based fuzz of positioned reads
%%%===================================================================

read_dups_fuzz_test_() ->
    {timeout, 120,
     ?_test(begin
                State = {_, _, DB} = setup([], [create, dupsort]),
                rand:seed(exsss, {1, 2, 3}),
                % Random variable-length values inserted in random order via
                % the buffered path; the model is the sorted unique list.
                Values = lists:usort(
                    [rand_value() || _ <- lists:seq(1, 300)]),
                Shuffled = [V || {_, V} <- lists:sort(
                    [{rand:uniform(), V} || V <- Values])],
                lists:foreach(
                    fun(V) -> ok = elmdb:put(DB, <<"k">>, V) end,
                    Shuffled),
                lists:foreach(
                    fun(_) ->
                        Opts = rand_opts(Values),
                        Expected = model_read(Values, Opts),
                        ?assertEqual({ok, Expected},
                                     elmdb:read_dups(DB, <<"k">>, Opts))
                    end,
                    lists:seq(1, 400)),
                cleanup(State)
            end)}.

rand_value() ->
    rand:bytes(rand:uniform(6)).

rand_opts(Values) ->
    From = case rand:uniform(3) of
        1 -> [];
        2 -> [{from, lists:nth(rand:uniform(length(Values)), Values)}];
        3 -> [{from, rand_value()}]
    end,
    Prefix = case rand:uniform(3) of
        1 -> [];
        2 -> [{prefix, binary:part(
                  lists:nth(rand:uniform(length(Values)), Values), 0, 1)}];
        3 -> [{prefix, rand_value()}]
    end,
    Limit = case rand:uniform(3) of
        1 -> [];
        2 -> [{limit, rand:uniform(5)}];
        3 -> [{limit, 0}]
    end,
    Direction = case rand:uniform(2) of
        1 -> [];
        2 -> [{direction, backward}]
    end,
    From ++ Prefix ++ Limit ++ Direction.

%% The reference semantics of read_dups: filter the sorted set by the
%% bounds, orient it, then bound the count.
model_read(Values, Opts) ->
    From = proplists:get_value(from, Opts, undefined),
    Prefix = proplists:get_value(prefix, Opts, undefined),
    Limit = proplists:get_value(limit, Opts, 0),
    Backward = proplists:get_value(direction, Opts, forward) =:= backward,
    Bounded = [V || V <- Values,
                    From =:= undefined orelse
                        (not Backward andalso V >= From) orelse
                        (Backward andalso V =< From),
                    Prefix =:= undefined orelse is_value_prefix(Prefix, V)],
    Oriented = case Backward of
        true -> lists:reverse(Bounded);
        false -> Bounded
    end,
    case Limit of
        0 -> Oriented;
        _ -> lists:sublist(Oriented, Limit)
    end.

is_value_prefix(Prefix, Value) ->
    byte_size(Value) >= byte_size(Prefix) andalso
        binary:part(Value, 0, byte_size(Prefix)) =:= Prefix.

%%%===================================================================
%%% Append benchmark: 10M-item single-key dup set on 64 KiB pages
%%%===================================================================

dup_append_bench_test_() ->
    {timeout, 600,
     ?_test(begin
                Dir = test_dir(),
                file:del_dir_r(Dir),
                filelib:ensure_dir(Dir ++ "/"),
                {ok, Env} = elmdb:env_open(
                    Dir, [{map_size, 1073741824}, {page_size, 65536},
                          no_sync, no_mem_init]),
                {ok, DB} = elmdb:db_open(Env, [create, dupfixed]),
                Total = 10000000,
                ChunkSize = 100000,
                Chunks = Total div ChunkSize,
                Started = erlang:monotonic_time(microsecond),
                AppendMicros = lists:foldl(
                    fun(Chunk, Acc) ->
                        Base = Chunk * ChunkSize,
                        Pairs = [{<<0>>, item(Base + N)}
                                 || N <- lists:seq(0, ChunkSize - 1)],
                        T0 = erlang:monotonic_time(microsecond),
                        ok = elmdb:put_batch_append(DB, Pairs),
                        Acc + (erlang:monotonic_time(microsecond) - T0)
                    end,
                    0,
                    lists:seq(0, Chunks - 1)),
                WallMicros = erlang:monotonic_time(microsecond) - Started,
                ok = elmdb:env_sync(Env),
                % Spot-check the built set before closing.
                ?assertEqual({ok, [item(5000000), item(5000001)]},
                             elmdb:read_dups(DB, <<0>>,
                                             [{from, item(5000000)},
                                              {limit, 2}])),
                ?assertEqual({ok, [item(Total - 1)]},
                             elmdb:read_dups(DB, <<0>>,
                                             [{from, item(Total - 1)}])),
                ok = elmdb:env_close(Env),
                Meta = meta(filename:join(Dir, "data.mdb")),
                ?assertEqual(Total, maps:get(main_entries, Meta)),
                ?assertEqual(65536, maps:get(psize, Meta)),
                ?assertEqual(3, maps:get(version, Meta)),
                FileSize = filelib:file_size(filename:join(Dir, "data.mdb")),
                BytesPerRow = FileSize / Total,
                AppendRate = Total / (AppendMicros / 1000000),
                WallRate = Total / (WallMicros / 1000000),
                ?debugFmt(
                    "dup append bench: ~p rows, append ~.2f Mrows/s, "
                    "end-to-end ~.2f Mrows/s, file ~p bytes, ~.3f B/row",
                    [Total, AppendRate / 1000000, WallRate / 1000000,
                     FileSize, BytesPerRow]),
                % 17-byte items must land near the raw data size; the
                % validated ceiling is well under 18 B/row.
                ?assert(BytesPerRow < 18.0),
                file:del_dir_r(Dir)
            end)}.
