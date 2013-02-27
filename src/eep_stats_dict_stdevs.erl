%%%-------------------------------------------------------------------
%%% @author Morton Swimmer <morton at swimmer dot org>
%%% @copyright (C) 2013, Morton Swimmer
%%% @doc
%%% An aggregate function that computes sample
%%% variance for named entities
%%% @end
%%% Created : 27 Feb 2013 by Morton Swimmer
%%%-------------------------------------------------------------------

-module(eep_stats_dict_stdevs).

-include_lib("eep_erl.hrl").

-behaviour(eep_aggregate).

%% aggregate behaviour.
-export([init/0]).
-export([accumulate/2]).
-export([compensate/2]).
-export([emit/1]).

-record(stdevs, {
	  m2 = 0 :: number(),
	  m = 0 :: number(),
	  d = 0 :: number(),
	  n = 0 :: number()
	 }).

-record(state, {acc_dict}).

init() ->
    #state{acc_dict = dict:new()}.

accumulate(#state{acc_dict = Dict} = State, {Entity, X}) ->
    OldStDevs = get_entity_values(Entity, Dict),
    N = OldStDevs#stdevs.n + 1,
    D = X - OldStDevs#stdevs.m,
    M = (D / N) + OldStDevs#stdevs.m,
    M2 = OldStDevs#stdevs.m2 + D*(X - M),
    State#state{acc_dict = dict:store(Entity, #stdevs{m2 = M2, m = M, d = D, n = N}, Dict)}.

compensate(#state{acc_dict = Dict} = State, {Entity, X}) ->
    OldStDevs = get_entity_values(Entity, Dict),
    N = OldStDevs#stdevs.n - 1,
    D = OldStDevs#stdevs.m - X,
    M = OldStDevs#stdevs.m + (D / N),
    M2 = D*(X - M) + OldStDevs#stdevs.m2,
    State#state{acc_dict = dict:store(Entity, #stdevs{m2 = M2, m = M, d = D, n = N}, Dict)}.

emit(#state{acc_dict = Dict}) ->
    calculate_stdevs(Dict).

calculate_stdevs(Dict) ->
    dict:fold(fun(Entity, StDevs, Acc) -> 
		      dict:store(Entity, math:sqrt(StDevs#stdevs.m2 / (StDevs#stdevs.n - 1)), Acc) 
	      end, 
	      dict:new(), Dict).

get_entity_values(Entity, Dict) ->
    case dict:is_key(Entity, Dict) of
	true ->
	    dict:fetch(Entity, Dict);
	false ->
	    #stdevs{m2 = 0, m = 0, d = 0, n = 0}
    end.


-ifdef(TEST).

basic_test() ->
    R0 = init(),
    #state{acc_dict = D0} = R0,
    ?assertEqual(0, dict:size(D0)),
    R1 = accumulate(R0, {a, 1}),
    #state{acc_dict = D1} = R1,
    ?assertEqual(1, dict:size(D1)),
    V1 = dict:fetch(a, D1),
    ?assertEqual(1.0, V1#stdevs.m),

    R2 = accumulate(R1, {a, 8}),
    #state{acc_dict = D2} = R2,
    ?assertEqual(1, dict:size(D2)),
    V2 = dict:fetch(a, D2),
    ?assertEqual(4.5, V2#stdevs.m),

    ?assertEqual(4.949747468305833, dict:fetch(a, emit(R2))),

    R3 = accumulate(R2, {a, 1}),
    ?assertEqual(4.041451884327381, dict:fetch(a, emit(R3))),

    R4 = accumulate(R3, {a, 2}),
    ?assertEqual(3.366501646120693, dict:fetch(a, emit(R4))),

    R5 = compensate(R4, {a, 2}),
    ?assertEqual(4.041451884327381, dict:fetch(a, emit(R5))).

-endif.
