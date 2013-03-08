%%% @author Morton Swimmer <morton at swimmer dot org>
%%% @copyright (C) 2013, Morton Swimmer
%%% @doc
%%% An aggregation module for named entities that counts on a per-entity basis.
%%% Internally, it uses an Erlang dictionary so this can use a lot
%%% of memory if you let it run long and you have a large diversity 
%%% of entities.
%%% @end
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a
%%% copy of this software and associated documentation files (the
%%% "Software"), to deal in the Software without restriction, including
%%% without limitation the rights to use, copy, modify, merge, publish,
%%% distribute, sublicense, and/or sell copies of the Software, and to permit
%%% persons to whom the Software is furnished to do so, subject to the
%%% following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included
%%% in all copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
%%% OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
%%% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
%%% NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
%%% DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
%%% OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
%%% USE OR OTHER DEALINGS IN THE SOFTWARE.
%%%
%%% Created : 26 Feb 2013 by Morton Swimmer <morton at swimmer dot org>

-module(eep_stats_dict_dict_count).

-include_lib("eep_erl.hrl").

-behaviour(eep_aggregate).

%% aggregate behaviour.
-export([init/0]).
-export([accumulate/2]).
-export([compensate/2]).
-export([emit/1]).

-record(state, {acc_dict, dict_dict}).

%%--------------------------------------------------------------------
%% @doc
%% Initialize the dict counter with a new, fresh, dictionary.
%% @end
%%--------------------------------------------------------------------
-spec init() -> record().

init() ->
    #state{acc_dict = dict:new(), dict_dict = dict:new()}.

%%--------------------------------------------------------------------
%% @doc
%% Accumulate an event with key KeyName from the given dict Dict.
%% This means that KeyName will be used to get the value we are counting
%% from the Dict as opposed to getting it directly as a parameter as
%% in eep_stats_dict_count
%% @end
%%--------------------------------------------------------------------
-spec accumulate(State :: record(), {KeyName :: any(), Dict :: dict()}) -> record().

accumulate(#state{acc_dict = AccDict, dict_dict = DictDict} = State, {KeyName, Dict}) ->
    Key = dict:fetch(KeyName, Dict),
    NewDict = dict:erase(KeyName, Dict),
%    io:format("Key: ~p, NewDict: ~p~n", [Key, dict:to_list(NewDict)]),
    State#state{acc_dict = dict:update_counter(Key, 1, AccDict), dict_dict = dict:append(Key, dict:to_list(NewDict), DictDict)}.

%%--------------------------------------------------------------------
%% @doc
%% Compensate for an event with key KeyName from the given dict Dict
%% @end
%%--------------------------------------------------------------------
-spec compensate(State :: record(), {KeyName :: any(), Dict :: dict()}) -> record().

compensate(#state{acc_dict = AccDict, dict_dict = DictDict} = State, {KeyName, Dict}) ->
    Key = dict:fetch(KeyName, Dict),
    ListForKey = dict:fetch(Key, DictDict),
    NewListForKey = lists:nthtail(1, ListForKey),
    State#state{acc_dict = dict:update_counter(Key, -1, AccDict), dict_dict = dict:store(Key, NewListForKey, DictDict)}.

%%--------------------------------------------------------------------
%% @doc
%% Prepare the results for emit and return it.
%% You will get a dict() with a key and the value being a tuple contain
%% the count value and a list() of
%% of the records that led to this count.
%% @end
%% TODO: should we be returning a list() or a dict() in the value?
%%--------------------------------------------------------------------
-spec emit(record()) -> dict().

emit(#state{acc_dict = AccDict, dict_dict = DictDict}) ->
    dict:map(fun(K, Count) ->
		     {Count, dict:fetch(K, DictDict)}
	     end, AccDict).

-ifdef(TEST).

basic_test() ->
    R0 = init(),
    #state{acc_dict = D0} = R0,
    ?assertEqual(0, dict:size(D0)),

    R1 = accumulate(R0, {name, dict:from_list([{name, <<"apple">>}, {b, 2}, {c, 3}])}),
    #state{acc_dict = D1} = R1,
%    io:format("D1: ~p~n", [dict:to_list(D1)]),
    ?assertEqual(1, dict:size(D1)),

    R2 = accumulate(R1, {name, dict:from_list([{name, <<"apple">>}, {b, 12}, {c, 13}])}),
    #state{acc_dict = D2} = R2,
    ?assertEqual(1, dict:size(D2)),
 %   io:format("D2: ~p~n", [dict:to_list(D2)]),
    ?assertEqual(2, dict:fetch(<<"apple">>, D2)),

    R3 = accumulate(R2, {name, dict:from_list([{name, <<"orange">>}, {b, 22}, {c, 23}])}),
    #state{acc_dict = D3} = R3,
    ?assertEqual(2, dict:size(D3)),
    ?assertEqual(2, dict:fetch(<<"apple">>, D3)),
    ?assertEqual(1, dict:fetch(<<"orange">>, D3)),

    ?assertEqual(2, dict:size(emit(R3))),
%    Dict3 = dict:from_list([{<<"apple">>, {2, [dict:from_list([{b, 2}, {c, 3}]), dict:from_list([{b, 12}, {c, 13}])]}},
%				{<<"orange">>, {1, [dict:from_list([{b, 22}, {c, 23}])]}}]),
    Dict3 = dict:from_list([{<<"apple">>, {2, [[{b, 2}, {c, 3}], [{b, 12}, {c, 13}]]}},
				{<<"orange">>, {1, [[{b, 22}, {c, 23}]]}}]),
    %io:format("have: ~p~nwant: ~p~n", [dict:to_list(emit(R3)), dict:to_list(Dict3)]),
    ?assertEqual(dict:to_list(Dict3), dict:to_list(emit(R3))),
    ?assertEqual(Dict3, emit(R3)),

    R4 = compensate(R3, {name, dict:from_list([{name, <<"apple">>}, {b, 2}, {c, 3}])}),
    #state{acc_dict = D4} = R4,
    ?assertEqual(2, dict:size(D4)),
    ?assertEqual(1, dict:fetch(<<"apple">>, D4)),
    ?assertEqual(1, dict:fetch(<<"orange">>, D4)),

    ?assertEqual(2, dict:size(emit(R4))),
%    io:format("~p~n", [dict:to_list(emit(R4))]),
    ?assertEqual(dict:from_list([{<<"apple">>, {1, [[{b, 12}, {c, 13}]]}}, {<<"orange">>, {1, [[{b, 22}, {c, 23}]]}}]), emit(R4)).

-endif.
