% modulo di debug e testing
-module(measurements).

-export([average_lookup_time/2, average_setup_time/2]).


lookup_time(Foglio) ->
    TimeUnit = microsecond, 
    {Time, _Value} = timer:tc(spreadsheet, get, [Foglio, 1, 1, 1], TimeUnit),
    {TimeUnit, Time}
.

% tempo medio di ricerca con N nodi
average_lookup_time(Foglio, N) -> 
    Times = lists:map(fun(_I) ->
            {_TimeUnit, Time} = lookup_time(Foglio),
            Time 
        end, 
        lists:seq(1, N)
    ),
    SumOfTimes = lists:sum(Times),
    Length = lists:foldl(fun(_X, Acc) -> Acc + 1 end, 0, Times),
    {microsecond, SumOfTimes / Length}
.

setup_time(Foglio) ->
    TimeUnit = microsecond, 
    {Time, _Value} = timer:tc(spreadsheet, set, [Foglio, 1, 1, 1, 'SETUP'], TimeUnit),
    {TimeUnit, Time}
.

% tempo di configurazione con N nodi
average_setup_time(Foglio, N) -> 
    Times = lists:map(fun(_I) ->
            {_TimeUnit, Time} = setup_time(Foglio),
            Time 
        end, 
        lists:seq(1, N)
    ),
    SumOfTimes = lists:sum(Times),
    Length = lists:foldl(fun(_X, Acc) -> Acc + 1 end, 0, Times),
    {microsecond, SumOfTimes / Length}
.