% Modulo per la gestione dei files CSV
-module(csv_manager).
% Behaviour utilizzato
-behaviour(gen_server).

-export([
    start_link/0,
    stop/0,
    to_csv/2,
    to_csv/3,
    from_csv/1,
    from_csv/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

% Dichiarazione record  
-record(state, {table}).                        % record che rappresenta la tabella
-record(spreadsheet, {table, rows, columns}).   % record che rappresenta un foglio di calcolo
-record(owner, {sheet, pid}).                   % record che rappresenta owner del foglio 
-record(policy, {pid, sheet, rule}).            % record che rappresenta le policy

%%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:call(?MODULE, stop).

to_csv(TableName, FileName) ->
    gen_server:call(?MODULE, {to_csv, TableName, FileName}).

to_csv(TableName, FileName, Timeout) ->
    gen_server:call(?MODULE, {to_csv_timeout, TableName, FileName, Timeout}, infinity).

from_csv(FilePath) ->
    Pid = self(),
    gen_server:call(?MODULE, {from_csv, FilePath, Pid}).

from_csv(FilePath, Timeout) ->
    Pid = self(),
    gen_server:call(?MODULE, {from_csv_timeout, FilePath, Pid, Timeout}, infinity).

%%% gen_server callbacks

init([]) ->
    {ok, #state{table = []}}.

handle_call({to_csv, TableName, FileName}, _From, State) ->
    Reply = to_csv_impl(TableName, FileName),
    {reply, Reply, State};

handle_call({to_csv_timeout, TableName, FileName, Timeout}, _From, State) ->
    Reply = to_csv_timeout(TableName, FileName, Timeout),
    {reply, Reply, State};

handle_call({from_csv, FilePath, Pid}, _From, State) ->
    Reply = from_csv_impl(FilePath, Pid),
    {reply, Reply, State};

handle_call({from_csv_timeout, FilePath, Pid, Timeout}, _From, State) ->
    Reply = from_csv_timeout(FilePath, Pid, Timeout),
    {reply, Reply, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% Definizione funzione to_csv_impl(TableName, FileName)
to_csv_impl(TableName, FileName) ->
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(TableName, TabelleLocali) of
        false -> {error, invalid_name_of_table};
        true ->
            % Si apre il file in scrittura
            {ok, File} = file:open(FileName, [write]),
            % Vengono estratti dati dalla tabella Mnesia
            Records = ets:tab2list(TableName),
            % Si convertono i dati in CSV
            CsvContent = records_to_csv(Records),
            % Viene eseguita la scrittura su file
            file:write(File, CsvContent),
            % Si chiude il file
            file:close(File)
    end
.

% Definizione funzione ausiliare records_to_csv(Records)
records_to_csv(Records) ->
    % I dati vengono convertiti in formato CSV
    lists:map(fun({Sheet, Table, Lines, Column}) ->
            Row = lists:join(",", [atom_to_list(Sheet), integer_to_list(Table), integer_to_list(Lines), io_lib:format("~p", [Column])]),
            Row ++"\n"
        end, Records
    )
.

% Definizione funzione ausiliare remove_extension(FileName)
remove_extension(FileName) ->
    Reverse = string:reverse(FileName),
    SubString = string:sub_string(Reverse, 5, 100),
    Final = string:reverse(SubString),
    Final
.

% Definizione funzione ausiliare get_global_name(Pid)
get_global_name(Pid) ->
    case lists:filter(
           fun(Name) -> global:whereis_name(Name) == Pid end,
           global:registered_names()) of
        [GlobalName] -> {GlobalName};
        [] -> {error, not_found}
    end
.

% Definizione funzione from_csv_impl(FilePath)
from_csv_impl(FilePath, Pid) ->
    SpreadsheetFields = record_info(fields, spreadsheet),
    TableName = remove_extension(FilePath),
    TabelleLocali = mnesia:system_info(tables),
    Process = element(1, (get_global_name(Pid))),
    case lists:member(list_to_atom(TableName),TabelleLocali) of
        true -> {error, table_is_already_in_mnesia};
        false ->
            NodeList = [node()],
            mnesia:create_schema(NodeList),
            mnesia:create_table(list_to_atom(TableName), [
                {attributes, SpreadsheetFields},
                {disc_copies, NodeList},
                {type, bag}
            ]),
            {ok, File} = file:open(FilePath, [read]),
            read_lines(File),
            
            % aggiorno la tabella owner
            F = fun()->
                Data = #owner{sheet=list_to_atom(TableName), 
                              pid = global:whereis_name(Process)},
                mnesia:write(Data)
            end,
            {atomic, ok} = mnesia:transaction(F),

            % aggiorno le policy
            F1 = fun() ->
                Data = #policy{pid= global:whereis_name(Process), 
                               sheet=list_to_atom(TableName), rule=write},
                mnesia:write(Data)
            end,
            {atomic, ok} = mnesia:transaction(F1),

            file:close(File)
    end
.

% Definizione funzione ausiliare read_lines(File)
read_lines(File) ->
    case io:get_line(File, "") of
        eof -> ok;
        Line ->
            process_line(Line),
            read_lines(File)
    end
.

% Definizione funzione ausiliare process_line(Line)
process_line(Line) ->
    % Viene rimosso il carattere newline alla fine delle riga
    TrimmedLine = string:trim(Line),
    NewLine = "[" ++ TrimmedLine ++ "]",
    Parse = fun(S) -> 
        {ok, Ts, _} = erl_scan:string(S),
        {ok, Result} = erl_parse:parse_term(Ts ++ [{dot,1} || element(1, lists:last(Ts)) =/= dot]),
        Result 
    end,
    Prova = Parse(NewLine),
    [Col1|Tail1] = Prova,
    [Col2|Tail2] = Tail1,
    [Col3|Tail3] = Tail2,
    [Col4|_] = Tail3,
    F = fun() ->
        Data = {Col1,Col2,Col3,Col4},
        mnesia:write(Data)     
    end,
    Result = mnesia:transaction(F),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} -> Res
    end
.

% Funzioni con parametro TIMEOUT
% Definizione to_csv_impl(TableName, FileName, Timeout)
to_csv_timeout(TableName, FileName, Timeout) ->
    myflush(),
    MyPid = self(),
    Csv_created = to_csv_impl(TableName, FileName),
    case Csv_created of
        error -> error;
        _ ->  
            % Creazione prcesso timer che elimina il file .csv 
            spawn(fun() ->
                timer:sleep(Timeout),
                Result = file:delete(FileName),
                case Result of 
                    {aborted, Reason} -> {error, Reason};
                    ok -> MyPid!{timeout}
                end
            end)
    end,
    ToReturn = receive
        {timeout} -> 
            io:format("Timeout raggiunto, file .csv eliminato.\n"),
            timeout
    end,
    myflush(),
    ToReturn
.

% Definizione funzione from_csv_impl(FilePath, Pid, Timeout)
from_csv_timeout(FilePath, Pid, Timeout) ->
    myflush(),
    MyPid = self(),
    Csv_loaded = from_csv_impl(FilePath, Pid),
    case Csv_loaded of
        error -> error;
        _ -> 
            spawn(fun() -> 
                % Creo processo timer che elimina la tabella
                timer:sleep(Timeout),
                TableName = remove_extension(FilePath),
                Result = mnesia:delete_table(list_to_atom(TableName)),
                case Result of
                    {aborted, Reason} -> {error, Reason};
                    {atomic, ok} -> MyPid!{timeout}
                end
            end)
        end,
    ToReturn = receive
        {timeout} -> 
            io:format("Timeout raggiunto, tabella eliminata.\n"),
            timeout
    end,
    myflush(),
    ToReturn
.

% Definizione funzione myflush()
myflush() ->
    receive
        % Svuota tutta la coda dei messaggi
        _AnyPattern -> myflush()
    after
        % Se non sono presenti messaggi nella coda
        0 -> ok
    end
.