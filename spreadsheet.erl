% Modulo per la creazione e modifica dei fogli di calcolo
-module(spreadsheet).

% Libreria per la costruzione di query QLC (Query List Comprehension) 
-include_lib("stdlib/include/qlc.hrl").

-export([
    new/1,
    new/4,
    share/2,
    get/4,
    get/5,
    set/5,
    set/6,
    info/1,
    add_row/2,
    del_row/3
]).

% Dichiarazione record
-record(spreadsheet, {table, rows, columns}).            % record che rappresenta un foglio di calcolo
-record(owner, {sheet, pid}).                           % record che rappresenta owner del foglio 
-record(policy, {pid, sheet, rule}).                    % record che rappresenta le policy
-record(format, {sheet, tab_index, nrow, ncolumns}).    % record che rappresenta il formato dei fogli (BAG)

% Definizione funzione new(TabName, N, M, K)
new(TabName, N, M, K) ->
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    % Controllo dell'esistenza di TabName
    case lists:member(TabName, TabelleLocali) of
        true -> {error, invalid_name};
        false ->
            % Creazione dello schema delle tabelle
            SpreadsheetFields = record_info(fields, spreadsheet),
            
            NodeList = [node()]++nodes(),

            mnesia:create_table(TabName, [
                {attributes, SpreadsheetFields},
                {disc_copies, NodeList},
                {type, bag}
            ]),
            % Si popola il foglio con k tabelle di n righe e m colonne
            populate_spreadsheet(TabName, K, N, M),
            % Si crea una nuova tabella in cui si specifica che il nodo è proprietario del foglio (tabella)
            populate_owner_table(TabName),
            % Si salvano le informazioni in base al formato della tabella
            populate_format_table(TabName, K, N, M)
    end
.

% Definizione funzione new(TabName)
new(TabName) -> spreadsheet:new(TabName, 10, 10, 10).

% Definizione funzione populate_owner_table(Sheet)
populate_owner_table(Sheet)->
    F = fun()->
        Data = #owner{sheet=Sheet, pid=self()},
        mnesia:write(Data)
    end,
    {atomic, ok} = mnesia:transaction(F),
    % aggiorno le policy
    F1 = fun() ->
            Data = #policy{pid=self(), sheet=Sheet, rule=write},
            mnesia:write(Data)
        end,
    {atomic, ok} = mnesia:transaction(F1)
.

% Definizione funzione create_row(TabName, I, J, M)
create_row(TabName, I, J, M) -> {TabName, I, J, create_columns(M)}.

% Definizione funzione create_columns(M)
create_columns(M)-> lists:duplicate(M, undef).

% Definizione funzione populate_spreadsheet(Name, K, N, M)
populate_spreadsheet(Name, K, N, M) when K > 1, N > 1, M > 1 ->
    Fila = fun(I) -> 
        lists:map(
            fun(J) -> 
                create_row(Name, I, J, M) 
            end, 
            lists:seq(1, N)
        )
    end,
    Matrice = lists:flatmap(Fila, lists:seq(1, K)),

    save_in_mnesia(Name, Matrice)
.

% Definizione funzione save_in_mnesia(Sheet, Matrice)
save_in_mnesia(Sheet, Matrice) ->
    F = fun() ->
        lists:foreach(
            fun(Elem)-> 
                mnesia:write(Sheet, Elem, write)
            end,
            Matrice
        ) 
    end,
    Result = mnesia:transaction(F),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} -> Res
    end
.

% Definizione funzione populate_format_table(TabName, K, N, M)
populate_format_table(TabName, K, N, M) ->
    Fun = fun() -> lists:foreach(
            fun(I) -> mnesia:write(#format{
                sheet=TabName, 
                tab_index=I, 
                nrow=N, 
                ncolumns=M}
            ) end,
            lists:seq(1, K)
        )
    end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, ok} -> ok
    end  
.

% Definizione funzione get(SpreadSheet, TableIndex, I, J)
get(SpreadSheet, TableIndex, I, J) ->
    MioPid = self(),
    % Costruzione query per recuperare righe tabella policy
    PolicyQuery = qlc:q(
        [ X#policy.rule ||
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.sheet == SpreadSheet
        ]
    ),
    % Si esegue la query
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] ->
                    % Solo se il file è condiviso, Policy è popolata
                    Condition = (Policy == read) or (Policy == write), 
                    case Condition of
                        false -> {error, not_allowed};
                        true -> get_value(SpreadSheet, TableIndex, I, J)
                    end;
                Msg -> {error, {unknown, Msg}}
            end
    end
.

% Definizione funzione ausiliare get_value(SpreadSheet, TableIndex, I, J)
get_value(SpreadSheet, TableIndex, I, J) ->
    TakeRowQuery = qlc:q(
        % Costruzione query per recuperare le righe della tabella SpreadSheet
        [ X#spreadsheet.columns ||
            X <- mnesia:table(SpreadSheet),
            X#spreadsheet.table == TableIndex,
            X#spreadsheet.rows == I
        ]
    ),
    % Si esegue la query
    Fun1 = fun() -> qlc:e(TakeRowQuery) end,
    Result1 = mnesia:transaction(Fun1),
    case Result1 of
        {aborted, Reason1} -> {error, Reason1};
        {atomic, []} -> {error, not_found};
        {atomic, [RowI]} -> lists:nth(J, RowI);
        Msg -> {error, {unknown, Msg}}
    end
.

% Funzione get con parametro TIMEOUT
% Definizione funzione get(SpreadSheet, TableIndex, I, J, Timeout)
get(SpreadSheet, TableIndex, I, J, Timeout) ->
    MioPid = self(),
    % Costruzione query per recuperare righe tabella policy
    PolicyQuery = qlc:q(
        [ X#policy.rule ||
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.sheet == SpreadSheet
        ]
    ),
    % Si esegue la query
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] ->
                    % Solo se il file è condiviso, Policy è popolata
                    Condition = (Policy == read) or (Policy == write), 
                    case Condition of
                        false -> {error, not_allowed};
                        true -> get_timeout(SpreadSheet, TableIndex, I, J, Timeout)
                    end;
                Msg -> {error, {unknown, Msg}}
            end
    end
.

% Definizione funzione ausiliare get_timeout(SpreadSheet, TableIndex, I, J, Timeout)
get_timeout(SpreadSheet, TableIndex, I, J, Timeout) ->
    myflush(),
    MioPid = self(),
    % Si crea un processo timer
    spawn(fun() ->
        receive after Timeout -> MioPid!{timeout} end
    end),
    % Si crea un processo getter
    spawn(fun() ->
        MioPid!{result, get_value(SpreadSheet, TableIndex, I, J)}
    end),
    ToReturn = receive
        {result, Res} -> Res;
        {timeout} -> timeout
    after 10000 -> {error, no_message_received}
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

% Definizione funzione set(SpreadSheet, TableIndex, I, J, Value)
set(SpreadSheet, TableIndex, I, J, Value) ->
    MioPid = self(),
    % Costruzione query per recuperare righe tabella policy
    PolicyQuery = qlc:q(
        [ X#policy.rule ||
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.sheet == SpreadSheet
        ]
    ),
    % Si esegue la query
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] -> 
                    case Policy == write of
                        false -> {error, not_allowed};
                        true -> set_value(SpreadSheet, TableIndex, I, J, Value)
                    end;
                Msg1 -> {error, {unknown, Msg1}} 
            end
    end
.

% Definizione funzione ausiliare set_value(SpreadSheet, TableIndex, I, J, Value)
set_value(SpreadSheet, TableIndex, I, J, Value) ->
    % Costruzione query per recuperare righe tabella SpreadSheet
    TakeColumnQuery = qlc:q(
        [ X#spreadsheet.columns ||
            X <- mnesia:table(SpreadSheet),
            X#spreadsheet.table == TableIndex,
            X#spreadsheet.rows == I
        ]
    ),
    % Si esegue la query
    Fun1 = fun() -> qlc:e(TakeColumnQuery) end,
    Result1 = mnesia:transaction(Fun1),
    case Result1 of
        {aborted, Reason1} -> {error, Reason1};
        {atomic, Res1} ->
            case Res1 of
                [] -> {error, not_found};
                [RowI] -> 
                    {L1, L2} = lists:split(J, RowI),
                    L1WithoutLast = lists:droplast(L1),
                    FinalRow = L1WithoutLast ++ [Value] ++ L2,
                    F2 = fun() ->
                        Record = {SpreadSheet, TableIndex, I, RowI},  
                        mnesia:delete_object(Record),
                        NewRecord = {SpreadSheet, TableIndex, I, FinalRow},
                        mnesia:write(NewRecord)
                    end,
                    {atomic, ok} = mnesia:transaction(F2), ok;
                Msg -> {error, {unknown, Msg}}
            end
    end
.

% Funzione set con parametro TIMEOUT
% Definizione funzione aset(SpreadSheet, TableIndex, I, J, Value, Timeout)
set(SpreadSheet, TableIndex, I, J, Value, Timeout) ->
    MioPid = self(),
     % Costruzione query per recuperare righe tabella policy
    PolicyQuery = qlc:q(
        [ X#policy.rule ||
            X <- mnesia:table(policy),
            X#policy.pid == MioPid,
            X#policy.sheet == SpreadSheet
        ]
    ),
    % Si esegue la query
    Fun = fun() -> qlc:e(PolicyQuery) end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, Res} ->
            case Res of
                [] -> {error, not_allowed};
                [Policy] -> 
                    case Policy == write of
                        false -> {error, not_allowed};
                        true -> set_timeout(SpreadSheet, TableIndex, I, J, Value, Timeout)
                    end;
                Msg1 -> {error, {unknown, Msg1}} 
            end
    end
.

% Definizione funzione ausiliare set_timeout(SpreadSheet, TableIndex, I, J, Value, Timeout)
set_timeout(SpreadSheet, TableIndex, I, J, Value, Timeout) ->
    myflush(),
    ValueToRestore = get_value(SpreadSheet, TableIndex, I, J),
    ToReturn = case ValueToRestore of
        {error, Reason1} -> {error, Reason1};
        _ -> 
            MioPid = self(),
            % Si crea un processo timer
            spawn(fun() ->
                receive after Timeout -> MioPid!{timeout} end
            end),
            % Si crea un processo setter
            spawn(fun() ->
                MioPid!{result, set_value(SpreadSheet, TableIndex, I, J, Value)}
            end),
            receive
                {result, Res} -> Res;
                {timeout} ->
                    % Si aspetta il setter per evitare race condition
                    receive
                        {result, _} -> ok
                    end,
                    % Si ritorna al valore precedente
                    Result = set_value(SpreadSheet, TableIndex, I, J, ValueToRestore),
                    case Result of
                        {error, Reason} -> {error, Reason};
                        ok -> timeout
                    end
            after 10000 -> {error, no_message_received}
            end
    end,
    myflush(),
    ToReturn
.

% Definizione funzione share(Sheet, AccessPolicies)
share(Sheet, AccessPolicies) ->
    % Controllo che l'invocazione di share venga fatta solo dal proprietario della tabella
    {Pid, Ap} = AccessPolicies,
    Proc = global:whereis_name(Pid),

    Condition = (Ap == read) or (Ap == write),
    case Condition of
        false -> {error, wrong_policy_format};
        true -> 
            F = fun() -> mnesia:read({owner, Sheet}) end,
            Result = mnesia:transaction(F),
            case Result of
                {aborted, Reason} -> {error, Reason};
                {atomic, Res} ->
                    case Res of
                        [] ->  {error, sheet_not_found};
                        [{owner, Sheet, Value}] -> 
                            % Si controlla che la richiesta di condivisione sia fatta da\l proprietario del foglio
                            case Value == self() of
                                false -> {error, not_the_owner};
                                true -> 
                                    Query = qlc:q([X || 
                                        X <- mnesia:table(policy),
                                        X#policy.pid =:= Proc,
                                        X#policy.sheet =:= Sheet 
                                    ]),
                                    F2 = fun() -> qlc:e(Query) end,
                                    % Si controlla l'esistenza del pid e del foglio nella tabella
                                    Result1 = mnesia:transaction(F2),
                                    case Result1 of
                                        {aborted, Reason1} -> {error, Reason1};
                                        {atomic, Res1} ->
                                            case Res1 of
                                                % Tabella "vuota" quindi si salvano i dati
                                                [] -> 
                                                    F3 = fun()->
                                                            Data = #policy{pid=Proc, sheet=Sheet, rule=Ap},
                                                            mnesia:write(Data)
                                                        end,
                                                    Result2 = mnesia:transaction(F3),
                                                    case Result2 of
                                                        {aborted, Reason2} -> {error, Reason2};
                                                        {atomic, _} -> ok
                                                    end;
                                                [{policy, Pid_found, Sheet_found, Policy_found}] ->
                                                    % Elemento gia' presente quindi si sottoscrivono i dati
                                                    F4 = fun() ->
                                                            mnesia:delete_object({policy, Pid_found, Sheet_found, Policy_found})
                                                        end,
                                                    Result4 = mnesia:transaction(F4),
                                                    case Result4 of
                                                        {aborted, Reason4} -> {error, Reason4};
                                                        {atomic, _} -> 
                                                            % Si compila la tabella policy
                                                            F3 = fun()->
                                                                    Data = #policy{pid=Proc, sheet=Sheet, rule=Ap},
                                                                    mnesia:write(Data)
                                                                end,
                                                            Result3 = mnesia:transaction(F3),
                                                            case Result3 of
                                                                {aborted, Reason3} -> {error, Reason3};
                                                                {atomic, _} -> ok
                                                            end
                                                    end;
                                                Msg -> {error, {unknown, Msg}}
                                            end 
                                    end
                            end;
                        Msg2 -> {error, {unknown, Msg2}} 
                    end             
            end
    end
.

% Definizione funzione info(Sheet)
info(Sheet) ->
    % Si controlla l'esistenza del foglio di calcolo
    mnesia:start(),
    TabelleLocali = mnesia:system_info(tables),
    case lists:member(Sheet, TabelleLocali) of
        false -> {error, not_exist};
        true ->
            % Si recuperano i PID con permessi di scrittura
            QueryScrittura = qlc:q([X#policy.pid || 
                X <- mnesia:table(policy),
                X#policy.sheet == Sheet,
                X#policy.rule == write]),
            FScrittura = fun() -> qlc:e(QueryScrittura) end,
            ResultScrittura = mnesia:transaction(FScrittura),
            case ResultScrittura of
                {aborted, ReasonS} -> {error, ReasonS};
                {atomic, ListaPidScrittura} ->
                    % Si recuperano i PID con permessi di lettura
                    QueryLettura = qlc:q([X#policy.pid || 
                        X <- mnesia:table(policy),
                        X#policy.sheet == Sheet,
                        X#policy.rule == read]),
                    FLettura = fun() -> qlc:e(QueryLettura) end,
                    ResultLettura = mnesia:transaction(FLettura),
                    case ResultLettura of
                        {aborted, ReasonL} -> {error, ReasonL};
                        {atomic, ListaPidLettura} ->
                            ListaPermessi = {policy_list, 
                                {read, ListaPidLettura}, 
                                {write, ListaPidScrittura}
                            },
                            % Viene calcolato il numero di celle per tabella
                            ResultCelle = cells_for_tab(Sheet),
                            case ResultCelle of
                                {error, ReasonCelle} -> {error, ReasonCelle};
                                {result, ResCelle} ->
                                    CellePerTabella = {cells_for_tab, ResCelle},
                                    Info = [ListaPermessi, CellePerTabella],
                                    Info
                            end 
                    end
            end
    end
.

%  Definizione della funzione cells_for_tab(Sheet)
cells_for_tab(Sheet) ->
    Query = qlc:q([
        {X#format.tab_index, 
            (X#format.nrow * X#format.ncolumns)} || 
                X <- mnesia:table(format),
                X#format.sheet == Sheet]),
    F = fun() -> qlc:e(Query) end,
    Result = mnesia:transaction(F),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, CellePerTab} -> {result, CellePerTab}
    end
.

% SPECIFICA AVANZATA

% Definizione funzione add_row(SpreadSheet, TabIndex)
add_row(SpreadSheet, TabIndex) ->
    mnesia:start(),
    % Recupero del numero di righe esistenti nella tabella
    NRows = get_number_of_rows(SpreadSheet, TabIndex),
    % Crea della nuova riga
    NewRowIndex = NRows + 1,
    NewRow = create_row(SpreadSheet, TabIndex, NewRowIndex, get_number_of_columns(SpreadSheet, TabIndex)),

    save_in_mnesia(SpreadSheet, [NewRow]).

% Funzione ausiliare per il calcolo del numero di righe
get_number_of_rows(SpreadSheet, TabIndex) ->
    TakeRowQuery = qlc:q(
        [ X#spreadsheet.rows ||
            X <- mnesia:table(SpreadSheet),
            X#spreadsheet.table == TabIndex
        ]
    ),
    Fun1 = fun() -> qlc:e(TakeRowQuery) end,
    Result1 = mnesia:transaction(Fun1),
    case Result1 of
        {aborted, Reason1} -> {error, Reason1};
        {atomic, Rows} -> lists:max(Rows)
    end.

% % Funzione ausiliare per il calcolo del numero delle colonne
get_number_of_columns(SpreadSheet, TabIndex) ->
    Query = qlc:q(
        [ X#format.ncolumns ||
            X <- mnesia:table(format),
            X#format.sheet == SpreadSheet,
            X#format.tab_index == TabIndex
        ]
    ),
    Fun = fun() -> qlc:e(Query) end,
    Result = mnesia:transaction(Fun),
    case Result of
        {aborted, Reason} -> {error, Reason};
        {atomic, [Columns]} -> Columns
    end.

% Definizione funzione del_row(SpreadSheet, TabIndex, RowIndex)
del_row(SpreadSheet, TabIndex, RowIndex) ->
    mnesia:start(),
    % Costruzione della query per il recupero della riga da eliminare
    RowQuery = qlc:q(
        [ X#spreadsheet.columns ||
            X <- mnesia:table(SpreadSheet),
            X#spreadsheet.table == TabIndex,
            X#spreadsheet.rows == RowIndex
        ]
    ),
    % Si esegue la query
    Fun1 = fun() -> qlc:e(RowQuery) end,
    Result1 = mnesia:transaction(Fun1),
    case Result1 of
        {aborted, Reason1} -> {error, Reason1};
        {atomic, [Row]} ->
            % Si elimina la riga specificata
            F2 = fun() -> 
                mnesia:delete_object({SpreadSheet, TabIndex, RowIndex, Row})
            end,
            Result2 = mnesia:transaction(F2),
            case Result2 of
                {aborted, Reason2} -> {error, Reason2};
                {atomic, ok} -> ok
            end;
        {atomic, []} -> {error, row_not_found}
    end.