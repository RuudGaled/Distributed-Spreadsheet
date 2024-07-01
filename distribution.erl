% Modulo per la gestione della distribuzione nei nodi
-module(distribution).

-export([
    create_table/0,
    start/0,
    stop/0
]).

% Dichiarazione record
-record(owner, {sheet, pid}).                           % record che rappresenta owner del foglio
-record(policy, {pid, sheet, rule}).                    % record che rappresenta le policy
-record(format, {sheet, tab_index, nrow, ncolumns}).    % record che rappresenta il formato dei fogli (BAG)


% Definizione funzione create_table()
create_table() ->
    NodeList = [node()] ++ nodes(),
    % Creazione dello schema delle tabelle e di quest'ultime
    mnesia:create_schema(NodeList),
    % Si esegue l'avvio di Mnesia
    start_remote(),
    OwnerFields = record_info(fields, owner),
    PolicyFields = record_info(fields, policy),
    FormatFields = record_info(fields, format),
    mnesia:create_table(owner, [
        {attributes, OwnerFields},
        {disc_copies, NodeList}
    ]),
    mnesia:create_table(policy, [
        {attributes, PolicyFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    mnesia:create_table(format, [
        {attributes, FormatFields},
        {disc_copies, NodeList},
        {type, bag}
    ]),
    % Viene eseguito lo stop di Mnesia per tutti i nosi
    distribution:stop()
.

% Definizione funzione start()
start() ->
    % Si esegue l'avvio di Mnesia per ogni nodo
    start_remote(),
    % Vengono caricate la tabelle già esistenti
    mnesia:wait_for_tables([owner, policy, format], 5000)
.

% Definizione funzione start_remote()
start_remote() ->
    % Avvio del modulo Mnesia dentro ogni nodo
    MyPid = self(),
    lists:foreach(
        fun(Node) ->
            spawn(
                Node, 
                fun() ->
                    mnesia:start(),
                    MyPid!{mnesia_started}
                end
            ) 
        end,
        nodes()
    ),
    % Controllo che ogni nodo abbia avviato Mnesia
    lists:foreach(
        fun(_Node) -> 
            receive 
                {mnesia_started} -> ok 
            end
        end,
        nodes()
    ),
    % Viene avviato anche sul nodo principale
    mnesia:start()
.

% Definizione funzione stop()
stop() ->
    % Si fermal'esecuzione del modulo Mnesia dentro ogni nodo
    MyPid = self(),
    lists:foreach(
        fun(Node) ->
            spawn(
                Node, 
                fun() ->
                    mnesia:stop(),
                    MyPid!{mnesia_stopped} 
                end
            ) 
        end,
        nodes()
    ),
    % Controllo che ogni nodo abbia terminato Mnesia
    lists:foreach(
        fun(_Node) -> 
            receive 
                {mnesia_stopped} -> ok 
            end
        end,
        nodes()
    ),
    % Viene terminato anche sul nodo principale
    mnesia:stop()
.