# Distributed-Spreadsheet

## Specifica del problema
Il progetto prevede lo sviluppo di un'applicazione distribuita utilizzando il linguaggio Erlang per la gestione dei fogli di calcolo. I fogli di calcolo possono includere più schede (Tab), ognuna delle quali può essere accessibile e condivisibile con chiunque possieda i permessi necessari.	

## Avvio del Progetto
1. Per ogni nodo della rete che farà parte del sistema distribuito, eseguire il comando:
```shell
erl -sname "nome_nodo" -setcoockie "nome_cookie"
```

2. Compilare i moduli (per ogni nodo):
```erl
c(spreadsheet).
c(distribution).
c(csv_manager).
c(measurements).
```

3. Per far sì che i nodi comunichino tra loro, è necessario pingare ogni partecipante alla rete distribuita. È sufficiente che un nodo contatti tutti gli altri attraverso il comando:
```erl
net_adm:ping(nome_del_nodo).
```

4. Per un corretto funzionamento del programma in un contesto distribuito, è necessario salvare globalmente il nome di ogni nodo. È possibile fare ciò con:
```erl
global:register_name(nodo_del_nodo, self()).
```

5. Per eseguire effettivamente il programma in maniera distribuita, si utilizzano i seguenti comandi
   - Per creare tabella Mnesia su tutti i nodi della rete
    ```erl
    distribution:create_table().
    ```
    - Per avviare DBMS Mnesia su tutti i nodi della rete
    ```erl
    distribution:start().
    ```

6. A questo punto è possibile utilizzare i metodi implementati nel progetto per "lavorare" con i fogli di calcolo

7. Per terminare l'esecuzione del programma nel contesto distribuito, è necessario utilizzare il comando (su un qualsiasi nodo facente parte della rete)
```erl
distribution:stop().
```