# Telegram Chat Bot
<a href="https://imgur.com/sZNQS9M"><img src="https://i.imgur.com/sZNQS9M.png" title="source: imgur.com" /></a>
## Cos'è?
Telegram Chat Bot (TCB) è un progetto di Matteo Di Mauro, matricola X81000474, dell'università di Catania

Consiste nell'acquisizione di messaggi di gruppi telegram per poterne valutare i sentimenti.


## Obiettivo
Visualizzare i dati ottenuti in un grafico, da poter visionare e fare le dovute valutazioni.


## Struttura


### Guida d'uso
#### Requisiti
Per la corretta esecuzione di Telegram Chat Bot è necessario:

  * Docker;
  * Scaricare kafka_2.12-2.4.1.tgz nella cartella kafka/src/setup del progetto;
  * Scaricare spark-2.4.5-bin-hadoop2.7.tgz nella cartella spark/src/setup del progetto;
  * Eseguire il file script 'build' posizionato nella cartella principale del progetto.
    ```
    $ ./build
    ```

Usa lo script 'start_gnome', posizionato nella cartella principale del progetto.
```
$ ./start_gnome
```
Stop esecuzione
Per fermare tutto, usa lo script 'stop'.

```
$ ./stop
```
