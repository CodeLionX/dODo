# Seminar Meeting 2019-05-28

## Recap last tasks

| Status | Who?  | Until when?   | What? |
| :----: | :---: | :------------ | :---- |
| :heavy_check_mark: | Sebi | 28.05. | Do not throw exception on EOF if file is empty before inference number is reached |
| :heavy_check_mark: | Sebi | 28.05. | Use column names (parsed from CSV or synthetic) for output |
| :heavy_check_mark: | Sebi | 28.05. | test all available datasets to be compatible to our algorithm and fix issues |
| :heavy_check_mark: | Jul  | 28.05. | write E2E (acceptance) test for our test data set |

## Input from Thorsten

- Kafka Seminar
  - intermidiate presentation kafka, Mittwoch, 12.06. 11:00 Uhr
  - final presentation kafka, Mittwoch, 17.07. 11:00 Uhr
- Naumann hat im Web Table Seminar über Vorträge erzählt, was ihm wichtig ist: [VL-Seite](https://hpi.de/naumann/teaching/teaching/ss-19/processing-web-tables.html)
- Folien von Thorsten (von 29.05) sind auch interessant für unser seminar

## Feedback

- was passiert, wenn 3 benachbarte Nodes failen?
  - sanity check (3 benachbarte)? --> neustart
  - wir unterstützen das nicht
- marker für "candidate doesn't hold" und Kandidaten später generieren
  - bei speicherproblemen
  - weniger Daten zu synchronisieren
- Option für `null`-handling ist ok (wenn man Algorithmus optimieren wollte, dann müsste man das betrachten)

## Presentation

- Wichtig: schöne Geschichte, verständlich
- Nur kurze Zeit: 15min
- Ort: F2.10
- Englisch ist zu empfehlen
- Folien
  - immer auf Englisch
  - HPI template
  - **Namen!**
  - kaum Text
  - Diagramme
- Einführung:
  - Thema, was versuchen wir zu lösen?
  - davon ausgehen, das jmd. dabei ist, der nichts davon weiß
  - Akka?, Actor Programming? jedes Fachwort kurz beschreiben (was ist der Kern?)
- nicht auf "Thank you" enden: Summary slide, die Diskussion anstößt
- Voraussetzungen / Setting vorher klären: Wie wichtig sind uns fault-tolerance / reliability, ...
- eher auf eine Sache konzentrieren und die anderen Dinge als gegeben sehen, Systemkomplettbeschreibung wird erst in der Ausarbeitung gehen
- Zeitliche Aufteilung zwischen den Personen sollte ausgeglichen sein

## Next Tasks

| Who?  | Until when?   | What? |
| :---: | :------------ | :---- |
| Jul |  | Batch work and results for workers inside the actor systems to reduce message load on master actor |
| Sebi |  | Side-channel for data set exchange between actor systems (using Netty) |