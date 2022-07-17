# Aufgabe 1
- Aggregiert Fälle pro Bundesland 

# Aufgabe 2
- a: Aggregiert Gesamttodesfallmeldung pro Tag
- b: Registrierte Todesfälle pro Altergruppe pro Tag

# Aufagabe 3
- a: Durschschnittliche Zeitspanne zwischen Erkranksbeginn und Meldedatum pro Bundesland
- b: Gesamtanzahl Todesfälle unter 80 pro Bundesland

# Aufgabe 4
- a: Anzahl Fälle nach Geschlecht
- b: Anzahl Genesungen pro Monat pro Altersgruppe

# Aufgabe 2b
Altersgruppe AnzahlTodesfall Meldedatum

K: Meldedatum-Altersgruppe V: AnzahlTodesfall
Sum.integersPerKey()

K: Altersgruppe,MeldeJahr,MeldeMonat V: (K: MeldeTag V: Sum(AnzahlTodesfall))

# Aufgabe 3b
Bundesland AnzahlTodesfall Altersgruppe

Filter Altersgruppe

Altersgruppe verwerfen, Bundesland AnzahlTodesfall bleibt

KV Bundesland AnzahlTodesfall daraus machen

Sum.integersPerKey()

# Aufgabe 4b
AnzahlGenesen Altersgruppe Meldedatum


# NeuerFall NeuerTodesfall NeuGenesen berücksichtigt?
exe1: ja

exe2a: ja

exe2b: ja

exe3a: nicht relevant?

exe3b: ja, aber werte stimmen immer noch nicht mit wdr über ein

exe4a: ja

exe4b: ja

# Quelle zum überprüfen, ob werte korrekt sind
https://www1.wdr.de/nachrichten/themen/coronavirus/corona-daten-nrw-100.html