# Apache Beam Covid Analysation

Run as follows

```
./gradlew run --args="--input="ABSOLUTE-PATH-TO-DATA-CSV""
```

The data used is from the Robert-Koch-Institut, which reports COVID data for Germany daily.

Apache Beam does not support skipping the CSV Header row as for right now, there is an open [ticket](https://github.com/apache/beam/issues/17990) on GitHub.

To overcome this issue I simply delete the CSV Header manually and save it separately:

```
FID,IdBundesland,Bundesland,Landkreis,Altersgruppe,Geschlecht,AnzahlFall,AnzahlTodesfall,Meldedatum,IdLandkreis,Datenstand,NeuerFall,NeuerTodesfall,Refdatum,NeuGenesen,AnzahlGenesen,IstErkrankungsbeginn,Altersgruppe2
```
