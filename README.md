# Apache Beam Covid Analysis

Educational repository to practise Data Pipelines with Apache Beam. The current implementation is run locally and takes about 30 seconds to process all Pipelines. If desired, the same code could be deployed to the cloud on for example a Spark cluster.

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
