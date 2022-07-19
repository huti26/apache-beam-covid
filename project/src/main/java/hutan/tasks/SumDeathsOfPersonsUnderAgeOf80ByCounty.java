package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class SumDeathsOfPersonsUnderAgeOf80ByCounty {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("KV(4:altersgruppe, KV(13:neuerTodesfall, KV(2:bundesland, 7:anzahlTodesfall)))",
                        MapElements
                                .into(TypeDescriptors.kvs(
                                                TypeDescriptors.strings(),
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.integers(),
                                                        TypeDescriptors.kvs(
                                                                TypeDescriptors.strings(),
                                                                TypeDescriptors.integers()
                                                        )
                                                )
                                        )
                                )
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(
                                            fields[4],
                                            KV.of(
                                                    Integer.parseInt(fields[13]),
                                                    KV.of(
                                                            fields[2],
                                                            Integer.parseInt(fields[7])
                                                    )
                                            )
                                    );
                                }))
                .apply("Filter over 80 and unbekannt",
                        Filter.by(element -> !element.getKey().startsWith("A80+") && !element.getKey().startsWith("unbekannt")))
                .apply("Remove altersgruppe: KV(altersgruppe, KV(neuerTodesfall, KV(bundesland, anzahlTodesfall))) -> " +
                                "KV(neuerTodesfall, KV(bundesland, anzahlTodesfall))",
                        MapElements
                                .into(TypeDescriptors.kvs(
                                                TypeDescriptors.integers(),
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.strings(),
                                                        TypeDescriptors.integers()
                                                )
                                        )
                                )
                                .via(element -> KV.of(
                                        element.getValue().getKey(),
                                        KV.of(
                                                element.getValue().getValue().getKey(),
                                                element.getValue().getValue().getValue()
                                        )
                                )))
                .apply("Remove non new cases",
                        Filter.by(element -> element.getKey() >= 0))
                .apply("Remove neuerTodessfall: KV(neuerTodesfall, KV(bundesland, anzahlTodesfall)) -> KV(bundesland, anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply("Sum the amount of cases",
                        Sum.integersPerKey())
                .apply("Convert key value pairs to strings: String(bundesland,sum(anzahlTodesfall))",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/sum_deaths_under_80_by_county.csv").withoutSharding());

    }


}
