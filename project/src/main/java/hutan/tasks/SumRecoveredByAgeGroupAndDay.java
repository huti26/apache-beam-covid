package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class SumRecoveredByAgeGroupAndDay {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("KV(15:neuGenesen, KV(4:altersgruppe + , + 8:meldedatum, 16:anzahlGenesen)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[15]), KV.of(fields[4] + "," + fields[8], Integer.parseInt(fields[16])));
                                }))
                .apply("Remove non new cases",
                        Filter.by(element -> element.getKey() >= 0))
                .apply("KV(neuGenesen, KV(altersgruppe + , + meldedatum, anzahlGenesen) -> KV(altersgruppe + , + meldedatum, anzahlGenesen)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply("Sum anzahlGenesen",
                        Sum.integersPerKey())
                .apply("KV(altersgruppe + , + meldedatum, sum(anzahlGenesen)) -> String(altersgruppe,meldedatum,sum(anzahlGenesen))",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/sum_recovered_by_age_group_and_day.csv").withoutSharding());

    }


}
