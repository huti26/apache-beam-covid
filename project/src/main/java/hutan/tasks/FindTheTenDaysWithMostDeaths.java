package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.io.Serializable;
import java.util.Comparator;


public class FindTheTenDaysWithMostDeaths {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("Extract fields: KV(13:neuerTodesfall, KV(8:meldedatum, 7:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[13]), KV.of(fields[8], Integer.parseInt(fields[7])));
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Unnest KV -> Remove neuerFall field",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply(Top.of(10, new CompareCount()))
                .apply("Extract key value pairs",
                        FlatMapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(list -> list))
                .apply("Convert key value pairs to strings",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + ";" + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/10_days_with_most_deaths.csv").withoutSharding());

    }

    // Die CompareCount Klasse kann genutzt werden, um den numerischen
    // Wert zweier KV<String, Int> (Schlüssel-Wert Paare) zu vergleichen.
    private static class CompareCount implements Comparator<KV<String, Integer>>, Serializable {

        @Override
        public int compare(KV<String, Integer> left, KV<String, Integer> right) {
            return Integer.compare(left.getValue(), right.getValue());
        }
    }


}
