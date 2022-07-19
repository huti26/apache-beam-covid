package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class SumCasesBySex {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("KV(12:neuerFall, KV(5:geschlecht, 6:anzahlFall))",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[12]), KV.of(fields[5], Integer.parseInt(fields[6])));
                                }))
                .apply("Remove non new cases",
                        Filter.by(element -> element.getKey() >= 0))
                .apply("Remove neuerFall: KV(12:neuerFall, KV(5:geschlecht, 6:anzahlFall)) -> KV(5:geschlecht, 6:anzahlFall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> {
                                    return KV.of(element.getValue().getKey(), element.getValue().getValue());
                                }))
                .apply("Sum the amount of cases",
                        Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/cases_by_sex.csv").withoutSharding());

    }


}
