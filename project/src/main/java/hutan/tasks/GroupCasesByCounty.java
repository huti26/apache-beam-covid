package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class GroupCasesByCounty {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("Extract fields 2:bundesland & 6:anzahlFall as KV, 12:neuerFall",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var bundesland = fields[2];
                                    var anzahlFall = Integer.parseInt(fields[6]);
                                    var neuerfall = Integer.parseInt(fields[12]);
                                    return KV.of(neuerfall, KV.of(bundesland, anzahlFall));
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Unnest KV -> Remove neuerFall field",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + ";" + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/cases_by_county.csv").withoutSharding());

    }

}
