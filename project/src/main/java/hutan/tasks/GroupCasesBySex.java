package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;


public class GroupCasesBySex {

    public static PDone calculate(PCollection<String> input) {

        return input
                .apply("Extract fields: KV(12:neuerFall, 5:geschlecht + , + 6:anzahlFall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[12]), fields[5] + "," + fields[6]);
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Remove neuerFall:  KV(1:geschlecht, 2:anzahlFall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(fields[1], Integer.parseInt(fields[2]));
                                }))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/cases_by_sex.csv").withoutSharding());

    }


}
