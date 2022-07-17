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
                .apply("Extract fields 5:geschlecht & 6:anzahlFall & 12 neuerFall",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var geschlecht = fields[5];
                                    var anzahlFall = fields[6];
                                    var neuerFall = Integer.parseInt(fields[12]);
                                    return KV.of(neuerFall, geschlecht + "," + anzahlFall);
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Remove neuerFall",
                        MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    var geschlecht = fields[1];
                                    var anzahlFall = Integer.parseInt(fields[2]);
                                    return KV.of(geschlecht, anzahlFall);
                                }))
                .apply("Sum the amount of cases", Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("Write to file",
                        TextIO.write().to("pipeline_results/cases_by_sex.csv").withoutSharding());

    }


}
