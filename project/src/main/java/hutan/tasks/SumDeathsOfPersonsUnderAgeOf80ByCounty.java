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
                .apply("Extract fields: String(4:altersgruppe + , + 2:bundesland + , + 7:anzahlTodesfall + , + 13:neuerTodesfall)",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(line -> {
                                    var fields = line.split(",");
                                    return fields[4] + "," + fields[2] + "," + fields[7] + "," + fields[13];
                                }))
                .apply("Filter over 80 and unbekannt",
                        Filter.by(line -> !line.startsWith("A80+") && !line.startsWith("unbekannt")))
                .apply("Remove altersgruppe, create KV to filter by neuerTodesfall: KV(3:neuerTodesfall, 1:bundesland + , + 2:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[3]), fields[1] + "," + fields[2]);
                                }))
                .apply("Remove non new cases",
                        Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings: String(neuerTodesfall,bundesland,anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + "," + element.getValue()))
                .apply("Remove neuerTodesfall, create KV to sum: KV(1:bundesland, 2:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(fields[1], Integer.parseInt(fields[2]));
                                }))
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
