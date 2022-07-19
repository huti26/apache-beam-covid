package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;


public class CumSumDeathsByAgeGroup {

    public static PDone calculate(PCollection<String> input) {

        // 2020/12/06
        // 0123456789
        return input
                .apply("Extract fields: KV(13:neuerTodesfall, 8:meldedatum + , + 4:altersgruppe + , + 7:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[13]), fields[8] + "," + fields[4] + "," + fields[7]);
                                }))
                .apply("Remove non new cases", Filter.by(element -> element.getKey() >= 0))
                .apply("Convert key value pairs to strings",
                        MapElements.into(TypeDescriptors.strings()).via(element -> element.getKey() + "," + element.getValue()))
                .apply("KV(1:meldedatum + , + 3:altersgruppe, 2:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(fields[1] + "," + fields[2], Integer.parseInt(fields[3]));
                                }))
                .apply("Sum the amount of cases",
                        Sum.integersPerKey())
                .apply("KV(meldedatum + , + altersgruppe, sum(anzahlTodesfall)) -> String(meldedatum,altersgruppe,sum(anzahlTodesfall))",
                        MapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> element.getKey() + "," + element.getValue()))
                .apply("Transform to: KV(1:Altersgruppe + , + MeldeJahr MeldeMonat, KV(MeldeTag, 2:AnzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(
                                            fields[1] + "," + fields[0].substring(0, 7),
                                            KV.of(
                                                    Integer.parseInt(fields[0].substring(8, 10)),
                                                    Integer.parseInt(fields[2])
                                            )
                                    );
                                }))
                .apply("Group days per agegroup,year,month; value is an iterable of KV(day, deaths)",
                        GroupByKey.create())
                .apply("KV.of(0:ageGroup + , + year, KV.of(month, dayIterable))",
                        MapElements
                                .into(
                                        TypeDescriptors.kvs(
                                                TypeDescriptors.strings(),
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.integers(),
                                                        TypeDescriptors.iterables(
                                                                TypeDescriptors.kvs(
                                                                        TypeDescriptors.integers(),
                                                                        TypeDescriptors.integers()
                                                                )
                                                        )
                                                )
                                        )
                                )
                                .via(element -> {
                                            var ageGroupYearMonthSeperated = element.getKey().split(",");
                                            return KV.of(
                                                    ageGroupYearMonthSeperated[0] + "," + ageGroupYearMonthSeperated[1].substring(0, 4),
                                                    KV.of(
                                                            Integer.parseInt(ageGroupYearMonthSeperated[1].substring(5, 7)),
                                                            element.getValue()
                                                    )
                                            );
                                        }

                                )
                )
                .apply("Group days per agegroup,year -> get iterable for months",
                        GroupByKey.create())
                .apply("Calculate cumsum",
                        FlatMapElements
                                .into(TypeDescriptors.strings())
                                .via(element -> {
                                    var agegroupYear = element.getKey();
                                    var monthsIterable = element.getValue();

                                    // arrays start at 0, months start at 1
                                    // get month sizes, check how many elements are in the dayIterable
                                    var monthSizes = new int[13];
                                    for (var month : monthsIterable) {
                                        var monthNumber = month.getKey();
                                        var daysIterable = month.getValue();
                                        monthSizes[monthNumber] = calculateMonthSize(daysIterable);
                                    }


                                    // calculate cumsum for each month, starting with month 1, ending with month 12
                                    var dailyDeathsPerMonth = new int[13][];
                                    var lastMonthCumsum = 0;

                                    for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                        for (var month : monthsIterable) {
                                            var monthNumber = month.getKey();

                                            if (monthNumber == currentMonth) {
                                                var daysIterable = month.getValue();
                                                var currentMonthSize = monthSizes[monthNumber];

                                                var deaths = calculateMonthCumSum(
                                                        daysIterable,
                                                        lastMonthCumsum,
                                                        currentMonthSize
                                                );

                                                // cumsum per month is the value of the last date of a month
                                                lastMonthCumsum = deaths[currentMonthSize];

                                                dailyDeathsPerMonth[monthNumber] = deaths;

                                            }
                                        }
                                    }

                                    // create output by iterating through dailyDeathsPerMonth
                                    var output = new ArrayList<String>();

                                    for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                        var dailyDeaths = dailyDeathsPerMonth[currentMonth];

                                        if (dailyDeaths != null) {
                                            for (int day = 1; day < dailyDeaths.length; day++) {
                                                var dayFormatted = day < 10 ? "0" + day : String.valueOf(day);
                                                output.add(agegroupYear + "/" + currentMonth + "/" + dayFormatted + "," + dailyDeaths[day]);
                                            }
                                        }


                                    }

                                    return output;
                                })
                )

                .apply("Write to file",
                        TextIO.write().to("pipeline_results/deaths_by_age_group_cumsum.csv").withoutSharding());


    }

    public static int calculateMonthSize(Iterable<KV<Integer, Integer>> days) {
        var monthSize = 0;
        for (var day : days) {
            var dayNumber = day.getKey();
            if (dayNumber > monthSize) {
                monthSize = dayNumber;
            }
        }
        return monthSize;
    }

    public static int[] calculateMonthCumSum(Iterable<KV<Integer, Integer>> daysIterable, int lastMonthCumSum, int monthSize) {

        // write iterable values to array
        // days start at 1, arrays start at 0, we keep deaths[1] = deathsOnDay1 & later skip the 0 field
        var deaths = new int[monthSize + 1];
        for (var dayDeathsPair : daysIterable) {
            var day = dayDeathsPair.getKey();
            var deathsOnDay = dayDeathsPair.getValue();
            deaths[day] = deathsOnDay;
        }

        // add last months cumsum to day 1 of new month
        deaths[1] += lastMonthCumSum;

        // cumsum & add to final list & start at 1 to ignore empty 0 field
        for (int day = 1; day < deaths.length; day++) {
            deaths[day] = deaths[day] + deaths[day - 1];
        }

        return deaths;
    }


}
