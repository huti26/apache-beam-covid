package hutan.tasks;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;


public class CumSumDeathsByAgeGroup2 {

    public static PDone calculate(PCollection<String> input) {

        // 2020/12/06
        // 0123456789
        return input
                .apply("KV(13:neuerTodesfall, KV(8:meldedatum + , + 4:altersgruppe, 7:anzahlTodesfall))",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers())))
                                .via(line -> {
                                    var fields = line.split(",");
                                    return KV.of(Integer.parseInt(fields[13]), KV.of(fields[8] + "," + fields[4], Integer.parseInt(fields[7])));
                                }))
                .apply("Remove non new cases",
                        Filter.by(element -> element.getKey() >= 0))
                .apply("Remove neuerTodesfall: KV(13:neuerTodesfall, KV(8:meldedatum + , + 4:altersgruppe, 7:anzahlTodesfall))" +
                                " -> KV(8:meldedatum + , + 4:altersgruppe, 7:anzahlTodesfall)",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                                .via(element -> KV.of(element.getValue().getKey(), element.getValue().getValue())))
                .apply("Sum the amount of cases per agegroup and date",
                        Sum.integersPerKey())
                .apply("Convert key value pairs to strings",
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
                .apply("Group days per agegroup,year -> KV(ageGroup + , + year, monthsIterable(daysIterable))",
                        GroupByKey.create())
                .apply("KV(ageGroup + , + year, monthsIterable(daysIterable)) -> KV.of(ageGroup, KV(year, monthsIterable(daysIterable)))",
                        MapElements
                                .into(
                                        TypeDescriptors.kvs(
                                                TypeDescriptors.strings(), // agegroup
                                                TypeDescriptors.kvs(
                                                        TypeDescriptors.integers(), // years
                                                        TypeDescriptors.iterables(
                                                                TypeDescriptors.kvs(
                                                                        TypeDescriptors.integers(), // months
                                                                        TypeDescriptors.iterables(
                                                                                TypeDescriptors.kvs(
                                                                                        TypeDescriptors.integers(), // day
                                                                                        TypeDescriptors.integers()  // deaths
                                                                                )
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                                .via(element -> {
                                    var ageGroupYearSplit = element.getKey().split(",");
                                    return KV.of(
                                            ageGroupYearSplit[0],
                                            KV.of(
                                                    Integer.parseInt(ageGroupYearSplit[1]),
                                                    element.getValue()
                                            )
                                    );
                                })
                )
                .apply("Group days, months iterables per agegroup -> KV(ageGroup, yearsIterable(monthsIterable(daysIterable)))",
                        GroupByKey.create())
                .apply("Calculate cumsum: KV(ageGroup, yearsIterable(monthsIterable(daysIterable)))",
                        FlatMapElements.into(TypeDescriptors.strings())
                                .via(element -> {
                                    var agegroup = element.getKey();
                                    var yearsIterable = element.getValue();
                                    var output = new ArrayList<String>();

                                    // calculate cumsum for each year
                                    // starting with month 1, ending with month 12
                                    // years 2020 2021 2022
                                    var dailyDeathsPerMonthPerYear = new int[3][13][];

                                    // determine how many days we have values for for each month in each year
                                    var monthSizes = new int[3][13];

                                    for (var yearIterable : yearsIterable) {
                                        var currentYear = yearIterable.getKey();
                                        var monthsIterable = yearIterable.getValue();

                                        // arrays start at 0, months start at 1
                                        // get month sizes
                                        for (var month : monthsIterable) {
                                            var monthNumber = month.getKey();
                                            var daysIterable = month.getValue();
                                            monthSizes[currentYear][monthNumber] = calculateMonthSize(daysIterable);
                                        }

                                        // cumsum on month basis
                                        var lastMonthCumsum = 0;
                                        for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                            for (var month : monthsIterable) {
                                                var monthNumber = month.getKey();

                                                if (monthNumber == currentMonth) {
                                                    var daysIterable = month.getValue();
                                                    var currentMonthSize = monthSizes[currentYear][monthNumber];

                                                    var deaths = calculateMonthCumSum(
                                                            daysIterable,
                                                            lastMonthCumsum,
                                                            currentMonthSize
                                                    );

                                                    // cumsum per month is the value of the last date of a month
                                                    lastMonthCumsum = deaths[currentMonthSize];

                                                    dailyDeathsPerMonthPerYear[currentYear][monthNumber] = deaths;

                                                }
                                            }
                                        }

                                        // create output by iterating through dailyDeathsPerMonth
//                                        var output = new ArrayList<String>();


                                    }

                                    // At this point, each year has a correct cumsum, but it does not consider the years prior
                                    // For each completed year, we have data for the 31.12.YEAR
                                    // We take this value and add it to all values of the next year
                                    // Once we don't have such a value, we have found our last year in our data and don't need to do anything
                                    var last_years_deaths = 0;
                                    for (int year = 2020; year <= 2022; year++) {

                                        // add last_years_deaths to current year
                                        if (last_years_deaths != 0) {
                                            for (int month = 1; month <= 12; month++) {
                                                var currentMonthSize = monthSizes[year][month];
                                                for (int day = 0; day <= currentMonthSize; day++) {
                                                    dailyDeathsPerMonthPerYear[year][month][day] += last_years_deaths;
                                                }

                                            }
                                        }

                                        // if last year was finished, continue
                                        if (dailyDeathsPerMonthPerYear[year][12].length == 32) {
                                            last_years_deaths = dailyDeathsPerMonthPerYear[year][12][31];
                                        } else {
                                            break;
                                        }
                                    }

//                                    for(int currentYear = 2020; currentYear <= 2022; currentYear++){
//                                        for (int currentMonth = 12; currentMonth >= 1; currentMonth--) {
//                                            var dailyDeaths = dailyDeathsPerMonthPerYear[currentYear][currentMonth];
//                                            if(dailyDeaths != null){
//                                                // find last day meassured in year
//                                                for(var dailyDeath : dailyDeaths){
//                                                    if
//                                                }
//
//                                                for(int day=31;day >=1;day--){
//                                                    if( dailyDeaths[day] != null){
//
//                                                    }
//                                                }
//
//                                                var last_day = monthSizes[currentYear][currentMonth];
//                                            }
//
//                                        }
//                                    }


                                    // Create Output
                                    for (var yearIterable : yearsIterable) {
                                        var currentYear = yearIterable.getKey();

                                        for (int currentMonth = 1; currentMonth <= 12; currentMonth++) {
                                            var dailyDeaths = dailyDeathsPerMonthPerYear[currentYear][currentMonth];

                                            if (dailyDeaths != null) {
                                                for (int day = 1; day < dailyDeaths.length; day++) {
                                                    var dayFormatted = day < 10 ? "0" + day : String.valueOf(day);
                                                    output.add(agegroup + "/" + currentYear + "/" + currentMonth + "/" + dayFormatted + "," + dailyDeaths[day]);
                                                }
                                            }
                                        }
                                    }


                                    return output;
                                })
                )

                .apply("Write to file",
                        TextIO.write().to("pipeline_results/deaths_by_age_group_cumsum2.csv").withoutSharding());


    }

    public static int calculateMonthSize(Iterable<KV<Integer, Integer>> days) {
        var monthSize = 0;
        for (var day :
                days) {
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
