package com.epam.taxi.service;

import com.epam.taxi.model.Driver;
import com.epam.taxi.model.Trip;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    private JavaPairRDD<Long, Integer> getPairsTripIdAndKm(JavaRDD<Trip> trips) {
        return trips
                .mapToPair(trip -> new Tuple2<>(trip.driverId(), trip.km()))
                .reduceByKey(Integer::sum);
    }

    private JavaPairRDD<Long, String> getPairsDriverIdAndName(JavaRDD<Driver> drivers) {
        return drivers
                .mapToPair(driver -> new Tuple2<>(driver.id(), driver.name()));
    }

    public long getAmountOfTripsToBostonLongerThanTenKm(JavaRDD<Trip> trips) {
        return trips.filter(trip -> trip.location().equalsIgnoreCase("boston"))
                .filter(trip -> trip.km() > 10)
                .count();
    }

    public long getSumOfAllKmTripsToBoston(JavaRDD<Trip> trips) {
        return trips
                .filter(trip -> trip.location().equalsIgnoreCase("boston"))
                .map(Trip::km)
                .reduce(Integer::sum);
    }


    public List<String> getThreeDriversWithMaxTotalKm(JavaRDD<Trip> trips, JavaRDD<Driver> drivers) {
        return getPairsTripIdAndKm(trips).join(getPairsDriverIdAndName(drivers))
                .mapToPair(tuple -> new Tuple2<>(tuple._2()._1(), tuple._2()._2()))
                .sortByKey(false)
                .map(tuple -> tuple._2)
                .take(3);

    }

    public void performanceInfo(JavaRDD<Trip> trips) {
        Long tripsLessThanFiveKm = trips.filter(trip -> trip.km() < 5).count();
        System.out.println("trips less than five km: " + tripsLessThanFiveKm);

        Long tripsBetweenFiveAndTenKmBothIncluded = trips.filter(trip -> trip.km() >= 5)
                .filter(trip -> trip.km() <= 10).count();
        System.out.println("trips between five and ten km both included: " + tripsBetweenFiveAndTenKmBothIncluded);

        Long tripsGreaterThanTenKm = trips.filter(trip -> trip.km() > 10).count();
        System.out.println("trips greater than 10 km: " + tripsGreaterThanTenKm);

        HashMap<String, Long> tripsMap = new HashMap<>();
        tripsMap.put("LessThanFiveKm", tripsLessThanFiveKm);
        tripsMap.put("tripsBetweenFiveAndTenKmBothIncluded", tripsBetweenFiveAndTenKmBothIncluded);
        tripsMap.put("tripsGreaterThanTenKm", tripsGreaterThanTenKm);
        List<Map.Entry<String, Long>> tripListMapped = tripsMap.entrySet().stream()
                .sorted((e1, e2) -> (int) (e2.getValue() - e1.getValue()))
                .collect(Collectors.toList());
        final Long popular = tripListMapped.get(0).getValue();
        Stream<Map.Entry<String, Long>> mostPopular = tripsMap.entrySet().stream()
                .filter(element -> element.getValue() == popular);
        System.out.println("most popular trips: " + mostPopular.collect(Collectors.toList()));
    }
}
