package com.epam.Taxi;

import com.epam.Taxi.model.Driver;
import com.epam.Taxi.model.Trip;
import com.epam.Taxi.service.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;


public class Manager {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("Taxi - Spark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> driversLines = sc.textFile("data/taxi/drivers.txt");
        JavaRDD<String> tripsLines = sc.textFile("data/taxi/trips.txt");

        long numberOfDrivers = driversLines.count();
        System.out.println("number of drivers: " + numberOfDrivers);

        long numberOfTrips = tripsLines.count();
        System.out.println("number of trips: " + numberOfTrips);

        JavaRDD<Trip> trips = tripsLines.map(line -> line.split(" "))
                .map(arg -> new Trip(parseLong(arg[0].trim()), arg[1].trim(), parseInt(arg[2].trim())));

        JavaRDD<Driver> drivers = driversLines.map(line -> line.split(","))
                .map(arg -> new Driver(parseLong(arg[0].trim()), arg[1].trim(), arg[2].trim(), arg[3].trim()));

        Utils utils = new Utils();
        long amountOfTripsToBostonLongerThanTenKm = utils.getAmountOfTripsToBostonLongerThanTenKm(trips);
        System.out.println("amount of trips to Boston longer than 10 km: " + amountOfTripsToBostonLongerThanTenKm);

        long sumOfAllKmTripsToBoston = utils.getSumOfAllKmTripsToBoston(trips);
        System.out.println("sum of all km trips to Boston: " + sumOfAllKmTripsToBoston);

        List<String> ThreeDriversWithMaxTotalKm = utils.getThreeDriversWithMaxTotalKm(trips, drivers);
        System.out.println("The Three Drivers with max total km: " + ThreeDriversWithMaxTotalKm);

        //Performance lab
        utils.performanceInfo(trips);

    }
}
