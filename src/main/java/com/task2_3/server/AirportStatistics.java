package com.task2_3.server;


public class AirportStatistics extends Statistics {
    double importance;
    String mostLikelyCauseDelay;
    String mostLikelyCauseCanc;
    Object[][] mostServedRoutes; //each element is an array [Percentage, Route]
    Object[][] mostServedAirlines; //each element is an array [Percentage, Airline]

}
