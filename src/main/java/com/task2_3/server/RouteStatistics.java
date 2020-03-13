package com.task2_3.server;

public class RouteStatistics extends Statistics {
    double importance;
    String mostLikelyCauseDelay;
    String mostLikelyCauseCanc;
    double meanDelay;
    Object[][] bestAirlines; //each element is an array [QoS_indicator, Airline]
}
