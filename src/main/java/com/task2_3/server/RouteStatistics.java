package com.task2_3.server;

import java.util.HashMap;
import java.util.Map;

public class RouteStatistics extends Statistics {
    double importance;
    String mostLikelyCauseDelay;
    String mostLikelyCauseCanc;
    double meanDelay;
    Map<Float, Airline> bestAirlines; //each element is an array [QoS_indicator, Airline]

    public RouteStatistics(double cancellationProb, double fifteenDelayProb, double importance, String mostLikelyCauseDelay, String mostLikelyCauseCanc, double meanDelay, HashMap<Float, Airline> bestAirlines) {
        super(cancellationProb, fifteenDelayProb);
        this.importance = importance;
        this.mostLikelyCauseDelay = mostLikelyCauseDelay;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.meanDelay = meanDelay;
        this.bestAirlines = new HashMap<>(bestAirlines);
    }
}
