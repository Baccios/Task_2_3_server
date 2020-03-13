package com.task2_3.server;

public class RouteStatistics extends Statistics {
    double importance;
    String mostLikelyCauseDelay;
    String mostLikelyCauseCanc;
    double meanDelay;
    Object[][] bestAirlines; //each element is an array [QoS_indicator, Airline]

    public RouteStatistics(double cancellationProb, double fifteenDelayProb, double importance, String mostLikelyCauseDelay, String mostLikelyCauseCanc, double meanDelay, Object[][] bestAirlines) {
        super(cancellationProb, fifteenDelayProb);
        this.importance = importance;
        this.mostLikelyCauseDelay = mostLikelyCauseDelay;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.meanDelay = meanDelay;
        this.bestAirlines = bestAirlines;
    }
}
