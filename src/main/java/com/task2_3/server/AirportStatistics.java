package com.task2_3.server;


public class AirportStatistics extends Statistics {

    double importance;
    String mostLikelyCauseDelay;
    String mostLikelyCauseCanc;
    Object[][] mostServedRoutes; //each element is an array [Percentage, Route]
    Object[][] mostServedAirlines; //each element is an array [Percentage, Airline]

    public AirportStatistics(double cancellationProb, double fifteenDelayProb, double importance, String mostLikelyCauseDelay, String mostLikelyCauseCanc, Object[][] mostServedRoutes, Object[][] mostServedAirlines) {
        super(cancellationProb, fifteenDelayProb);
        this.importance = importance;
        this.mostLikelyCauseDelay = mostLikelyCauseDelay;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.mostServedRoutes = mostServedRoutes;
        this.mostServedAirlines = mostServedAirlines;
    }

}
