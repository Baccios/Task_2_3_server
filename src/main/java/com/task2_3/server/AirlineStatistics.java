package com.task2_3.server;

public class AirlineStatistics extends Statistics {
    double qosIndicator;
    String mostLikelyCauseCanc;
    Object[][] mostServedAirports; //each element is an array [Percentage, Airport]

    public AirlineStatistics(double cancellationProb, double fifteenDelayProb, double qosIndicator, String mostLikelyCauseCanc, Object[][] mostServedAirports) {
        super(cancellationProb, fifteenDelayProb);
        this.qosIndicator = qosIndicator;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.mostServedAirports = mostServedAirports;
    }
}
