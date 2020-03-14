package com.task2_3.server;

import java.util.HashMap;
import java.util.Map;

public class AirlineStatistics extends Statistics {
    double qosIndicator;
    String mostLikelyCauseCanc;
    Map<Float, Airport> mostServedAirports; //each element is an array [Percentage, Airport]

    public AirlineStatistics(double cancellationProb, double fifteenDelayProb, double qosIndicator, String mostLikelyCauseCanc, HashMap<Float, Airport> mostServedAirports) {
        super(cancellationProb, fifteenDelayProb);
        this.qosIndicator = qosIndicator;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.mostServedAirports = new HashMap<>(mostServedAirports);
    }
}
