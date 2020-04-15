package com.task2_3.server;

import java.util.HashMap;
import java.util.Map;

public class AirlineStatistics extends Statistics {
    public double qosIndicator;
    public String mostLikelyCauseCanc;
    public Map<Double, Airport> mostServedAirports; //each element is an array [Percentage, Airport]

    public AirlineStatistics(double cancellationProb, double fifteenDelayProb, double qosIndicator, String mostLikelyCauseCanc, HashMap<Double, Airport> mostServedAirports) {
        super(cancellationProb, fifteenDelayProb);
        this.qosIndicator = qosIndicator;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.mostServedAirports = new HashMap<>(mostServedAirports);
    }

    public  AirlineStatistics() {
        super();
        qosIndicator = -1;
        mostLikelyCauseCanc = null;
        mostServedAirports = null;
    }
}
