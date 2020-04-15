package com.task2_3.server;

import java.util.HashMap;
import java.util.Map;

public class AirlineStatistics extends Statistics {
    public double qosIndicator;
    public Map<Double, Airport> mostServedAirports; //each element is an array [Percentage, Airport]

    public AirlineStatistics(double cancellationProb, double fifteenDelayProb, double qosIndicator, HashMap<Double, Airport> mostServedAirports) {
        super(cancellationProb, fifteenDelayProb);
        this.qosIndicator = qosIndicator;
        this.mostServedAirports = new HashMap<>(mostServedAirports);
    }

    public  AirlineStatistics() {
        super();
        qosIndicator = -1;
        mostServedAirports = null;
    }
}
