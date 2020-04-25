package com.task2_3.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AirlineStatistics extends Statistics {
    public double qosIndicator;
    public ArrayList<RankingItem<Airport>> mostServedAirports; //each element is an array [Percentage, Airport]
    //public Map<Double, Airport> mostServedAirports; //each element is an array [Percentage, Airport]

    public AirlineStatistics(double cancellationProb, double fifteenDelayProb, double qosIndicator, ArrayList<RankingItem<Airport>> mostServedAirports) {
        super(cancellationProb, fifteenDelayProb);
        this.qosIndicator = qosIndicator;
        this.mostServedAirports = new ArrayList<>(mostServedAirports);
    }

    public  AirlineStatistics() {
        super();
        qosIndicator = -1;
        mostServedAirports = null;
    }
}
