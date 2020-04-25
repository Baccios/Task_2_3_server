package com.task2_3.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class RouteStatistics extends Statistics {
    public double importance;
    public String mostLikelyCauseDelay;
    public String mostLikelyCauseCanc;
    public double meanDelay;
    public ArrayList<RankingItem<Airline>> bestAirlines; //each element is an array [QoS_indicator, Airline]

    //RouteStatistics constructor is never called, since we need two query methods to initialize all the attributes.
    public RouteStatistics(double cancellationProb, double fifteenDelayProb, double importance, String mostLikelyCauseDelay, String mostLikelyCauseCanc, double meanDelay, ArrayList<RankingItem<Airline>> bestAirlines) {
        super(cancellationProb, fifteenDelayProb);
        this.importance = importance;
        this.mostLikelyCauseDelay = mostLikelyCauseDelay;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.meanDelay = meanDelay;
        this.bestAirlines = new ArrayList<>(bestAirlines);
    }

    public RouteStatistics(){
        super();
        this.bestAirlines = null;
    }
}
