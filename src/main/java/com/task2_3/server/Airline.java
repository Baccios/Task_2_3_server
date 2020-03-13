package com.task2_3.server;

import java.util.HashMap;

public class Airline {
    String identifier;
    String name;
    AirlineStatistics stats;

    public Airline(String id, String name, AirlineStatistics stats) {
        this.identifier = id;
        this.name = name;
        this.stats = stats;
    }

}
