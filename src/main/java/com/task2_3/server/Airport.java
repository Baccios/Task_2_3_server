package com.task2_3.server;

public class Airport {
    int code;
    String IATA_code;
    String name;
    String city;
    String state;
    AirportStatistics stats;

    public Airport(int code, String IATA_code, String name, String city, String state, AirportStatistics stats) {
        this.code = code;
        this.IATA_code = IATA_code;
        this.name = name;
        this.city = city;
        this.state = state;
        this.stats = stats;
    }
}
