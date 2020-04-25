package com.task2_3.server;

import java.util.HashMap;
import java.util.Iterator;

public class UpdatePacket {
    private HashMap<String, Airline> airlines;
    private HashMap<String, Airport> airports;
    private HashMap<String, Route> routes;

    public Iterator<Airline> airlineIterator() {
        return this.airlines.values().iterator();
    }

    public Iterator<Airport> airportIterator() {
        return this.airports.values().iterator();
    }

    public Iterator<Route> routeIterator() {
        return this.routes.values().iterator();
    }


    public UpdatePacket(HashMap<String, Airline> airlines, HashMap<String, Airport> airports, HashMap<String, Route> routes) {
        this.airlines = airlines;
        this.airports = airports;
        this.routes = routes;
    }
}
