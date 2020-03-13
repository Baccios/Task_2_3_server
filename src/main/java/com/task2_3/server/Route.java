package com.task2_3.server;

public class Route {
    Airport origin;
    Airport destination;
    RouteStatistics stats;

    public Route(Airport origin, Airport destination, RouteStatistics stats) {
        this.origin = origin;
        this.destination = destination;
        this.stats = stats;
    }
}
