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

    /**
     * Generate the route map identifier
     * @return the identifier
     */
    public String toCode() {
        return this.origin.IATA_code+this.destination.IATA_code;
    }
}
