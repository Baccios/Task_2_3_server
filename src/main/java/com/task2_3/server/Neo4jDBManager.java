package com.task2_3.server;

import org.neo4j.driver.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import static org.neo4j.driver.Values.parameters;


public class Neo4jDBManager implements AutoCloseable {
    private final Driver driver;

    public Neo4jDBManager(String uri, String user, String password){
        //driver initialization using user and password of the neo4jdatabase
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );

    }

    @Override
    public void close() {
        driver.close();
    }



    public String matchNode (String nodeName) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (n:Node {name: $nodeName}) RETURN name(a)", parameters("nodeName", nodeName));
                return result.single().get(0).asString();
            });
        }
    }

    private Void createNode_query (Transaction tx, String nodeName){
        tx.run("CREATE (n:Node {name:$nodeName})",parameters("nodeName",nodeName));
        return null;
    }

    /**
     * Create a node in the database with the specified node name
     * @param nodeName the label name of the new node
     */
    public void createNode (String nodeName){
        try(Session session = driver.session()){
            session.writeTransaction( new TransactionWork<Void>()
            {
                @Override
                public Void execute(Transaction tx) {
                    return createNode_query(tx, nodeName);
                }
            } );
        }
    }


    private Void flushRelationships_query(Transaction tx) {
        tx.run("MATCH -[r]-> DELETE r");
        tx.commit();
        return null;
    }

    /**
     * Delete all relationships in the Neo4j database.
     * It's performed at the beginning of an update operation on the database
     */
    public void flushRelationships() {
        try(Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Void>()
            {
                @Override
                public Void execute(Transaction tx) {
                    return flushRelationships_query(tx);
                }
            });
        }
    }

    private Void flushRoutes_query(Transaction tx) {
        tx.run("MATCH (r:Route) DELETE r");
        tx.commit();
        return null;
    }

    /**
     * Delete all routes in the Neo4j database.
     * It's performed at the beginning of an update operation on the database
     */
    public void flushRoutes() {
        try(Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Void>()
            {
                @Override
                public Void execute(Transaction tx) {
                    return flushRoutes_query(tx);
                }
            });
        }
    }


    private Void updateAirports_query(Transaction tx, Iterator<Airport> iter) {
        String baseQuery =
                "MERGE (airport:Airport {IATA_Code:$IATA}) "+
                "SET airport.name = $name, "+
                    "airport.cancellationProb = $cancProb, "+
                    "airport.fifteenDelayProb = $delayProb, "+
                    "airport.qosIndicator = $qosIndicator, "+
                    "airport.mostLikelyCauseDelay = $causeDelay, "+
                    "airport.mostLikelyCauseCanc = $causeCanc "+
                "WITH airport ";
        String currQuery = baseQuery;
        while(iter.hasNext()) {
            Airport currAirport = iter.next();
            ArrayList<RankingItem<Airline>> airlineRanking = currAirport.stats.mostServedAirlines;
            ArrayList<RankingItem<Route>> routeRanking = currAirport.stats.mostServedRoutes;
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("IATA", currAirport.IATA_code);
            parameters.put("name", currAirport.name);
            parameters.put("cancProb", currAirport.stats.cancellationProb);
            parameters.put("delayProb", currAirport.stats.fifteenDelayProb);
            parameters.put("qosIndicator", currAirport.stats.qosIndicator);
            parameters.put("causeDelay", currAirport.stats.mostLikelyCauseDelay);
            parameters.put("causeCanc", currAirport.stats.mostLikelyCauseCanc);
            for (int i = 0; i < airlineRanking.size(); ++i) {
                parameters.put("air_id_"+i, airlineRanking.get(i).item.identifier);
                parameters.put("air_percentage_"+i, airlineRanking.get(i).value);
                currQuery += "MERGE (airline:Airline {identifier: air_id_"+i+"}) " + //if the airline is new it's created
                             "CREATE (airport)-[:SERVED_BY {percentage: $air_percentage_"+i+"}]->(airline) ";
            }
            for (int i = 0; i < routeRanking.size(); ++i) {
                parameters.put("destIATA_"+i, routeRanking.get(i).item.destination.IATA_code);
                parameters.put("route_percentage_"+i, routeRanking.get(i).value);
                currQuery += "MATCH (dest:Airport {IATA_code: $destIATA_"+i+"})"+
                        "MERGE (airport)<-[:ORIGIN]-(route:Route)-[:DESTINATION]->(dest)" + //if the route is new it's created
                        "CREATE (airport)-[:POSSIBLE_DEPARTURE {percentage: $route_percentage_"+i+"}]->(route) ";
            }
            tx.run(currQuery, parameters);
            currQuery = baseQuery;
        }
        tx.commit();
        return null;
    }

    /**
     * Update all airports in the Neo4j database rebasing on the airports data in the parameter.
     * @param iter an iterator through all the updated airports
     */
    public void updateAirports(Iterator<Airport> iter) {
        try(Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction tx) {
                    return updateAirports_query(tx, iter);
                }
            });
        }
    }

    private Void updateAirlines_query(Transaction tx, Iterator<Airline> iter) {
        String baseQuery =
                "MERGE (airline:Airline {identifier:$identifier}) "+
                        "SET airline.name = $name, "+
                        "airline.cancellationProb = $cancProb, "+
                        "airline.fifteenDelayProb = $delayProb, "+
                        "airline.qosIndicator = $qosIndicator "+
                        "WITH airline ";
        String currQuery = baseQuery;
        while(iter.hasNext()) {
            Airline currAirline = iter.next();
            ArrayList<RankingItem<Airport>> ranking = currAirline.stats.mostServedAirports;
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("identifier", currAirline.identifier);
            parameters.put("name", currAirline.name);
            parameters.put("cancProb", currAirline.stats.cancellationProb);
            parameters.put("delayProb", currAirline.stats.fifteenDelayProb);
            parameters.put("qosIndicator", currAirline.stats.qosIndicator);
            for (int i = 0; i < ranking.size(); ++i) {
                parameters.put("IATA_"+i, ranking.get(i).item.IATA_code);
                parameters.put("percentage_"+i, ranking.get(i).value);
                currQuery += "MERGE (airport:Airport {IATA_code: $IATA_"+i+"}) " + //if the airport is new it's created
                        "CREATE (airline)-[:SERVES {percentage: $percentage_"+i+"}]->(airport) ";
            }
            tx.run(currQuery, parameters);
            currQuery = baseQuery;
        }
        tx.commit();
        return null;
    }

    /**
     * Update all airlines in the Neo4j database rebasing on the airlines data in the parameter.
     * @param iter an iterator through all the updated airlines
     */
    public void updateAirlines(Iterator<Airline> iter) {
        try(Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction tx) {
                    return updateAirlines_query(tx, iter);
                }
            });
        }
    }

    private Void updateRoutes_query(Transaction tx, Iterator<Route> iter) {
        String baseQuery =
                "MATCH (origin:Airport {IATA_code: $origin}) "+
                "MATCH (destination:Airport {IATA_code: $destination})"+
                "MERGE (origin)<-[:ORIGIN]-(route:Route)-[:DESTINATION]->(destination)"+
                "SET route.cancellationProb = $cancProb, "+
                    "route.fifteenDelayProb = $delayProb, "+
                    "route.meanDelay = $meanDelay "+
                "WITH route ";
        String currQuery = baseQuery;
        while(iter.hasNext()) {
            Route currRoute = iter.next();
            ArrayList<RankingItem<Airline>> ranking = currRoute.stats.bestAirlines;
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("origin", currRoute.origin.IATA_code);
            parameters.put("destination", currRoute.destination.IATA_code);
            parameters.put("cancProb", currRoute.stats.cancellationProb);
            parameters.put("delayProb", currRoute.stats.fifteenDelayProb);
            for (int i = 0; i < ranking.size(); ++i) {
                parameters.put("identifier_"+i, ranking.get(i).item.identifier);
                parameters.put("qos_"+i, ranking.get(i).value);
                currQuery += "MERGE (airline:Airline {identifier: $identifier_"+i+"}) " + //if the airport is new it's created
                        "CREATE (route)-[:SERVED_BY {qosIndicator: $qos_"+i+"}]->(airline) ";
            }
            tx.run(currQuery, parameters);
            currQuery = baseQuery;
        }
        tx.commit();
        return null;
    }

    /**
     * Update all routes in the Neo4j database rebasing on the routes data in the parameter.
     * @param iter an iterator through all the updated routes
     */
    public void updateRoutes(Iterator<Route> iter) {
        try(Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Void>() {
                @Override
                public Void execute(Transaction tx) {
                    return updateRoutes_query(tx, iter);
                }
            });
        }
    }


    public void update(UpdatePacket packet) {
        flushRelationships();
        flushRoutes();
        updateAirlines(packet.airlineIterator());
        updateAirports(packet.airportIterator());
        updateRoutes(packet.routeIterator());
    }


}
