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


    private Integer flushNodes_query(Transaction tx) {
        Result result = tx.run(
            "MATCH (n) WITH n LIMIT 10000 DELETE n RETURN count(*)"
        );
        return result.single().get(0).asInt();
    }

    /**
     * Delete all nodes in the Neo4j database.
     * It's performed at the beginning of an update operation on the database
     */
    public void flushNodes() {
        int currentResult = 1;
        while(currentResult > 0) {
            try (Session session = driver.session()) {
                currentResult = session.writeTransaction(new TransactionWork<Integer>() {
                    @Override
                    public Integer execute(Transaction tx) {
                        return flushNodes_query(tx);
                    }
                });
            }
            System.out.println(currentResult+" nodes deleted!"); //DEBUG
        }
    }

    private Integer flushRelationships_query(Transaction tx) {
        Result result = tx.run(
            "MATCH ()-[r]->() WITH r LIMIT 10000 "+
            "DELETE r "+
            "RETURN count(*) "
        );
        return result.single().get(0).asInt();
    }

    /**
     * Delete all relationships in the Neo4j database.
     * It's performed at the beginning of an update operation on the database
     */
    public void flushRelationships() {
        int currentResult = 1;
        while(currentResult > 0) {
            try (Session session = driver.session()) {
                currentResult = session.writeTransaction(new TransactionWork<Integer>() {
                    @Override
                    public Integer execute(Transaction tx) {
                        return flushRelationships_query(tx);
                    }
                });
            }
            System.out.println(currentResult+" relationships deleted!"); //DEBUG
        }
    }


    private Void updateAirports_query(Transaction tx, ArrayList<Airport> airports) {
        String baseQuery = "MERGE (airport:Airport {IATA_code:$IATA}) "+
        "SET airport.name = $name, "+
            "airport.cancellationProb = $cancProb, "+
            "airport.fifteenDelayProb = $delayProb, "+
            "airport.qosIndicator = $qosIndicator, "+
            "airport.mostLikelyCauseDelay = $causeDelay, "+
            "airport.mostLikelyCauseCanc = $causeCanc, "+
            "airport.city = $city, "+
            "airport.state = $state "+
        "WITH airport ";
        String currQuery = baseQuery;
        for (Airport currAirport : airports) {
            ArrayList<RankingItem<Airline>> airlineRanking = currAirport.stats.mostServedAirlines;
            if (airlineRanking == null) {
                System.out.println("Error! Airline ranking (by airport) hasn't been initialized!");
                return null;
            }
            ArrayList<RankingItem<Route>> routeRanking = currAirport.stats.mostServedRoutes;
            if (routeRanking == null) {
                System.out.println("Error! Route ranking (by airport) hasn't been initialized!");
                return null;
            }
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("IATA", currAirport.IATA_code);
            parameters.put("name", currAirport.name);
            parameters.put("cancProb", currAirport.stats.cancellationProb);
            parameters.put("delayProb", currAirport.stats.fifteenDelayProb);
            parameters.put("qosIndicator", currAirport.stats.qosIndicator);
            parameters.put("causeDelay", currAirport.stats.mostLikelyCauseDelay);
            parameters.put("causeCanc", currAirport.stats.mostLikelyCauseCanc);
            parameters.put("state", currAirport.state);
            parameters.put("city", currAirport.city);


            for (int i = 0; i < airlineRanking.size(); ++i) {
                parameters.put("air_id_" + i, airlineRanking.get(i).item.identifier);
                parameters.put("air_percentage_" + i, airlineRanking.get(i).value);
                currQuery += "MERGE (airline_" + i + ":Airline {identifier: $air_id_" + i + "}) " + //if the airline is new it's created
                        "CREATE (airport)-[:SERVED_BY {percentage: $air_percentage_" + i + "}]->(airline_" + i + ") ";
            }
            currQuery += "WITH airport ";
            for (int i = 0; i < routeRanking.size(); ++i) {
                parameters.put("destIATA_" + i, routeRanking.get(i).item.destination.IATA_code);
                parameters.put("route_percentage_" + i, routeRanking.get(i).value);
                currQuery += "MATCH (dest:Airport {IATA_code: $destIATA_" + i + "})" +
                        "MERGE (airport)<-[:ORIGIN]-(route_" + i + ":Route)-[:DESTINATION]->(dest)" + //if the route is new it's created
                        "CREATE (airport)-[:POSSIBLE_DEPARTURE {percentage: $route_percentage_" + i + "}]->(route_" + i + ") ";
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
        final int BATCH_SIZE = 500;
        ArrayList<Airport> currAirports = new ArrayList<>();
        while(iter.hasNext()) {
            for(int i = 0; i<BATCH_SIZE; ++i) {
                if(!iter.hasNext()) {
                    break;
                }
                Airport current = iter.next();
                currAirports.add(current);
            }
            try (Session session = driver.session()) {
                session.writeTransaction(new TransactionWork<Void>() {
                    @Override
                    public Void execute(Transaction tx) {
                        return updateAirports_query(tx, currAirports);
                    }
                });
            }
            System.out.println(currAirports.size() + " Airports has been inserted!"); //DEBUG
            currAirports.clear();
        }
    }

    private Void updateAirlines_query(Transaction tx, ArrayList<Airline> airlines) {
        String baseQuery = "MERGE (airline:Airline {identifier:$identifier}) "+
                "SET airline.name = $name, "+
                "airline.cancellationProb = $cancProb, "+
                "airline.fifteenDelayProb = $delayProb, "+
                "airline.qosIndicator = $qosIndicator "+
                "WITH airline ";
        String currQuery = baseQuery;

        for(Airline currAirline : airlines) {
            ArrayList<RankingItem<Airport>> ranking = currAirline.stats.mostServedAirports;
            System.out.println("inserting airline named " + currAirline.identifier + " with a ranking size of " + ranking.size());
            if (ranking == null) {
                System.out.println("Error! Airport ranking (by airline) hasn't been initialized!");
                return null;
            }
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("identifier", currAirline.identifier);
            parameters.put("name", currAirline.name);
            parameters.put("cancProb", currAirline.stats.cancellationProb);
            parameters.put("delayProb", currAirline.stats.fifteenDelayProb);
            parameters.put("qosIndicator", currAirline.stats.qosIndicator);
            for (int i = 0; i < ranking.size(); ++i) {
                parameters.put("IATA_" + i, ranking.get(i).item.IATA_code);
                parameters.put("percentage_" + i, ranking.get(i).value);
                currQuery += "MERGE (airport_" + i + ":Airport {IATA_code: $IATA_" + i + "}) " + //if the airport is new it's created
                        "CREATE (airline)-[:SERVES {percentage: $percentage_" + i + "}]->(airport_" + i + ") ";
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
        final int BATCH_SIZE = 2;
        ArrayList<Airline> currAirlines = new ArrayList<>();
        while(iter.hasNext()) {
            for(int i = 0; i<BATCH_SIZE; ++i) {
                if(!iter.hasNext()) {
                    break;
                }
                Airline current = iter.next();
                currAirlines.add(current);
            }
            try (Session session = driver.session()) {
                session.writeTransaction(new TransactionWork<Void>() {
                    @Override
                    public Void execute(Transaction tx) {
                        return updateAirlines_query(tx, currAirlines);
                    }
                });
            }
            System.out.println(currAirlines.size() + " Airlines have been inserted!"); //DEBUG
            currAirlines.clear();
        }
    }

    private Void updateRoutes_query(Transaction tx, ArrayList<Route> routes) {
        String baseQuery = "MATCH (origin:Airport {IATA_code: $origin}) "+
        "MATCH (destination:Airport {IATA_code: $destination})"+
        "MERGE (origin)<-[:ORIGIN]-(route:Route)-[:DESTINATION]->(destination)"+
        "SET route.cancellationProb = $cancProb, "+
            "route.fifteenDelayProb = $delayProb, "+
            "route.meanDelay = $meanDelay "+
        "WITH route ";
        String currQuery = baseQuery;

        for (Route currRoute : routes) {
            ArrayList<RankingItem<Airline>> ranking = currRoute.stats.bestAirlines;
            if (ranking == null) {
                System.out.println("Error! Airline ranking (by route) hasn't been initialized!");
                return null;
            }
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("origin", currRoute.origin.IATA_code);
            parameters.put("destination", currRoute.destination.IATA_code);
            parameters.put("cancProb", currRoute.stats.cancellationProb);
            parameters.put("delayProb", currRoute.stats.fifteenDelayProb);
            parameters.put("meanDelay", currRoute.stats.meanDelay);
            for (int i = 0; i < ranking.size(); ++i) {
                parameters.put("identifier_" + i, ranking.get(i).item.identifier);
                parameters.put("qos_" + i, ranking.get(i).value);
                currQuery += "MERGE (airline_" + i + ":Airline {identifier: $identifier_" + i + "}) " + //if the airport is new it's created
                        "CREATE (route)-[:SERVED_BY {qosIndicator: $qos_" + i + "}]->(airline_" + i + ") ";
            }
            //System.out.println("Inserting " + ranking.size() + " ranking relationships for a route"); //DEBUG
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
        final int BATCH_SIZE = 500;
        ArrayList<Route> currRoutes = new ArrayList<>();
        while(iter.hasNext()) {
            for(int i = 0; i<BATCH_SIZE; ++i) {
                if(!iter.hasNext()) {
                    break;
                }
                Route current = iter.next();
                currRoutes.add(current);
            }
            try (Session session = driver.session()) {
                session.writeTransaction(new TransactionWork<Void>() {
                    @Override
                    public Void execute(Transaction tx) {
                        return updateRoutes_query(tx, currRoutes);
                    }
                });
            }
            System.out.println(currRoutes.size() + " Routes has been inserted!"); //DEBUG
            currRoutes.clear();
        }
    }


    public void update(UpdatePacket packet) {

        System.out.println("Flushing all the database...");
        flushRelationships();
        System.out.println("All relationships are flushed! Flushing nodes...");
        flushNodes();
        System.out.println("Database correctly flushed!");

        System.out.println("Updating all airlines in the database...");
        updateAirlines(packet.airlineIterator());
        System.out.println("Airlines correctly updated!");

        System.out.println("Updating all Airports in the database...");
        updateAirports(packet.airportIterator());
        System.out.println("Airports correctly updated!");



        System.out.println("Updating all Routes in the database...");
        updateRoutes(packet.routeIterator());
        System.out.println("Routes correctly updated!");


    }


}
