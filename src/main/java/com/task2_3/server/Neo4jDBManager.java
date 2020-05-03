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
        tx.run("CREATE (n:Node {name:$nodeName})\n",parameters("nodeName",nodeName));
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
            "MATCH (n) WITH n LIMIT 10000 DELETE n RETURN count(*)\n"
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
            "MATCH ()-[r]->() WITH r LIMIT 10000 \n"+
            "DELETE r \n"+
            "RETURN count(*) \n"
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
        String baseQuery = "MERGE (airport:Airport {IATA_code:$IATA}) \n"+
        "SET airport.name = $name, \n"+
            "airport.cancellationProb = $cancProb, \n"+
            "airport.fifteenDelayProb = $delayProb, \n"+
            "airport.qosIndicator = $qosIndicator, \n"+
            "airport.mostLikelyCauseDelay = $causeDelay, \n"+
            "airport.mostLikelyCauseCanc = $causeCanc, \n"+
            "airport.city = $city, \n"+
            "airport.state = $state \n"+
        "WITH airport \n";
        String currQuery = baseQuery;
        for (Airport currAirport : airports) {
            ArrayList<RankingItem<Airline>> airlineRanking = currAirport.stats.mostServedAirlines;
            if (airlineRanking == null) {
                System.out.println("Error! Airline ranking (by airport) hasn't been initialized!");
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
                currQuery += "MERGE (airline_" + i + ":Airline {identifier: $air_id_" + i + "}) \n" + //if the airline is new it's created
                        "CREATE (airport)-[:SERVED_BY {percentage: $air_percentage_" + i + "}]->(airline_" + i + ") \n";
            }
            /*
            currQuery += "WITH airport ";
            for (int i = 0; i < routeRanking.size(); ++i) {
                parameters.put("destIATA_" + i, routeRanking.get(i).item.destination.IATA_code);
                parameters.put("route_percentage_" + i, routeRanking.get(i).value);
                currQuery +=
                        "MERGE (airport)<-[:ORIGIN]-(route_" + i + ":Route)-[:DESTINATION]->(dest_"+ i +":Airport {IATA_code: $destIATA_" + i + "}) " + //if the route is new it's created
                        "CREATE (airport)-[:POSSIBLE_DEPARTURE {percentage: $route_percentage_" + i + "}]->(route_" + i + ") ";
            }
            */

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
        final int BATCH_SIZE = 20;
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
            System.out.println(currAirports.size() + " Airports have been inserted!"); //DEBUG
            currAirports.clear();
        }
    }

    /**
     * Update all airport departures for airports in the list. If the maximum number of transactions is reached then last array indexes
     * are left in the indexes parameter.
     * @param tx The transaction instance
     * @param airports The list of airports to update
     * @param indexes The two indexes from which starting analyzing the list: indexes[0] indicates the first element to consider, while
     *                indexes[1] indicates the first element to consider in airports.get(indexes[0]) element. If the method managed to update all
     *                elements in the list, then indexes is left in state {-1,-1}, otherwise they are left to the status of the method at the
     *                ending point, so that this same array can be used to call this method again for the next transaction.
     * @return Void
     */
    private Void updateDepartures_query(Transaction tx, ArrayList<Airport> airports, int[] indexes) {
        final int MAX_ATOMIC_OPERATIONS = 80;
        int opCounter = 0;
        boolean cycleInterrupt = false;
        String baseQuery = "MATCH (airport:Airport {IATA_code:$IATA}) \n";
        String currQuery = baseQuery;
        for(int idx = indexes[0]; idx < airports.size(); ++idx) {
            Airport currAirport = airports.get(idx);

            ArrayList<RankingItem<Route>> routeRanking = currAirport.stats.mostServedRoutes;
            if (routeRanking == null) {
                System.out.println("Error! Route ranking (by airport) hasn't been initialized!");
                return null;
            }
            HashMap<String, Object> parameters = new HashMap<>();
            parameters.put("IATA", currAirport.IATA_code);

            if(opCounter == MAX_ATOMIC_OPERATIONS) {  //can't start a new iteration when we already reached the max number of operations
                indexes[0] = idx;
                indexes[1] = 0;
                cycleInterrupt = true;
                break;
            }

            int initialIndex = (idx == indexes[0]) ? indexes[1] : 0;

            for (int i = initialIndex; i < routeRanking.size(); ++i) {
                opCounter++;
                parameters.put("destIATA_" + i, routeRanking.get(i).item.destination.IATA_code);
                parameters.put("route_percentage_" + i, routeRanking.get(i).value);
                currQuery += "WITH airport "+
                        "MATCH (dest_"+ i +":Airport {IATA_code: $destIATA_" + i + "}) \n"+
                        "MATCH (airport)<-[:ORIGIN]-(route_" + i + ":Route)-[:DESTINATION]->(dest_"+ i +") \n" + //if the route is new it's created
                        "CREATE (airport)-[:POSSIBLE_DEPARTURE {percentage: $route_percentage_" + i + "}]->(route_" + i + ") \n";
                if(opCounter == MAX_ATOMIC_OPERATIONS && i != (routeRanking.size() - 1)) {
                    System.out.println("Breaking the array in more transactions..."); //DEBUG
                    indexes[0] = idx;
                    indexes[1] = i+1;
                    cycleInterrupt = true;
                    break;
                }
            }


            tx.run(currQuery, parameters);
            if(cycleInterrupt) {
                break;
            }
            currQuery = baseQuery;
        }
        if(!cycleInterrupt) { //Everything was fine, no need to break
            indexes[0] = -1;
            indexes[1] = -1;
        }
        System.out.println("Inserting "+ (opCounter + 1) +" departures..."); //DEBUG
        tx.commit();
        return null;
    }

    /**
     * To be called after all the other update methods, update all possible departures from each airport
     * @param iter the iterator through all the updated airports
     */
    public void updateDepartures(Iterator<Airport> iter) {
        final int BATCH_SIZE = 50;
        ArrayList<Airport> currAirports = new ArrayList<>();
        while(iter.hasNext()) {
            for(int i = 0; i<BATCH_SIZE; ++i) {
                if(!iter.hasNext()) {
                    break;
                }
                Airport current = iter.next();
                currAirports.add(current);
            }
            int[] indexes = {0,0}; //indexes[0] is the active airport index, indexes[1] is active ranking index inside the airport
            //Perform transactions until all elements in the array have been updated
            while(indexes[0] != -1) {
                try (Session session = driver.session()) {
                    session.writeTransaction(new TransactionWork<Void>() {
                        @Override
                        public Void execute(Transaction tx) {
                            return updateDepartures_query(tx, currAirports, indexes);
                        }
                    });
                }
            }
            currAirports.clear();

        }
    }


    private Void updateAirlines_query(Transaction tx, ArrayList<Airline> airlines) {
        String baseQuery = "MERGE (airline:Airline {identifier:$identifier}) \n"+
                "SET airline.name = $name, \n"+
                "airline.cancellationProb = $cancProb, \n"+
                "airline.fifteenDelayProb = $delayProb, \n"+
                "airline.qosIndicator = $qosIndicator \n"+
                "WITH airline \n";
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
                currQuery += "MERGE (airport_" + i + ":Airport {IATA_code: $IATA_" + i + "}) \n" + //if the airport is new it's created
                        "CREATE (airline)-[:SERVES {percentage: $percentage_" + i + "}]->(airport_" + i + ") \n";
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
        final int BATCH_SIZE = 1;
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
        String baseQuery = "MERGE (origin:Airport {IATA_code: $origin}) \n"+
        "MERGE (destination:Airport {IATA_code: $destination}) \n"+
        "MERGE (origin)<-[:ORIGIN]-(route:Route)-[:DESTINATION]->(destination) \n"+
        "SET route.cancellationProb = $cancProb, \n"+
            "route.fifteenDelayProb = $delayProb, \n"+
            "route.meanDelay = $meanDelay, \n"+
            "route.mostLikelyCauseDelay = $mostLikelyCauseDelay, \n"+
            "route.mostLikelyCauseCanc = $mostLikelyCauseCanc \n"+
        "WITH route \n";
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
            parameters.put("mostLikelyCauseDelay", currRoute.stats.mostLikelyCauseDelay);
            parameters.put("mostLikelyCauseCanc", currRoute.stats.mostLikelyCauseCanc);
            for (int i = 0; i < ranking.size(); ++i) {
                parameters.put("identifier_" + i, ranking.get(i).item.identifier);
                parameters.put("qos_" + i, ranking.get(i).value);
                currQuery += "MERGE (airline_" + i + ":Airline {identifier: $identifier_" + i + "}) \n" + //if the airport is new it's created
                        "CREATE (route)-[:SERVED_BY {qosIndicator: $qos_" + i + "}]->(airline_" + i + ") \n";
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
            System.out.println(currRoutes.size() + " Routes have been inserted!"); //DEBUG
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

        System.out.println("Updating all Routes in the database...");
        updateRoutes(packet.routeIterator());
        System.out.println("Routes correctly updated!");

        System.out.println("Updating all Airports in the database...");
        updateAirports(packet.airportIterator());
        System.out.println("Airports correctly updated!");


        System.out.println("Updating all possible departures in the database...");
        updateDepartures(packet.airportIterator());
        System.out.println("Departures correctly updated!");




    }


}
