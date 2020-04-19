package com.task2_3.server;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.*;

import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;

//import static com.sun.org.apache.xml.internal.security.keys.keyresolver.KeyResolver.iterator;

/**
 * This class implements the handler for all interactions with MongoDB server.
 * Above all, it handles the MongoDB side of the update procedure through the beginUpdate() method.
 */
public class MongoDBManager {
    private MongoClient mongoClient;

    private HashMap<String, Airport> airports; //all airports mapped by their IATA code
    private HashMap<String, Airline> airlines; //all airlines mapped by their OP_UNIQUE_CODE
    private HashMap<String, Route> routes; //all routes mapped by "$ORIGIN_IATA$DESTINATION_IATA"

    final private Document weightDocument =
            new Document("$trunc", new Document("$divide", Arrays.asList(new Document("$subtract", Arrays.asList("$$NOW",
                    new Document("$dateFromString",
                            new Document("dateString", "$FL_DATE")))), 2592000000L)));

    final private Document QoSDocument =
            new Document("$cond", Arrays.asList(new Document(
                    "$and",
                    Arrays.asList(
                            new Document("$eq", Arrays.asList("$cancProb", 0L)),
                            new Document("$eq", Arrays.asList("$delayProb", 0L)))),
            -1d,
            new Document("$divide", Arrays.asList(1L,
                    new Document("$add", Arrays.asList(new Document("$multiply", Arrays.asList("$meanDelay", "$delayProb")),
                            new Document("$multiply", Arrays.asList("$cancProb", 100L))))))));

    /**
     * Open a connection with MongoDB server, Must be called at the beginning of the application
     */
    public void openConnection(){
      //    mongoClient= MongoClients.create("mongodb://localhost:27017");
        mongoClient = MongoClients.create(
                    "mongodb+srv://admin-user:nhJ1kdby9BqEj0ig@us-flights-cluster-doppu.mongodb.net/test");
    }

    /**
     * Close a MongoDBManager
     */
    public void close(){

    }


    public void insertDocument(Document doc){

        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        collection.insertOne(doc);
    }

    public void getDocument() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        //MongoCursor<Document> cursor = collection.find(eq("QUARTER" ,1)).limit(100).iterator();
        try(MongoCursor<Document> cursor = collection.find(eq("QUARTER" ,1)).limit(100).iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Begin the read oriented database update by querying MongoDB for aggregate statistics.
     * Put results in local data structures that will be used to generate Neo4j creation scripts
     */
    public void beginUpdate() {
        //initialize local data structures
        buildAirports();
        buildAirlines();
        buildRoutes();

        //populate statistic fields through queries
        getIndexes_byAirline();
        getIndexes_byAirport();
        getMostServedAirports_byAirline();
        getIndexes_byRoute();
        //TODO: the remaining methods

        //return local data structures
        //TODO: decide how to return data structures
    }



    /**
     * Initialize the internal data structure containing all airline instances
     */
    public void buildAirlines() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Airline> temp_airlines = new HashMap<>();
        //retrieve all airlines in the database
        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Collections.singletonList(group("$OP_UNIQUE_CARRIER"))
                ).cursor()
        ) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Airline current = new Airline(doc.getString("_id"), null, null); //TODO: retrieve the name
                temp_airlines.put(doc.getString("_id"), current);
                System.out.println(doc.getString("_id"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.airlines = temp_airlines;
    }

    /**
     * Initialize the internal data structure containing all airport instances
     */
    public void buildAirports() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Airport> temp_airports = new HashMap<>();
        //retrieve all airlines in the database
        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Collections.singletonList(group("$ORIGIN_AIRPORT"))
                ).cursor()
        ) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document)doc.get("_id");
                Airport current = new Airport(id.getDouble("ORIGIN_AIRPORT_ID").intValue(),
                                                id.getString("ORIGIN_IATA"),
                                                null, //TODO: retrieve airport name
                                                id.getString("ORIGIN_CITY_NAME"),
                                                id.getString("ORIGIN_STATE_NM"),
                                                null
                                    );
                temp_airports.put(id.getString("ORIGIN_IATA"), current);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.airports = temp_airports;
    }

    /**
     * Initialize the internal data structure containing all route instances. Must be called after buildAirports() and buildAirlines()
     */
    public void buildRoutes() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Route> temp_routes = new HashMap<>();

        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Collections.singletonList(group(and(eq("origin", "$ORIGIN_AIRPORT.ORIGIN_IATA"),
                                eq("destination", "$DEST_AIRPORT.DEST_IATA"))))
                ).cursor()
        ) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document)doc.get("_id");
                Airport originAirport = airports.get(id.getString("origin"));
                Airport destAirport = airports.get(id.getString("destination"));
                Route temp = new Route(originAirport, destAirport, null);
                temp_routes.put(temp.toCode(), temp);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.routes = temp_routes;
    }


    /**
     * Fill all airlines with the statistics regarding numerical indexes.
     * Initialize the stats field in each airline if it's null.
     */
    public void getIndexes_byAirport() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(
                                addFields(new Field("weight", weightDocument),
                                        new Field("DEP_DELAY",
                                                new Document("$max", Arrays.asList(0L, "$DEP_DELAY")))
                                ),
                                group("$ORIGIN_AIRPORT.ORIGIN_IATA",
                                        sum("DelaySum", eq("$divide", Arrays.asList("$DEP_DELAY", "$weight"))),
                                        sum("Delay15Sum", eq("$divide", Arrays.asList("$DEP_DEL15", "$weight"))),
                                        sum("CancSum", eq("$divide", Arrays.asList("$CANCELLED", "$weight"))),
                                        sum("WeightsSum", eq("$divide", Arrays.asList(1L, "$weight")))
                                ),
                                addFields(
                                        new Field("meanDelay",
                                                new Document("$divide", Arrays.asList("$DelaySum", "$WeightsSum"))),
                                        new Field("delayProb",
                                                new Document("$divide", Arrays.asList("$Delay15Sum", "$WeightsSum"))),
                                        new Field("cancProb",
                                                new Document("$divide", Arrays.asList("$CancSum", "$WeightsSum")))),
                                addFields(
                                        new Field("QoSIndex", QoSDocument)
                                ))
                ).cursor()
        ) {
            Airport currAirport = null;
            AirportStatistics currStats = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                currAirport = this.airports.get(doc.getString("_id"));

                currStats = currAirport.stats == null ? new AirportStatistics() : currAirport.stats;
                currAirport.stats = currStats;
                currStats.cancellationProb = doc.getDouble("cancProb");
                currStats.fifteenDelayProb = doc.getDouble("delayProb");
                currStats.qosIndicator = doc.getDouble("QoSIndex");
                System.out.println(doc.toJson()); //DEBUG
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getMostServedRoute_byAirport(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Double> totalWeights = new HashMap<>();
        //Retrieve total weights sum by airport to normalize result values
        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group(eq("origin", "$ORIGIN_AIRPORT.ORIGIN_IATA"),
                                        sum("SumOfWeights",
                                                eq("$divide", Arrays.asList(1L, "$weight")))),
                                sort(ascending("_id")))
                ).cursor()
        ) {

            while(cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document) doc.get("_id");
                totalWeights.put(id.getString("origin"), doc.getDouble("SumOfWeights"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Retrieving the MostServedRoute query
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group(and(eq("origin", "$ORIGIN_AIRPORT.ORIGIN_IATA"),
                                        eq("destination", "$DEST_AIRPORT.DEST_IATA")),
                                        sum("serviceCount",
                                                eq("$divide",
                                                        Arrays.asList(1L, "$weight")))),
                                project(include("serviceCount")),
                                sort(orderBy(ascending("_id.origin", "_id.destination"),
                                        descending("serviceCount"))))
                ).cursor()
        ) {
            Airport currOriginAirport = null;
            Airport currDestAirport = null;
            AirportStatistics currStats = null;
            Double currentWeight = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document)doc.get("_id");
                if(currOriginAirport == null || !id.getString("origin").equals(currOriginAirport)) {
                    currOriginAirport = this.airports.get(id.getString("origin"));
                    currDestAirport = this.airports.get(id.getString("destination"));
                    currStats = currOriginAirport.stats == null ? new AirportStatistics() : currOriginAirport.stats;
                    currStats.mostServedRoutes = new HashMap<>();
                    currOriginAirport.stats = currStats;
                    currentWeight = totalWeights.get(currOriginAirport.IATA_code);
                }
                Double currValue = doc.getDouble("serviceCount")/currentWeight;
                //System.out.println(currValue + ", " + doc.getDouble("serviceCount") ); //DEBUG
                Route currRoute = routes.get(currOriginAirport.IATA_code + currDestAirport.IATA_code);
                currStats.mostServedRoutes.put(currValue, currRoute);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getMostServedAirline_byAirport(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Double> totalWeights = new HashMap<>();
        //Retrieve total weights sum by airport to normalize result values
        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group("$ORIGIN_AIRPORT.ORIGIN_IATA",
                                        sum("SumOfWeights",
                                                eq("$divide", Arrays.asList(1L, "$weight")))),
                                sort(ascending("_id")))
                ).cursor()
        ) {

            while(cursor.hasNext()) {
                Document doc = cursor.next();
                //System.out.println(doc.toJson()); //DEBUG
                totalWeights.put(doc.getString("_id"), doc.getDouble("SumOfWeights"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Retrieving the MostServedAirline query
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group(and(eq("airline", "$OP_UNIQUE_CARRIER"),
                                        eq("origin", "$ORIGIN_AIRPORT.ORIGIN_IATA")),
                                        sum("serviceCount",
                                                eq("$divide",
                                                        Arrays.asList(1L, "$weight")))),
                                project(include("serviceCount")),
                                sort(orderBy(ascending("_id.origin"),
                                        descending("serviceCount"))))
                ).cursor()
        ) {
            Airport currAirport = null;
            AirportStatistics currStats = null;
            Double currentWeight = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document)doc.get("_id");
                if(currAirport == null || !id.getString("origin").equals(currAirport.identifier)) {
                    currAirport = this.airports.get(id.getString("origin"));
                    currStats = currAirport.stats == null ? new AirportStatistics() : currAirport.stats;
                    currStats.mostServedAirlines = new HashMap<>();
                    currAirport.stats = currStats;
                    currentWeight = totalWeights.get(currAirport.IATA_code);
                }
                Double currValue = doc.getDouble("serviceCount")/currentWeight;
                //System.out.println(currValue + ", " + doc.getDouble("serviceCount") ); //DEBUG
                Airline currAirline = airlines.get(id.getString("origin.OP_UNIQUE_CARRIER"));
                currStats.mostServedAirlines.put(currValue, currAirline);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Fill all airlines with the statistics regarding the map of the most served airports.
     * Initialize the stats field in each airline if it's null.
     */
    public void getMostServedAirports_byAirline() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<String, Double> totalWeights = new HashMap<>();
        //Retrieve total weights sum by airline to normalize result values
        try(
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group("$OP_UNIQUE_CARRIER",
                                        sum("SumOfWeights",
                                                eq("$divide", Arrays.asList(1L, "$weight")))),
                                sort(ascending("_id")))
                ).cursor()
        ) {

            while(cursor.hasNext()) {
                Document doc = cursor.next();
                //System.out.println(doc.toJson()); //DEBUG
                totalWeights.put(doc.getString("_id"), doc.getDouble("SumOfWeights"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Retrieving the MostServedAirports query
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(addFields(new Field("weight", weightDocument)),
                                group(and(eq("airline", "$OP_UNIQUE_CARRIER"),
                                        eq("origin", "$ORIGIN_AIRPORT")),
                                        sum("serviceCount",
                                                eq("$divide",
                                                        Arrays.asList(1L, "$weight")))),
                                project(include("serviceCount")),
                                sort(orderBy(ascending("_id.airline"),
                                        descending("serviceCount"))))
                ).cursor()
        ) {
            Airline currAirline = null;
            AirlineStatistics currStats = null;
            Double currentWeight = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id = (Document)doc.get("_id");
                if(currAirline == null || !id.getString("airline").equals(currAirline.identifier)) {
                    currAirline = this.airlines.get(id.getString("airline"));
                    currStats = currAirline.stats == null ? new AirlineStatistics() : currAirline.stats;
                    currStats.mostServedAirports = new HashMap<>();
                    currAirline.stats = currStats;
                    currentWeight = totalWeights.get(currAirline.identifier);
                }
                Double currValue = doc.getDouble("serviceCount")/currentWeight;
                //System.out.println(currValue + ", " + doc.getDouble("serviceCount") ); //DEBUG
                Airport currAirport = airports.get(id.getString("origin.ORIGIN_IATA"));
                currStats.mostServedAirports.put(currValue, currAirport);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Fill all airlines with the statistics regarding numerical indexes.
     * Initialize the stats field in each airline if it's null.
     */
    public void getIndexes_byAirline() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(
                                addFields(new Field("weight", weightDocument),
                                    new Field("DEP_DELAY",
                                        new Document("$max", Arrays.asList(0L, "$DEP_DELAY")))
                                ),
                                group("$OP_UNIQUE_CARRIER",
                                    sum("DelaySum", eq("$divide", Arrays.asList("$DEP_DELAY", "$weight"))),
                                    sum("Delay15Sum", eq("$divide", Arrays.asList("$DEP_DEL15", "$weight"))),
                                    sum("CancSum", eq("$divide", Arrays.asList("$CANCELLED", "$weight"))),
                                    sum("WeightsSum", eq("$divide", Arrays.asList(1L, "$weight")))
                                ),
                                addFields(
                                    new Field("meanDelay",
                                        new Document("$divide", Arrays.asList("$DelaySum", "$WeightsSum"))),
                                    new Field("delayProb",
                                        new Document("$divide", Arrays.asList("$Delay15Sum", "$WeightsSum"))),
                                    new Field("cancProb",
                                        new Document("$divide", Arrays.asList("$CancSum", "$WeightsSum")))),
                                addFields(
                                    new Field("QoSIndex", QoSDocument)
                                ),
                                project(include("meanDelay", "delayProb", "cancProb", "QoSIndex")))
                ).cursor()
        ) {
            Airline currAirline = null;
            AirlineStatistics currStats = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                currAirline = this.airlines.get(doc.getString("_id"));
                currStats = currAirline.stats == null ? new AirlineStatistics() : currAirline.stats;
                currAirline.stats = currStats;
                currStats.cancellationProb = doc.getDouble("cancProb");
                currStats.fifteenDelayProb = doc.getDouble("delayProb");
                currStats.qosIndicator = doc.getDouble("QoSIndex");
                //System.out.println(doc.toJson()); //DEBUG
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Fill all routes with their statistics.
     * Initialize the stats field in each route if it's null.
     */
    public void getIndexes_byRoute(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        try(MongoCursor<Document> cursor = collection.aggregate(
                Arrays.asList(
                        addFields(new Field("weight", weightDocument),
                                new Field("DEP_DELAY",
                                        new Document("$max", Arrays.asList(0L, "$DEP_DELAY")))
                        ),
                        Aggregates.group(new Document().append("DEST_IATA", "$DEST_AIRPORT.DEST_IATA").append("ORIGIN_IATA", "$ORIGIN_AIRPORT.ORIGIN_IATA"),
                                sum("DelaySum", eq("$divide", Arrays.asList("$DEP_DELAY", "$weight"))),
                                sum("Delay15Sum", eq("$divide", Arrays.asList("$DEP_DEL15", "$weight"))),
                                sum("CancSum", eq("$divide", Arrays.asList("$CANCELLED", "$weight"))),
                                sum("WeightsSum", eq("$divide", Arrays.asList(1L, "$weight"))),
                                sum( "totalCarrierDelays", new Document( //number of delays caused by carrier delay
                                    "$cond", new Document(
                                            //The condition is carrier delay>15 and carrier delay!=""
                                            "if", new Document("$gt", Arrays.asList("$CARRIER_DELAY", 15))
                                    )
                                    //.append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$CARRIER_DELAY", "") )).append("then", 1).append("else", 0)))
                                    .append("then", 1)
                                    .append("else", 0)
                                )
                            )
                            ,Accumulators.sum( "totalWeatherDelays", new Document(
                                    "$cond", new Document(
                                            "if", new Document("$gt", Arrays.asList("$WEATHER_DELAY", 15))
                                    ).append("then", 1)
                                     .append("else", 0)
                                )
                            )
                            ,Accumulators.sum( "totalNasDelays", new Document(
                                    "$cond", new Document(
                                            "if", new Document("$gt", Arrays.asList("$NAS_DELAY", 15))
                                    )
                                    .append("then", 1)
                                    .append("else", 0)
                                )
                            )
                            ,Accumulators.sum( "totalSecurityDelays", new Document(
                                    "$cond", new Document(
                                            "if", new Document("$gt", Arrays.asList("$SECURITY_DELAY", 15))
                                    )
                                    .append("then", 1)
                                    .append("else", 0)
                                )
                            ),Accumulators.sum( "totalAircraftDelays", new Document(
                                    "$cond", new Document(
                                            "if", new Document("$gt", Arrays.asList("$LATE_AIRCRAFT_DELAY", 15))
                                    )
                                    .append("then", 1)
                                    .append("else", 0)
                                )
                            ),Accumulators.sum( "cancellationsNumberCodeA", new Document(   //total of cancellations caused by carrier
                                        "$cond", new Document(
                                        "if", new Document("$eq", Arrays.asList("$CANCELLATION_CODE", "A"))
                                )
                                        .append("then", 1)
                                        .append("else", 0)
                                )
                            ),Accumulators.sum( "cancellationsNumberCodeB", new Document( //total of cancellations caused by weather
                                        "$cond", new Document(
                                        "if", new Document("$eq", Arrays.asList("$CANCELLATION_CODE", "B"))
                                )
                                        .append("then", 1)
                                        .append("else", 0)
                                )
                            ),Accumulators.sum( "cancellationsNumberCodeC", new Document( //total of cancellations caused by national air system
                                        "$cond", new Document(
                                        "if", new Document("$eq", Arrays.asList("$CANCELLATION_CODE", "C"))
                                )
                                        .append("then", 1)
                                        .append("else", 0)
                                )
                            ),Accumulators.sum( "cancellationsNumberCodeD", new Document( //total of cancellations caused by security
                                        "$cond", new Document(
                                        "if", new Document("$eq", Arrays.asList("$CANCELLATION_CODE", "D"))
                                    )
                                        .append("then", 1)
                                        .append("else", 0)
                                )
                            )
                        )
                        ,addFields(
                                        new Field("meanDelay",
                                                new Document("$divide", Arrays.asList("$DelaySum", "$WeightsSum"))),
                                        new Field("delayProb",
                                                new Document("$divide", Arrays.asList("$Delay15Sum", "$WeightsSum"))),
                                        new Field("cancProb",
                                                new Document("$divide", Arrays.asList("$CancSum", "$WeightsSum"))))

                        /*,Aggregates.sort(Sorts.ascending("totalRouteFlights"))*/)).iterator()) {
            Route currRoute = null;
            RouteStatistics currStats = null;
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id=(Document)doc.get("_id");

                //Data for evaluating most likely cause of delay. Each cause is given an integer.
                int totalCarrierDelays=doc.getInteger("totalCarrierDelays");
                int totalWeatherDelays=doc.getInteger("totalWeatherDelays");
                int totalNasDelays=doc.getInteger("totalNasDelays");
                int totalSecurityDelays=doc.getInteger("totalSecurityDelays");
                int[] delayArray={totalCarrierDelays,totalWeatherDelays,totalNasDelays,totalSecurityDelays};
                int indexOfLargest=0;
                for(int i=0;i<3;i++){
                    indexOfLargest=(delayArray[i] > delayArray[indexOfLargest])?i:indexOfLargest;
                }
                String mostLikelyCauseDelay=null;
                switch(indexOfLargest){
                    case 0:
                        mostLikelyCauseDelay="Carrier";
                        break;
                    case 1:
                        mostLikelyCauseDelay="Weather";
                        break;
                    case 2:
                        mostLikelyCauseDelay="National Air System";
                        break;
                    case 3:
                        mostLikelyCauseDelay="Security";
                        break;
                }

                //Data for evaluating most likely cause of cancellation. Each cause is given an alphabet character.
                int cancellationArray[]=new int[4];
                cancellationArray[0]=doc.getInteger("cancellationsNumberCodeA");    //code A: carrier
                cancellationArray[1]=doc.getInteger("cancellationsNumberCodeB");    //code B: weather
                cancellationArray[2]=doc.getInteger("cancellationsNumberCodeC");    //code C: national air system
                cancellationArray[3]=doc.getInteger("cancellationsNumberCodeD");    //code D: security
                indexOfLargest=0;
                for(int i=0;i<3;i++){
                    indexOfLargest=(cancellationArray[i] > cancellationArray[indexOfLargest])?i:indexOfLargest;
                }
                String mostLikelyCauseCanc=null;
                switch(indexOfLargest){
                    case 0:
                        mostLikelyCauseCanc="Carrier";
                        break;
                    case 1:
                        mostLikelyCauseCanc="Weather";
                        break;
                    case 2:
                        mostLikelyCauseCanc="National Air System";
                        break;
                    case 3:
                        mostLikelyCauseCanc="Security";
                        break;
                    default:
                        mostLikelyCauseCanc="";
                        break;
                }

                String destinationId=id.getString("DEST_IATA");
                String originId=id.getString("ORIGIN_IATA");

                currRoute = this.routes.get(originId+destinationId);
                if(currRoute==null){
                    System.out.println("Route doesn't exist! (must update route map)");
                    System.exit(1);
                }
                currStats = currRoute.stats == null ? new RouteStatistics() : currRoute.stats;
                currRoute.stats = currStats;
                currStats.meanDelay = doc.getDouble("meanDelay");
                currStats.cancellationProb = doc.getDouble("cancProb");
                currStats.fifteenDelayProb = doc.getDouble("delayProb");
                currStats.mostLikelyCauseDelay = mostLikelyCauseDelay;
                currStats.mostLikelyCauseCanc = mostLikelyCauseCanc;

            }
            getAirlinesRanking_byRoute();
      /*
            String strDouble = String.format("%.2f", delayProbability);*/
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    public void getAirlinesRanking_byRoute() {
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        try (
                MongoCursor<Document> cursor = collection.aggregate(
                        Arrays.asList(
                                addFields(new Field("weight", weightDocument),
                                        new Field("DEP_DELAY",
                                                new Document("$max", Arrays.asList(0L, "$DEP_DELAY")))
                                ),
                                group(new Document().append("DEST_IATA", "$DEST_AIRPORT.DEST_IATA").append("ORIGIN_IATA", "$ORIGIN_AIRPORT.ORIGIN_IATA").append("OP_UNIQUE_CARRIER", "$OP_UNIQUE_CARRIER"),
                                        sum("DelaySum", eq("$divide", Arrays.asList("$DEP_DELAY", "$weight"))),
                                        sum("Delay15Sum", eq("$divide", Arrays.asList("$DEP_DEL15", "$weight"))),
                                        sum("CancSum", eq("$divide", Arrays.asList("$CANCELLED", "$weight"))),
                                        sum("WeightsSum", eq("$divide", Arrays.asList(1L, "$weight")))
                                ),
                                addFields(
                                        new Field("meanDelay",
                                                new Document("$divide", Arrays.asList("$DelaySum", "$WeightsSum"))),
                                        new Field("delayProb",
                                                new Document("$divide", Arrays.asList("$Delay15Sum", "$WeightsSum"))),
                                        new Field("cancProb",
                                                new Document("$divide", Arrays.asList("$CancSum", "$WeightsSum")))),
                                addFields(
                                        new Field("QoSIndex", QoSDocument)
                                ),
                                sort(orderBy(ascending("_id.DEST_AIRPORT","_id.ORIGIN_AIRPORT"), ascending("QoSIndex")))
                        )
                ).cursor()
        ) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Document id=(Document)doc.get("_id");
                String destinationId=id.getString("DEST_IATA");
                String originId=id.getString("ORIGIN_IATA");

                Route currRoute = this.routes.get(originId+destinationId);
                Airline currAirline=this.airlines.get(id.getString("OP_UNIQUE_CARRIER"));
                RouteStatistics currStats=currRoute.stats;
                currStats.bestAirlines=(currStats.bestAirlines==null)?new HashMap<>():currStats.bestAirlines;
                currStats.bestAirlines.put(doc.getDouble("QoSIndex"),currAirline);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //efficient way of getting ranks by using a cursor
    /**
     * Fill all airports with the statistics regarding the map of the most served airlines.
     * Initialize the stats field in each airport if it's null.
     */
    //TODO: change this method name to a more specific one
    public void getRanking(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");


        //TODO choose if cursor or not, this solution is not MEMORY SAFE and slower than the equivalent with cursors, for testing remove Accumulators.push https://stackoverflow.com/questions/44587829/sorting-and-ranking-documents-based-on-key-values
        //if this solution is chosen delete RankClass and RankKey
        try(MongoCursor<Document> cursor = collection.aggregate(
                Arrays.asList(
                        Aggregates.group(new Document("DEST_AIRPORT", "$DEST_AIRPORT.DEST_AIRPORT_ID").append("OP_UNIQUE_CARRIER", "$OP_UNIQUE_CARRIER"), Accumulators.sum("count", 1)/*, Accumulators.push("items", new Document("push", "$$ROOT"))*/),
                        Aggregates.sort(orderBy(ascending("_id.DEST_AIRPORT"), descending("count")))
                        )
             ).iterator())
        {
            Document doc;
            Document id;
            int rank = 1;
            int current_airport = 0;

            while (cursor.hasNext()) {
                doc = (Document) cursor.next();
                id = (Document) doc.get("_id");

                String ind_carrier = id.getString("OP_UNIQUE_CARRIER" );
                int ind_airport = id.getInteger("DEST_AIRPORT" );
                rank++;
                if(current_airport != ind_airport) {
                    rank = 1;
                    current_airport = ind_airport;
                }
                System.out.println(ind_airport + "              " + ind_carrier + "            " + doc.getInteger("count") + "          " + rank);
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }

        //TODO this solution is better but doesn't exploit aggregations
        /*try(MongoCursor<Document> cursor = collection.find().sort(orderBy(ascending("DEST_AIRPORT.DEST_AIRPORT_ID", "OP_UNIQUE_CARRIER"))).projection(include("DEST_AIRPORT.DEST_AIRPORT_ID","OP_UNIQUE_CARRIER")).iterator()) {
            Document doc = cursor.next();
            Document airport = (Document) doc.get("DEST_AIRPORT");

            int current_airport = airport.getInteger("DEST_AIRPORT_ID");
            String current_carrier = doc.getString("OP_UNIQUE_CARRIER");
            int num_flight = 1;

            Vector<RankClass> rank_arr = new Vector<>();

            while (cursor.hasNext()) {
                doc = cursor.next();
                String ind_carrier = doc.getString("OP_UNIQUE_CARRIER");
                airport = (Document) doc.get("DEST_AIRPORT");
                int ind_airport = airport.getInteger("DEST_AIRPORT_ID");

                if(ind_airport != current_airport || !ind_carrier.equals(current_carrier)){
                    //ranking.put(new RankKey(current_airport, current_carrier), rank);

                    rank_arr.add(new RankClass(current_carrier, RankClass.last_rank, num_flight));
                    ranking.put(current_airport, rank_arr);

                    num_flight = 0;
                    current_airport = ind_airport;
                    current_carrier =  ind_carrier;
                }
                rank++;
            }
            //add last one
            ranking.put(new RankKey(current_airport, current_carrier), rank);

            for(Map.Entry<RankKey, Integer> entry: ranking.entrySet()){
                int airport_key = entry.getKey().getAirport();
                String carrier_key = entry.getKey().getCarrier();
                int rank_val = entry.getValue();
                System.out.println("aereoport: "+ airport_key + " carrier: "+carrier_key+" numero voli: "+rank_val);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }*/
    }

}

