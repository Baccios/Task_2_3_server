package com.task2_3.server;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.*;
import org.bson.conversions.*;

import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.*;
//import static com.sun.org.apache.xml.internal.security.keys.keyresolver.KeyResolver.iterator;

public class MongoDBManager {
    private MongoClient mongoClient;

    public void openConnection(){
      //    mongoClient= MongoClients.create("mongodb://localhost:27017");
        mongoClient = MongoClients.create(
                    "mongodb+srv://admin-user:nhJ1kdby9BqEj0ig@us-flights-cluster-doppu.mongodb.net/test");
    }
    public void close(){

    }


    /*
    *

MongoClient
MongoDatabase database = mongoClient.getDatabase("test");


    * */


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
    //gets the statistics for all routes
    public void getRouteStatistics(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        try(MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(Aggregates.group(new Document().append("DESTINATION_AIRPORT_ID", "$DEST_AIRPORT.DEST_AIRPORT_ID").append("ORIGIN_AIRPORT_ID", "$ORIGIN_AIRPORT.ORIGIN_AIRPORT_ID")
                    ,Accumulators.avg("meanDelay","$DEP_DELAY")  //Mean departure delay of a given route
                    ,Accumulators.sum("totalRouteCancellations","$CANCELLED") //number of cancellations of a given route
                    ,Accumulators.sum("totalRouteFlights",new Document("$sum", 1)) //total flights of a given route
                    ,Accumulators.sum( "targetRouteFlights", "$DEP_DEL15") //number of delays>15 of a given route
                    ,Accumulators.sum( "totalCarrierDelays", new Document( //number of delays caused by carrier delay
                            "$cond", new Document(
                                    //The condition is carrier delay>0 and carrier delay!=""
                                    "if", new Document("$gt", Arrays.asList("$CARRIER_DELAY", 0))
                            )
                            .append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$CARRIER_DELAY", "") )).append("then", 1).append("else", 0)))
                            .append("else", 0)
                        )
                    )
                    ,Accumulators.sum( "totalWeatherDelays", new Document(
                            "$cond", new Document(
                                    "if", new Document("$gt", Arrays.asList("$WEATHER_DELAY", 0))
                            )
                        .append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$WEATHER_DELAY", "") )).append("then", 1).append("else", 0)))
                                    .append("else", 0)
                            )
                    )
                    ,Accumulators.sum( "totalNasDelays", new Document(
                                    "$cond", new Document(
                                    "if", new Document("$gt", Arrays.asList("$NAS_DELAY", 0))
                            )
                        .append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$NAS_DELAY", "") )).append("then", 1).append("else", 0)))
                                    .append("else", 0)
                            )
                    )
                    ,Accumulators.sum( "totalSecurityDelays", new Document(
                                    "$cond", new Document(
                                    "if", new Document("$gt", Arrays.asList("$SECURITY_DELAY", 0))
                            )
                        .append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$SECURITY_DELAY", "") )).append("then", 1).append("else", 0)))
                                    .append("else", 0)
                            )
                    ),Accumulators.sum( "totalAircraftDelays", new Document(
                                    "$cond", new Document(
                                    "if", new Document("$gt", Arrays.asList("$LATE_AIRCRAFT_DELAY", 0))
                            )
                                    .append("then", new Document("$cond",new Document("if", new Document("$ne", Arrays.asList("$LATE_AIRCRAFT_DELAY", "") )).append("then", 1).append("else", 0)))
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

        ),Aggregates.sort(Sorts.ascending("totalRouteFlights")))).iterator()) {

            while (cursor.hasNext()) {
                Document doc = cursor.next();
        //        System.out.println(doc.toJson());

                Document id=(Document)doc.get("_id");

                //Data for evaluating delay probability
                int destinationId=id.getInteger("DESTINATION_AIRPORT_ID");
                Double originId=id.getDouble("ORIGIN_AIRPORT_ID"); //TODO convert to integer. Why ORIGIN_AIRPORT_ID field is double??
                double delayProbability=(doc.getInteger("targetRouteFlights")*100.0/doc.getInteger("totalRouteFlights"));

                //Data for evaluating most likely cause of delay. Each cause is given an integer.
                int  totalCarrierDelays=doc.getInteger("totalCarrierDelays");
                int totalWeatherDelays=doc.getInteger("totalWeatherDelays");
                int totalNasDelays=doc.getInteger("totalNasDelays");
                int totalSecurityDelays=doc.getInteger("totalSecurityDelays");
       //         System.out.println(totalCarrierDelays+" "+totalWeatherDelays+" "+totalNasDelays+" "+totalSecurityDelays);
                int[] delayArray={totalCarrierDelays,totalWeatherDelays,totalNasDelays,totalSecurityDelays};
                int indexOfLargest=0;
                for(int i=0;i<3;i++){
                    indexOfLargest=(delayArray[i] > delayArray[indexOfLargest])?i:indexOfLargest;
                }

                //Data for evaluating most likely cause of cancellation. Each cause is given an alphabet character.
                int cancellationsNumberCodeA=doc.getInteger("cancellationsNumberCodeA");    //code A: carrier
                int cancellationsNumberCodeB=doc.getInteger("cancellationsNumberCodeB");    //code B: weather
                int cancellationsNumberCodeC=doc.getInteger("cancellationsNumberCodeC");    //code C: national air system
                int cancellationsNumberCodeD=doc.getInteger("cancellationsNumberCodeD");    //code D: security
         //       System.out.println(cancellationsNumberCodeA+" "+cancellationsNumberCodeB+" "+cancellationsNumberCodeC+" "+cancellationsNumberCodeD);
                int[] cancellationArray={cancellationsNumberCodeA,cancellationsNumberCodeB,cancellationsNumberCodeC,cancellationsNumberCodeD};
                indexOfLargest=0;
                for(int i=0;i<3;i++){
                    indexOfLargest=(cancellationArray[i] > cancellationArray[indexOfLargest])?i:indexOfLargest;
                }

                double meanDelay=doc.getDouble("meanDelay");
                double cancellationProbability=(doc.getInteger("totalRouteCancellations")*100.0/doc.getInteger("totalRouteFlights"));

                System.out.println("destination: "+destinationId+" origin: "+originId+" Delay probability: "+delayProbability+" Mean delay: "+meanDelay+" Cancellation probability: "+cancellationProbability);
            }
      /*    double delayProbability=(targetDelays/totalDelays)*100;

            String strDouble = String.format("%.2f", delayProbability);
            System.out.println("Probability of having delay greater than 15 minutes is: ");
            System.out.println(strDouble+"%");*/
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }

    //efficient way of getting ranks by using a cursor
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
