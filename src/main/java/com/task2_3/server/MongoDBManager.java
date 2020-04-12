package com.task2_3.server;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.*;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.*;
//import static com.sun.org.apache.xml.internal.security.keys.keyresolver.KeyResolver.iterator;

public class MongoDBManager {
    private MongoClient mongoClient;

    public void openConnection(){
        //mongoClient= MongoClients.create("mongodb://localhost:27017");
        mongoClient = MongoClients.create(
                "mongodb+srv://admin-user:nhJ1kdby9BqEj0ig@us-flights-cluster-doppu.mongodb.net/test?retryWrites=true&w=majority");
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
    //efficient way of getting ranks by using a cursor
    public void getRanking(){
        MongoDatabase database = mongoClient.getDatabase("us_flights_db");
        MongoCollection<Document> collection = database.getCollection("us_flights");
        HashMap<Integer, Vector<RankClass>> ranking = new HashMap<>();


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

