package com.task2_3.server;

import com.mongodb.client.*;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
        HashMap<RankKey, Integer> ranking = new HashMap<>();


        try(MongoCursor<Document> cursor = collection.find().sort(orderBy(ascending("DEST_AIRPORT.DEST_AIRPORT_ID", "OP_UNIQUE_CARRIER"))).projection(include("DEST_AIRPORT.DEST_AIRPORT_ID","OP_UNIQUE_CARRIER")).iterator()) {
            Document doc = cursor.next();
            Document airport = (Document) doc.get("DEST_AIRPORT");

            Double current_airport = airport.getDouble("DEST_AIRPORT_ID");
            String current_carrier = doc.getString("OP_UNIQUE_CARRIER");
            int rank = 1;

            while (cursor.hasNext()) {
                doc = cursor.next();
                String ind_carrier = doc.getString("OP_UNIQUE_CARRIER");
                airport = (Document) doc.get("DEST_AIRPORT");
                Double ind_airport = airport.getDouble("DEST_AIRPORT_ID");

                if(!ind_airport.equals(current_airport) || !ind_carrier.equals(current_carrier)/* || current_airport == null || current_carrier == null*/){
                    ranking.put(new RankKey(current_airport, current_carrier), rank);

                    rank = 0;
                    current_airport = ind_airport;
                    current_carrier =  ind_carrier;

                }
                rank++;
            }
            ranking.put(new RankKey(current_airport, current_carrier), rank);

            for(Map.Entry<RankKey, Integer> entry: ranking.entrySet()){
                double airport_key = entry.getKey().getAirport();
                String carrier_key = entry.getKey().getCarrier();
                int rank_val = entry.getValue();
                System.out.println("aereoport: "+ airport_key + " carrier: "+carrier_key+" numero voli: "+rank_val);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

