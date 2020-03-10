package com.task2_3.server;

import com.mongodb.client.*;
import org.bson.Document;

public class MongoDBManager {
    private MongoClient mongoClient;

    public void openConnection(){
        mongoClient= MongoClients.create("mongodb://localhost:27017");
    }
    public void close(){

    }

    public void insertDocument(Document doc){

        MongoDatabase database = mongoClient.getDatabase("mydb");
        MongoCollection<Document> collection = database.getCollection("test");
        collection.insertOne(doc);
    }

    public void getDocument() {
        MongoDatabase database = mongoClient.getDatabase("mydb");
        MongoCollection<Document> collection = database.getCollection("test");
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                //TODO
            }
        } finally {
            cursor.close();
        }
    }
}
