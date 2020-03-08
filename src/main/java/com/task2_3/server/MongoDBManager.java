package com.task2_3.server;

import com.mongodb.client.*;

public class MongoDBManager implements DBManager{
    private MongoClient mongoClient;

    public void openConnection(){
        mongoClient= MongoClients.create("mongodb://localhost:27017");
    }
    public void close(){

    }
}
