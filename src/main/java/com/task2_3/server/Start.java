package com.task2_3.server;

public class Start {
    public static void main (String[] args) {
        //Admin_Protocol_Server myServer = new Admin_Protocol_Server(2020, "admin","ciaccio");
        /*try(Neo4jDBManager neomanager = new Neo4jDBManager("bolt://localhost:7687", "cacca", "bippa")){
            neomanager.createNode("ciccino");
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        MongoDBManager mongomanager = new MongoDBManager();
        mongomanager.openConnection();

        System.out.println("Connection opened!");
        mongomanager.buildAirlines();
        mongomanager.buildAirports();
        mongomanager.buildRoutes();
   /*     mongomanager.getIndexes_byAirline();
        mongomanager.getIndexes_byAirport();
        mongomanager.getMostServedAirports_byAirline();
        mongomanager.getMostServedAirline_byAirport();
        mongomanager.getMostServedRoute_byAirport();*/
        mongomanager.getIndexes_byRoute();

    }
}
