package com.task2_3.server;

public class Start {
    public static void main (String[] args) {
        System.out.println("Hello my baby!");
        //Admin_Protocol_Server myServer = new Admin_Protocol_Server(2020, "admin","ciaccio");
        try(Neo4jDBManager neomanager = new Neo4jDBManager("bolt://localhost:7687", "cacca", "bippa")){
            neomanager.createNode("ciccino");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
