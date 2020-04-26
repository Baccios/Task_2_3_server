package com.task2_3.server;

import java.util.Calendar;
import java.util.Timer;

public class Start {
    public static Updater updater;
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
        updater=new Updater(mongomanager,neomanager);
        setTimer(updater);
    }
    //Following method allows running the updater everyday at midnight
    public static void setTimer(Updater updater){
        Timer timer=new Timer();
        Calendar date = Calendar.getInstance();
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        // Schedule to run everyday in midnight
        timer.schedule(
                updater,
                date.getTime(),
                1000 * 60 * 60 * 24 * 7
        );
    }
}
