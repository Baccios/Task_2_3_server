package com.task2_3.server;

import java.util.Calendar;
import java.util.Timer;

public class Start {
    public static boolean lastScrapeOutcome = true;
    public static Timer updateTimer = new Timer();
    public static Timer scrapeTimer = new Timer();

    public static void main (String[] args) throws Exception {
        setTimerScraper();
        setTimerUpdater();
        Admin_Protocol_Server myServer = new Admin_Protocol_Server(2020, "admin","ciaccio");

        /*
        Neo4jDBManager neomanager = new Neo4jDBManager("bolt://172.16.1.15:7687", "neo4j", "root");
        MongoDBManager mongomanager = new MongoDBManager();

        PasswordManager pswman = new PasswordManager();
        pswman.setCredentials("admin", "admin");
        System.out.println(pswman.authenticate("admin", "admin"));
        System.out.println(pswman.authenticate("admin", "arribaia"));

        //neomanager.update(mongomanager.getUpdatePacket());
        mongomanager.close();
        neomanager.close();

        /*
        System.out.println("Connection opened!");
        updater=new Updater(mongomanager,neomanager);
        setTimer(updater);
         */
    }
    //Following method allows running the updater everyday at midnight
    public static void setTimerUpdater(){
        UpdaterTask updater = new UpdaterTask();
        Calendar date = Calendar.getInstance();
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);

        //first execution is now set 30 days from now at midnight
        date.add(Calendar.DAY_OF_MONTH, 30);

        updateTimer.schedule(
                updater,
                date.getTime(),
                1000 * 60 * 60 * 24 * 15 //database is updated every 15 days
        );
    }

    //Following method allows running the updater everyday at midnight
    public static void setTimerScraper(){
        ScraperTask scraper = new ScraperTask();
        Calendar date = Calendar.getInstance();
        date.set(Calendar.HOUR, 0);
        date.set(Calendar.MINUTE, 0);
        date.set(Calendar.SECOND, 0);
        date.set(Calendar.MILLISECOND, 0);
        //date is now set today at midnight
        if(lastScrapeOutcome)
            date.add(Calendar.DAY_OF_MONTH, 30);
        else
            date.add(Calendar.DAY_OF_MONTH, 3); //in case of errors on the site retry in three days

        scrapeTimer.schedule(
                scraper,
                date.getTime()
        );
    }

}
