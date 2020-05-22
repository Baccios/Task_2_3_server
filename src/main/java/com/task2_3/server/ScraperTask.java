package com.task2_3.server;

import java.util.Calendar;
import java.util.TimerTask;

public class ScraperTask extends TimerTask {

    public void run(){
        Scraper scraper = new Scraper();
        Start.lastScrapeOutcome = scraper.startScraping();
        scraper.close();
        Start.setTimerScraper(); //reschedule the timer
    }
}