package com.task2_3.server;

import java.util.TimerTask;

public class Updater extends TimerTask {
    private MongoDBManager mongomanager;
    private Neo4jDBManager neomanager;

    public Updater(MongoDBManager mongomanager,Neo4jDBManager neomanager){
        this.mongomanager=mongomanager;
        this.neomanager=neomanager;
    }
    public void run(){
        UpdatePacket updatePacket=mongomanager.getUpdatePacket();
        neomanager.update(updatePacket);
    }
}
