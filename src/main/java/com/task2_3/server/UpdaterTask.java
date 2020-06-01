package com.task2_3.server;

import java.util.TimerTask;

public class UpdaterTask extends TimerTask {

    public void run(){
        MongoDBManager mongomanager = new MongoDBManager();
        Neo4jDBManager neomanager = new Neo4jDBManager();

        UpdatePacket updatePacket= mongomanager.getUpdatePacket();
        neomanager.update(updatePacket);
        mongomanager.close();
        neomanager.close();
    }
}
