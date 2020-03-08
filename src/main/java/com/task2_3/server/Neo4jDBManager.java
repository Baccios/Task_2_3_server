package com.task2_3.server;

import org.neo4j.driver.v1.*;

public class Neo4jDBManager implements DBManager,AutoCloseable{
    private Driver driver;
    public void openConnection(){
        driver= GraphDatabase.driver( "bolt://localhost:27017", AuthTokens.basic( "admin", "admin" ) );
    }
    @Override
    public void close(){
        driver.close();
    }
}
