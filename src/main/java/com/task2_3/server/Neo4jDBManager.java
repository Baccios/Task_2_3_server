package com.task2_3.server;

import org.neo4j.driver.*;
import org.neo4j.driver.v1.*;
import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;


public class Neo4jDBManager implements AutoCloseable {
    private final Driver driver;

    public Neo4jDBManager(String uri, String user, String password){
        //driver initialization using user and password of the neo4jdatabase
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );

    }

    //returns an instance of the Neo4jDBManager with driver initialized
    public Neo4jDBManager openConnection(String uri, String user, String password){
       Neo4jDBManager GraphDBManager = new Neo4jDBManager(uri,user,password);
       return GraphDBManager;
    }

    @Override
    public void close() throws Exception{
        driver.close();
    }


    public void addNode (String nodeName) {
        try (Session session = driver.session()) {
            StatementResult cursor = session.run(query.withParameters(
                    Values.parameters( "searchText", "@html.it" ) ));
        }
    }

    public void matchNode (String nodeName) {
        try (Session session = driver.session()) {
            session.readTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    StatementResult result = tx.run("MATCH (n:Node {name: $nodeName}) RETURN name(a)", parameters("nodeName", nodeName));
                    return result.single().get(0).asString();
                }
            });

        }
    }

        public void createNode (String nodeName){
            try(Session session = driver.session()){
                session.writeTransaction(new TransactionWork<String>() {
                    @Override
                    public String execute(Transaction tx){
                        tx.run("CREATE (n:Node {name:$nodeName})",parameters("nodeName",nodeName));
                        return null;
                    }
                });

            }



    }

}
