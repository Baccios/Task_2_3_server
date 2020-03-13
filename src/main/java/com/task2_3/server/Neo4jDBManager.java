package com.task2_3.server;

import org.neo4j.driver.*;
import static org.neo4j.driver.Values.parameters;


public class Neo4jDBManager implements AutoCloseable {
    private final Driver driver;

    public Neo4jDBManager(String uri, String user, String password){
        //driver initialization using user and password of the neo4jdatabase
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );

    }

    @Override
    public void close() {
        driver.close();
    }



    public String matchNode (String nodeName) {
        try (Session session = driver.session()) {
            return session.readTransaction(tx -> {
                Result result = tx.run("MATCH (n:Node {name: $nodeName}) RETURN name(a)", parameters("nodeName", nodeName));
                return result.single().get(0).asString();
            });
        }
    }

        public void createNode (String nodeName){
            try(Session session = driver.session()){
                session.writeTransaction((TransactionWork<String>) tx -> {
                    tx.run("CREATE (n:Node {name:$nodeName})",parameters("nodeName",nodeName));
                    return null;
                });

            }



    }

}
