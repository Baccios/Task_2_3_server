package com.task2_3.server;

public class RankKey {
    int airport_id;
    String carrier_id;

    public RankKey(int airport_id, String carrier_id){
        this.airport_id = airport_id;
        this.carrier_id = carrier_id;
    }

    public int getAirport(){
        return airport_id;
    }

    public String getCarrier(){
        return carrier_id;
    }
}
