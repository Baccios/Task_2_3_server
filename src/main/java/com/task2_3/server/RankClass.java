package com.task2_3.server;

public class RankClass {
    static int last_rank = 1;
    String carrier_id;
    int total_flight;
    int rank;

    public RankClass(String carrier_id, int rank, int total_flight){
        this.carrier_id = carrier_id;
        this.rank = rank;
        this.total_flight = total_flight;
    }

    public int getRank() {
        return this.rank;
    }

    public String getCarrier_id(){
        return this.carrier_id;
    }

    public int getTotal_flight(){
        return this.total_flight;
    }
}
