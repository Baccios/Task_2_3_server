package com.task2_3.server;


import java.util.HashMap;

public class AirportStatistics extends Statistics {

    public double importance;
    public String mostLikelyCauseDelay;
    public String mostLikelyCauseCanc;
    public HashMap<Double, Route> mostServedRoutes; //each element is an array [Percentage, Route]
    public HashMap<Double, Airline> mostServedAirlines; //each element is an array [Percentage, Airline]

    public AirportStatistics(double cancellationProb, double fifteenDelayProb, double importance, String mostLikelyCauseDelay, String mostLikelyCauseCanc, HashMap<Double, Route> mostServedRoutes, HashMap<Double, Airline> mostServedAirlines) {
        super(cancellationProb, fifteenDelayProb);
        this.importance = importance;
        this.mostLikelyCauseDelay = mostLikelyCauseDelay;
        this.mostLikelyCauseCanc = mostLikelyCauseCanc;
        this.mostServedRoutes = new HashMap<>(mostServedRoutes);
        this.mostServedAirlines = new HashMap<>(mostServedAirlines);
    }

}
/*
db.orders.aggregate(
        {$group: {_id: {airport_name: "$airport_name", airline_name: "$airline_name"}, },
        )
*/
 /*
        var current = null,
        rank = 0;

        db.us_flights_db.find().sort({ "name": 1, "marks": -1 }).forEach(doc => {
        if ( doc.name != current || current == null ) {
        rank = 0;
        current = doc.name;
        }
        rank++;
        doc.rank = rank;
        delete doc._id;
        printjson(doc);
        })
 */

 /*
 findAirline
db.airline.find(
    {airline_name: "nome"}
)
  */