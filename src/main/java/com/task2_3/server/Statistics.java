package com.task2_3.server;

public class Statistics {
    public double cancellationProb;
    public double fifteenDelayProb;

    public Statistics(double cancellationProb, double fifteenDelayProb) {
        this.cancellationProb = cancellationProb;
        this.fifteenDelayProb = fifteenDelayProb;
    }
    public Statistics() {
        cancellationProb = -1;
        fifteenDelayProb = -1;
    }

}
