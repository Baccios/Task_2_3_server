package com.task2_3.server;

public class RankingItem<ItemType> {
    double value;
    ItemType item;

    public RankingItem(double value, ItemType item) {
        this.value = value;
        this.item = item;
    }
}
