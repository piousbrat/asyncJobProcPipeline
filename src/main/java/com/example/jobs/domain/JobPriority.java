package com.example.jobs.domain;

public enum JobPriority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final int rank;

    JobPriority(int rank) {
        this.rank = rank;
    }

    public int rank() {
        return rank;
    }
}
