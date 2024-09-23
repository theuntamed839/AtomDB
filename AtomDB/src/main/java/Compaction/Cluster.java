package Compaction;

import db.KVUnit;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

public class Cluster {
    private final List<KVUnit> units;

    public Cluster(List<KVUnit> units) {
        this.units = units;
    }

    public List<KVUnit> getUnits() {
        return Collections.unmodifiableList(units);
    }
}
