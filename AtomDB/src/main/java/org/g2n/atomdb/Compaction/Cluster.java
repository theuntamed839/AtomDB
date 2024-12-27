package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.db.KVUnit;

import java.util.Collections;
import java.util.List;

public class Cluster {
    private final List<KVUnit> units;

    public Cluster(List<KVUnit> units) {
        this.units = units;
    }

    public List<KVUnit> getUnits() {
        return Collections.unmodifiableList(units);
    }

    public byte[] getSmallestKeyInCluster() {
        return units.getFirst().getKey();
    }

    public byte[] getGreatestKeyInCluster() {
        return units.getLast().getKey();
    }
}
