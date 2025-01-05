package org.g2n.atomdb.Compaction;

import org.g2n.atomdb.Level.Level;
import org.g2n.atomdb.Table.SSTFileHelper;
import org.g2n.atomdb.Table.SSTInfo;
import org.g2n.atomdb.db.KVUnit;
import org.g2n.atomdb.sstIo.MMappedReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Validator {

    public static void validateSSTBasedOnSearch(SSTInfo sstInfo, byte[] key) throws IOException {
        System.out.println("TEST".repeat(30));
        System.out.println("SST: "+ sstInfo.getSst().getName());
        System.out.println(new String(key));

        // exists in plain search
        KVUnit kvUnit = linearSearch(sstInfo, key);
        if (kvUnit == null) {
            System.out.println("Key not found, not moving forward");
            return;
        }

        if (!sstInfo.getSstKeyRange().inRange(key)) {
            System.out.println(Arrays.toString(sstInfo.getSstKeyRange().getSmallest()));
            System.out.println(Arrays.toString(sstInfo.getSstKeyRange().getGreatest()));
            throw new RuntimeException("Key is not in range, even though it was found in it.");
        }

        Pointer found = null;
        for (Pointer pointer : sstInfo.getPointers().getList()) {
            if (Arrays.compare(pointer.key(), key) <= 0) {
                found = pointer;
            }
        }
        if (found == null) {
            System.out.println("Key not found");
            return;
        }

        if (found.equals(sstInfo.getPointers().getLast())) {
            found = sstInfo.getPointers().get(sstInfo.getPointers().size() - 2);
            System.out.println("adjusting for last pointer");
        }

        System.out.println("Found: pointerList="+ sstInfo.getPointers().size()+" position going to look is=" + found.position());
        var reader = new MMappedReader(sstInfo.getSst());
        reader.position((int) found.position());
        Cluster cluster = IndexedCluster.readSimpleCluster(reader, found, sstInfo.getNumberOfKeysInSingleCluster());
        for (KVUnit unit : cluster.getUnits()) {
            if (Arrays.compare(key, unit.getKey()) == 0) {
                System.out.println("Key found");
                return;
            }
        }
        System.out.println("Key not found");

    }

    private static KVUnit linearSearch(SSTInfo sstInfo, byte[] key) throws IOException {
        System.out.println("Performing linear search");
        System.out.println("sstInfo="+sstInfo.getSst().getName());

        var reader = new MMappedReader(sstInfo.getSst());
        var clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        var numberOfKeysInSingleCluster = sstInfo.getNumberOfKeysInSingleCluster();
        int numberOfRetrievedClusterCount = 0;

        List<Cluster> clusterList = new ArrayList<>();
        while (!(reader.position() == clusterEndPoint || numberOfRetrievedClusterCount == sstInfo.getPointers().size())) {
            Cluster cluster = IndexedCluster.readSimpleCluster(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++), numberOfKeysInSingleCluster);
            clusterList.add(cluster);
        }

        for (Cluster cluster : clusterList) {
            for (KVUnit unit : cluster.getUnits()) {
                if (Arrays.compare(key, unit.getKey()) == 0) {
                    System.out.println("Linear search: Key found");
                    return unit;
                }
            }
        }
        System.out.println("Linear search: Key not found");
        return null;
    }

    public static void validateSST(SSTInfo sstInfo) throws IOException {
        System.out.println("TEST".repeat(30));
        System.out.println("sstInfo="+sstInfo.getSst().getName());
        System.out.println("Number of entries="+sstInfo.getNumberOfEntries());

        if (sstInfo.getLevel() != Level.LEVEL_ZERO) {
            if (sstInfo.getNumberOfEntries() < 100) {
                throw new RuntimeException("Number of entries is too little");
            }
        }

        verifySSTInfo(sstInfo);

        var reader = new MMappedReader(sstInfo.getSst());
        var clusterEndPoint = (int) Math.abs(sstInfo.getPointers().get(sstInfo.getPointers().size() - 1).position());
        var numberOfKeysInSingleCluster = sstInfo.getNumberOfKeysInSingleCluster();
        int numberOfRetrievedClusterCount = 0;

        System.out.println("Cluster reading");
        System.out.println("clusterEndPoint="+clusterEndPoint);
        System.out.println("numberOfKeysInSingleCluster="+numberOfKeysInSingleCluster);
        System.out.println("number of pointers in sst="+sstInfo.getPointers().size());
        System.out.println("start position="+reader.position());

        List<Cluster> clusterList = new ArrayList<>();
        while (!(reader.position() == clusterEndPoint || numberOfRetrievedClusterCount == sstInfo.getPointers().size())) {
            Cluster cluster = IndexedCluster.readSimpleCluster(reader, sstInfo.getPointers().get(numberOfRetrievedClusterCount++), numberOfKeysInSingleCluster);
            clusterList.add(cluster);
        }

//        var inputString = "qwertyuiopasdfghjklzxcvbnm<>?:}{+_)(*&^%$#@!)}1234567890`~".repeat(5).getBytes();
//        byte[] preKey = new byte[] {0};
//        System.out.println("Cluster iteration");
//        System.out.println("Number of cluster retrieved="+clusterList.size());
//        for (Cluster cluster : clusterList) {
//            for (KVUnit unit : cluster.getUnits()) {
//                if (Arrays.compare(preKey, unit.getKey()) > 0) {
//                    System.out.println("Cluster is invalid");
//                    throw new RuntimeException("Cluster is invalid");
//                }
//                preKey = unit.getKey();
//
//                if (Arrays.compare(inputString, unit.getValue()) != 0) {
//                    System.out.println("Cluster is invalid");
//                    System.out.println("Expected: " + new String(inputString) + "\nbut got: " + new String(unit.getValue()));
//                    throw new RuntimeException("Cluster is invalid");
//                }
//                System.out.print("Key="+new String(unit.getKey())+" ");
//            }
//        }
//        System.out.println("");

        if (Arrays.compare(clusterList.getFirst().getUnits().getFirst().getKey(), sstInfo.getSstKeyRange().getSmallest()) != 0) {
            throw new RuntimeException("Smallest key is invalid");
        }

        if (Arrays.compare(clusterList.getLast().getUnits().getLast().getKey(), sstInfo.getSstKeyRange().getGreatest()) != 0) {
            throw new RuntimeException("Greatest key is invalid");
        }

        System.out.println("\nDone with the validation");
    }

    private static void verifySSTInfo(SSTInfo sstInfo) {
        SSTInfo retrievedSSTInfo = SSTFileHelper.getSSTInfo(sstInfo.getSst());
        if (retrievedSSTInfo.equals(sstInfo)) {
            System.out.println("SSTInfo is valid");
        } else {
            System.out.println("SSTInfo is invalid");
        }

        if (Arrays.compare(retrievedSSTInfo.getSstKeyRange().getSmallest(), sstInfo.getSstKeyRange().getSmallest()) == 0) {
            System.out.println("getSmallest is valid");
        } else {
            System.out.println("getSmallest is invalid");
        }

        if (Arrays.compare(retrievedSSTInfo.getSstKeyRange().getGreatest(), sstInfo.getSstKeyRange().getGreatest()) == 0) {
            System.out.println("getSmallest is valid");
        } else {
            System.out.println("getSmallest is invalid");
        }

        if (Arrays.compare(retrievedSSTInfo.getSstKeyRange().getSmallest(), retrievedSSTInfo.getSstKeyRange().getGreatest()) < 0) {
            System.out.println("getSmallest is valid");
        } else {
            System.out.println("getSmallest is invalid");
        }

        if (retrievedSSTInfo.getNumberOfEntries() == sstInfo.getNumberOfEntries()) {
            System.out.println("getNumberOfEntries is valid");
        } else {
            System.out.println("getNumberOfEntries is invalid");
        }

        if (retrievedSSTInfo.getLevel().equals(sstInfo.getLevel())) {
            System.out.println("getLevel is valid");
        } else {
            System.out.println("getLevel is invalid");
        }

        if (retrievedSSTInfo.getFilterPosition() == sstInfo.getFilterPosition()) {
            System.out.println("getFilterPosition is valid");
        } else {
            System.out.println("getFilterPosition is invalid");
        }

        if (retrievedSSTInfo.getPointersPosition() == sstInfo.getPointersPosition()) {
            System.out.println("getPointersPosition is valid");
        } else {
            System.out.println("getPointersPosition is invalid");
        }

        if (retrievedSSTInfo.getFileTorsoSize() == sstInfo.getFileTorsoSize()) {
            System.out.println("getFileTorsoSize is valid");
        } else {
            System.out.println("getFileTorsoSize is invalid");
        }

        if (retrievedSSTInfo.getNumberOfKeysInSingleCluster() == sstInfo.getNumberOfKeysInSingleCluster()) {
            System.out.println("getNumberOfKeysInSingleCluster is valid");
        } else {
            System.out.println("getNumberOfKeysInSingleCluster is invalid");
        }

        if (retrievedSSTInfo.getShortestCommonPrefixUsed() == sstInfo.getShortestCommonPrefixUsed()) {
            System.out.println("getShortestCommonPrefixUsed is valid");
        } else {
            System.out.println("getShortestCommonPrefixUsed is invalid");
        }

        int count = retrievedSSTInfo.getPointers().getList().size();
        for (int i = 0; i < count; i++) {
            Pointer retrievedPointer = retrievedSSTInfo.getPointers().get(i);
            Pointer gotPointer = retrievedSSTInfo.getPointers().get(i);
            if (Arrays.compare(retrievedPointer.key(), gotPointer.key()) != 0 || retrievedPointer.position() != gotPointer.position()) {
                System.out.println("Pointer " + i + " is invalid");
                System.out.println("Expected: " + sstInfo.getPointers().get(i) + " but got: " + retrievedSSTInfo.getPointers().get(i));
                throw new RuntimeException("Pointer is invalid");
            }
        }
    }
}
