package org.example.search;

import org.example.search.*;

import javax.lang.model.type.ArrayType;
import java.nio.channels.FileChannel;
import java.util.*;

public class SearchAlgos {
    static enum  ArrayTypes {
        LARGE_UNIFORM_ARRAY,
        LARGE_NON_UNIFORM_ARRAY,
        SMALL_UNIFORM_ARRAY,
        SMALL_NON_UNIFORM_ARRAY
    }
    public static void main(String[] args) {
        int LARGE = 100000000; // 10 cr
        int SMALL = 100000; // 1 lac
        int WARM_UP_ITERATIONS = 10;
        int[] largeUniformArr = sortedUniformArr(LARGE);
        int[] largeNonUniformArr = sortedNonUniformArr(LARGE);
        int[] smallUniformArr = sortedUniformArr(SMALL);
        int[] smallNonUniformArr = sortedNonUniformArr(SMALL);

        Search[] searches = new Search[] {
                new BranchLessBinarySearch(),
                //new ExponentialSearch(),
                new ExponentialSearchChatGPT(),
                //new InterpolationSearchBard(),
                //new InterpolationSearch(),
                new JavaSearch(),
                new TernarySearch(),
                new AtomDBSearch()
        };

        System.out.println("WARM UP");
        int[] largeUniformTargets = getTargets(largeUniformArr, WARM_UP_ITERATIONS);
        int[] largeNonUniformTargets = getTargets(largeNonUniformArr, WARM_UP_ITERATIONS);
        int[] smallUniformTargets = getTargets(smallUniformArr, WARM_UP_ITERATIONS);
        int[] smallNonUniformTargets = getTargets(smallNonUniformArr, WARM_UP_ITERATIONS);

        for (Search search : searches) {
            System.out.println(search.getClass().getName());
            bench(search, largeUniformArr, largeUniformTargets, ArrayTypes.LARGE_UNIFORM_ARRAY);
            bench(search, largeNonUniformArr, largeNonUniformTargets, ArrayTypes.LARGE_NON_UNIFORM_ARRAY);
            bench(search, smallUniformArr, smallUniformTargets, ArrayTypes.SMALL_UNIFORM_ARRAY);
            bench(search, smallNonUniformArr, smallNonUniformTargets, ArrayTypes.SMALL_NON_UNIFORM_ARRAY);
        }

        System.out.println("Iteration 1");
        int[] largeUniformTargets1 = getTargets(largeUniformArr, WARM_UP_ITERATIONS);
        int[] largeNonUniformTargets1 = getTargets(largeNonUniformArr, WARM_UP_ITERATIONS);
        int[] smallUniformTargets1 = getTargets(smallUniformArr, WARM_UP_ITERATIONS);
        int[] smallNonUniformTargets1 = getTargets(smallNonUniformArr, WARM_UP_ITERATIONS);

        for (Search search : searches) {
            System.out.println(search.getClass().getName());
            bench(search, largeUniformArr, largeUniformTargets1, ArrayTypes.LARGE_UNIFORM_ARRAY);
            bench(search, largeNonUniformArr, largeNonUniformTargets1, ArrayTypes.LARGE_NON_UNIFORM_ARRAY);
            bench(search, smallUniformArr, smallUniformTargets1, ArrayTypes.SMALL_UNIFORM_ARRAY);
            bench(search, smallNonUniformArr, smallNonUniformTargets1, ArrayTypes.SMALL_NON_UNIFORM_ARRAY);
        }

        System.out.println("Iteration 2");
        int[] largeUniformTargets12 = getTargets(largeUniformArr, WARM_UP_ITERATIONS);
        int[] largeNonUniformTargets12 = getTargets(largeNonUniformArr, WARM_UP_ITERATIONS);
        int[] smallUniformTargets12 = getTargets(smallUniformArr, WARM_UP_ITERATIONS);
        int[] smallNonUniformTargets12 = getTargets(smallNonUniformArr, WARM_UP_ITERATIONS);

        for (Search search : searches) {
            System.out.println(search.getClass().getName());
            bench(search, largeUniformArr, largeUniformTargets12, ArrayTypes.LARGE_UNIFORM_ARRAY);
            bench(search, largeNonUniformArr, largeNonUniformTargets12, ArrayTypes.LARGE_NON_UNIFORM_ARRAY);
            bench(search, smallUniformArr, smallUniformTargets12, ArrayTypes.SMALL_UNIFORM_ARRAY);
            bench(search, smallNonUniformArr, smallNonUniformTargets12, ArrayTypes.SMALL_NON_UNIFORM_ARRAY);
        }
    }


    private static void bench(Search search, int[] arr, int[] targets, ArrayTypes arrayType) {
        long start, end;
        start = System.nanoTime();
        for (int target : targets) {
            int index = search.search(arr, target);
            if(index < 0 || arr[index] != target) {
                System.out.println("**************************************");
                System.out.println("Guilty:" + search.getClass().getName());
                System.out.println("**************************************");
            }
        }
        end = System.nanoTime();
        System.out.println(arrayType + ": " + ((end-start)/1000_000.0) + "Milli");
    }

    private static int[] getTargets(int[] arr, int iterations) {
        int[] targets = new int[iterations];
        var random = new Random();
        for (int i = 0; i < targets.length; i++) {
            targets[i] = arr[random.nextInt(arr.length)];
        }
        return targets;
    }

    private static int[] sortedNonUniformArr(int size) {
        int[] arr = new int[size];
        var rand = new Random();
        for (int i = 0; i < arr.length; i++) {
            arr[i] = rand.nextInt();
        }
        Arrays.sort(arr);
        return arr;
    }

    private static int[] sortedUniformArr(int size) {
        int[] arr = new int[size];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = i;
        }
        return arr;
    }
}
