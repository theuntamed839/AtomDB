package io.github.theuntamed839;

import io.github.theuntamed839.dbs.BenchmarkDBAdapter;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static io.github.theuntamed839.Util.fillDB;

/**
 * --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED
 */
public class Benchmark {
    public static final long SEED = 1234567890L; // 1234

    public static void main(String[] args) throws Exception {
//        benchmarkWithRandomKVBytes(DBProvider.get(DB.LEVELDB),60_000_00, 500, 50);
        benchmarkWithRandomKVBytesWithNoInMemoryHold(
                DB.DATASTORE4J.getAdapter(), 20_000_00, 500, 500);

//        idk(DB.DATASTORE4J, 2000000, 500, 50);
    }

    private static void idk(DB adaptor, int entryCount, int keySize, int valueSize) throws Exception {
        BenchmarkDBAdapter db = adaptor.getAdapter();
        List<byte[]> keys = fillDB(db, entryCount, keySize, valueSize, SEED);
        long start = System.nanoTime();
        for (byte[] key : keys) {
            db.get(key);
        }
        long end = System.nanoTime();
        System.out.println("Total time for reading "+entryCount+" entries: "+(end-start)/1_000_000_000.0+" seconds");

        db.closeAndDestroy();
        db = adaptor.getAdapter();
        keys = fillDB(db, entryCount, keySize, valueSize, SEED);

        start = System.nanoTime();
        for (byte[] key : keys) {
            db.get(key);
        }
        end = System.nanoTime();
        System.out.println("Total time for reading "+entryCount+" entries: "+(end-start)/1_000_000_000.0+" seconds");

        db.closeAndDestroy();
        db = adaptor.getAdapter();
        keys = fillDB(db, entryCount, keySize, valueSize, SEED);

        start = System.nanoTime();
        for (byte[] key : keys) {
            db.get(key);
        }
        end = System.nanoTime();
        System.out.println("Total time for reading "+entryCount+" entries: "+(end-start)/1_000_000_000.0+" seconds");

        db.closeAndDestroy();
    }


    private static void benchmarkWithRandomKVBytes(BenchmarkDBAdapter db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        var map = getRandomKV(totalEntryCount, () -> keyBytesLength, () -> valueBytesLength);

        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            AtomicInteger i = new AtomicInteger();
            map.forEach((key, value) -> {
                try {
                    if (i.get() % 10000 == 0) {
                        System.out.println("progress="+i);
                    }
                    i.getAndIncrement();
                    db.put(key, value);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            endTime = System.nanoTime();

            writingTime = endTime - startTime;

            var list = new ArrayList<>(map.keySet());
            Collections.shuffle(list);
            System.out.println("Reading... ");
            startTime = System.nanoTime();
            list.forEach(each -> {
                try {
                    db.get(each);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised="+actualMemUsed);
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.closeAndDestroy();
        }
    }

    private static Map<byte[], byte[]> getRandomKV(int totalEntryCount, Supplier<Integer> keyBytesLength, Supplier<Integer> valueBytesLength) {
        // total entries
        System.out.println("random generation");
        var rand = new Random(1234);
        Map<byte[], byte[]> map = new HashMap<>(totalEntryCount);
        for (int i = 0; i < totalEntryCount; i++) {
            var key = new byte[rand.nextInt(10, keyBytesLength.get())];
            var value = new byte[rand.nextInt(10, valueBytesLength.get())];
            rand.nextBytes(key); rand.nextBytes(value);
            map.put(key, value);
        }
        // end
        return map;
    }

    private static void benchmarkWithRandomKVBytesWithNoInMemoryHold(BenchmarkDBAdapter db, int totalEntryCount, int keyBytesLength, int valueBytesLength) throws Exception {
        int interval = 10000;
        var rand = new Random(SEED);
        var randomSeeds = new ArrayList<Long>();
        for (int i = 0; i < totalEntryCount/interval; i++) {
            randomSeeds.add(rand.nextLong());
        }
        System.out.println("Number of threads: " + Thread.activeCount());
        long beforeUsedMem = Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
        long startTime , endTime, readingTime, writingTime;
        try {
            System.out.println("Writing... " + totalEntryCount);
            startTime = System.nanoTime();
            int countDown = randomSeeds.size();
            for (Long randomSeed : randomSeeds) {
                System.out.println("progress=" + (countDown--));
                rand = new Random(randomSeed);
                for (int i = 0; i < interval; i++) {
                    var key = new byte[keyBytesLength];
                    var value = new byte[valueBytesLength];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    db.put(key, value);
                }
            }
            endTime = System.nanoTime();

            writingTime = endTime - startTime;

            Collections.shuffle(randomSeeds);

            System.out.println("Reading... ");
//            Scanner scan = new Scanner(System.in);
//            scan.nextLine();
            startTime = System.nanoTime();
            countDown = randomSeeds.size();
            for (Long randomSeed : randomSeeds) {
                System.out.println("progress=" + (countDown--));
                rand = new Random(randomSeed);
                for (int i = 0; i < interval; i++) {
                    var key = new byte[keyBytesLength];
                    var value = new byte[valueBytesLength];
                    rand.nextBytes(key);
                    rand.nextBytes(value);
                    if (Arrays.compare(value, db.get(key)) != 0) {
                        throw new RuntimeException("Value mismatch for key: " + Arrays.toString(key));
                    }
                }
            }
            endTime = System.nanoTime();

            readingTime = endTime - startTime;
            System.out.println("writing time=" + writingTime/1000_000_000.0 + " , reading time=" + readingTime/1000_000_000.0);
            long afterUsedMem=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
            long actualMemUsed=afterUsedMem-beforeUsedMem;
            System.out.println("memory utilised In mb="+(actualMemUsed)/(1024 * 1024));
            System.out.println("Number of threads: " + Thread.activeCount());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.closeAndDestroy();
        }
    }
}

/**
 * AtomDBAdaptor
*         benchmarkWithRandomKVBytes(DBProvider.get(DB.ATOMDB),20_000_00, 500, 50);
 * writing time=19.9684539 , reading time=19.3050245
 * writing time=22.0252559 , reading time=15.4229418
 *
 * LevelDB 60000000
 * writing time=600.5900685 , reading time=346.6775644
 * memory utilised=1005701160
 * Number of threads: 4
 *
 * writing time=114.535569 , reading time=1446.8922146
 * memory utilised In mb=1382
 * Number of threads: 12
 * numberOfFilesRequiredToSearch=1 numberOfTimesThisHappened=5488959
 * numberOfFilesRequiredToSearch=2 numberOfTimesThisHappened=469131
 * numberOfFilesRequiredToSearch=3 numberOfTimesThisHappened=20428
 * numberOfFilesRequiredToSearch=4 numberOfTimesThisHappened=637
 * numberOfFilesRequiredToSearch=5 numberOfTimesThisHappened=18
 * numberOfFilesRequiredToSearch=6 numberOfTimesThisHappened=2
 * numberOfFilesRequiredToSearch=7 numberOfTimesThisHappened=1
 * total searches 6000000
 * totalValuesFromCache 20824
 * successfulSearchCount 6000000
 * unsuccessfulSearchCount 0
 * Number of actually compactions: 154
 [INFO] --- maven-jar-plugin:2.4:jar (default-jar) @ DataStore4J ---
 [INFO] Building jar: C:\WorkRelated\AtomDB\AtomDB\DataStore4J\target\DataStore4J-0.1.0.jar
 [INFO]
 [INFO] --- maven-javadoc-plugin:2.9.1:jar (attach-javadocs) @ DataStore4J ---
 [WARNING] Error injecting: org.apache.maven.plugin.javadoc.JavadocJar
 java.lang.ExceptionInInitializerError
 at org.apache.maven.plugin.javadoc.AbstractJavadocMojo.<clinit> (AbstractJavadocMojo.java:190)
 at jdk.internal.misc.Unsafe.ensureClassInitialized0 (Native Method)
 at jdk.internal.misc.Unsafe.ensureClassInitialized (Unsafe.java:1160)
 at jdk.internal.reflect.MethodHandleAccessorFactory.ensureClassInitialized (MethodHandleAccessorFactory.java:340)
 at jdk.internal.reflect.MethodHandleAccessorFactory.newConstructorAccessor (MethodHandleAccessorFactory.java:103)
 at jdk.internal.reflect.ReflectionFactory.newConstructorAccessor (ReflectionFactory.java:173)
 at java.lang.reflect.Constructor.acquireConstructorAccessor (Constructor.java:549)
 at java.lang.reflect.Constructor.newInstanceWithCaller (Constructor.java:499)
 at java.lang.reflect.Constructor.newInstance (Constructor.java:486)
 at com.google.inject.internal.DefaultConstructionProxyFactory$ReflectiveProxy.newInstance (DefaultConstructionProxyFactory.java:126)
 at com.google.inject.internal.ConstructorInjector.provision (ConstructorInjector.java:114)
 at com.google.inject.internal.ConstructorInjector.access$000 (ConstructorInjector.java:32)
 at com.google.inject.internal.ConstructorInjector$1.call (ConstructorInjector.java:98)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:112)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:127)
 at com.google.inject.internal.ProvisionListenerStackCallback.provision (ProvisionListenerStackCallback.java:66)
 at com.google.inject.internal.ConstructorInjector.construct (ConstructorInjector.java:93)
 at com.google.inject.internal.ConstructorBindingImpl$Factory.get (ConstructorBindingImpl.java:306)
 at com.google.inject.internal.InjectorImpl$1.get (InjectorImpl.java:1050)
 at com.google.inject.internal.InjectorImpl.getInstance (InjectorImpl.java:1086)
 at org.eclipse.sisu.space.AbstractDeferredClass.get (AbstractDeferredClass.java:48)
 at com.google.inject.internal.ProviderInternalFactory.provision (ProviderInternalFactory.java:85)
 at com.google.inject.internal.InternalFactoryToInitializableAdapter.provision (InternalFactoryToInitializableAdapter.java:57)
 at com.google.inject.internal.ProviderInternalFactory$1.call (ProviderInternalFactory.java:66)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:112)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:127)
 at com.google.inject.internal.ProvisionListenerStackCallback.provision (ProvisionListenerStackCallback.java:66)
 at com.google.inject.internal.ProviderInternalFactory.circularGet (ProviderInternalFactory.java:61)
 at com.google.inject.internal.InternalFactoryToInitializableAdapter.get (InternalFactoryToInitializableAdapter.java:47)
 at com.google.inject.internal.InjectorImpl$1.get (InjectorImpl.java:1050)
 at org.eclipse.sisu.inject.Guice4$1.get (Guice4.java:162)
 at org.eclipse.sisu.inject.LazyBeanEntry.getValue (LazyBeanEntry.java:81)
 at org.eclipse.sisu.plexus.LazyPlexusBean.getValue (LazyPlexusBean.java:51)
 at org.codehaus.plexus.DefaultPlexusContainer.lookup (DefaultPlexusContainer.java:263)
 at org.codehaus.plexus.DefaultPlexusContainer.lookup (DefaultPlexusContainer.java:255)
 at org.apache.maven.plugin.internal.DefaultMavenPluginManager.getConfiguredMojo (DefaultMavenPluginManager.java:520)
 at org.apache.maven.plugin.DefaultBuildPluginManager.executeMojo (DefaultBuildPluginManager.java:124)
 at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute2 (MojoExecutor.java:370)
 at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute (MojoExecutor.java:351)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:215)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:171)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:163)
 at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:117)
 at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:81)
 at org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder.build (SingleThreadedBuilder.java:56)
 at org.apache.maven.lifecycle.internal.LifecycleStarter.execute (LifecycleStarter.java:128)
 at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:298)
 at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:192)
 at org.apache.maven.DefaultMaven.execute (DefaultMaven.java:105)
 at org.apache.maven.cli.MavenCli.execute (MavenCli.java:960)
 at org.apache.maven.cli.MavenCli.doMain (MavenCli.java:293)
 at org.apache.maven.cli.MavenCli.main (MavenCli.java:196)
 at jdk.internal.reflect.DirectMethodHandleAccessor.invoke (DirectMethodHandleAccessor.java:103)
 at java.lang.reflect.Method.invoke (Method.java:580)
 at org.codehaus.plexus.classworlds.launcher.Launcher.launchEnhanced (Launcher.java:282)
 at org.codehaus.plexus.classworlds.launcher.Launcher.launch (Launcher.java:225)
 at org.codehaus.plexus.classworlds.launcher.Launcher.mainWithExitCode (Launcher.java:406)
 at org.codehaus.plexus.classworlds.launcher.Launcher.main (Launcher.java:347)
 Caused by: java.lang.StringIndexOutOfBoundsException: Range [0, 3) out of bounds for length 2
 at jdk.internal.util.Preconditions$1.apply (Preconditions.java:55)
 at jdk.internal.util.Preconditions$1.apply (Preconditions.java:52)
 at jdk.internal.util.Preconditions$4.apply (Preconditions.java:213)
 at jdk.internal.util.Preconditions$4.apply (Preconditions.java:210)
 at jdk.internal.util.Preconditions.outOfBounds (Preconditions.java:98)
 at jdk.internal.util.Preconditions.outOfBoundsCheckFromToIndex (Preconditions.java:112)
 at jdk.internal.util.Preconditions.checkFromToIndex (Preconditions.java:349)
 at java.lang.String.checkBoundsBeginEnd (String.java:4914)
 at java.lang.String.substring (String.java:2876)
 at org.apache.commons.lang.SystemUtils.getJavaVersionAsFloat (SystemUtils.java:1133)
 at org.apache.commons.lang.SystemUtils.<clinit> (SystemUtils.java:818)
 at org.apache.maven.plugin.javadoc.AbstractJavadocMojo.<clinit> (AbstractJavadocMojo.java:190)
 at jdk.internal.misc.Unsafe.ensureClassInitialized0 (Native Method)
 at jdk.internal.misc.Unsafe.ensureClassInitialized (Unsafe.java:1160)
 at jdk.internal.reflect.MethodHandleAccessorFactory.ensureClassInitialized (MethodHandleAccessorFactory.java:340)
 at jdk.internal.reflect.MethodHandleAccessorFactory.newConstructorAccessor (MethodHandleAccessorFactory.java:103)
 at jdk.internal.reflect.ReflectionFactory.newConstructorAccessor (ReflectionFactory.java:173)
 at java.lang.reflect.Constructor.acquireConstructorAccessor (Constructor.java:549)
 at java.lang.reflect.Constructor.newInstanceWithCaller (Constructor.java:499)
 at java.lang.reflect.Constructor.newInstance (Constructor.java:486)
 at com.google.inject.internal.DefaultConstructionProxyFactory$ReflectiveProxy.newInstance (DefaultConstructionProxyFactory.java:126)
 at com.google.inject.internal.ConstructorInjector.provision (ConstructorInjector.java:114)
 at com.google.inject.internal.ConstructorInjector.access$000 (ConstructorInjector.java:32)
 at com.google.inject.internal.ConstructorInjector$1.call (ConstructorInjector.java:98)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:112)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:127)
 at com.google.inject.internal.ProvisionListenerStackCallback.provision (ProvisionListenerStackCallback.java:66)
 at com.google.inject.internal.ConstructorInjector.construct (ConstructorInjector.java:93)
 at com.google.inject.internal.ConstructorBindingImpl$Factory.get (ConstructorBindingImpl.java:306)
 at com.google.inject.internal.InjectorImpl$1.get (InjectorImpl.java:1050)
 at com.google.inject.internal.InjectorImpl.getInstance (InjectorImpl.java:1086)
 at org.eclipse.sisu.space.AbstractDeferredClass.get (AbstractDeferredClass.java:48)
 at com.google.inject.internal.ProviderInternalFactory.provision (ProviderInternalFactory.java:85)
 at com.google.inject.internal.InternalFactoryToInitializableAdapter.provision (InternalFactoryToInitializableAdapter.java:57)
 at com.google.inject.internal.ProviderInternalFactory$1.call (ProviderInternalFactory.java:66)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:112)
 at com.google.inject.internal.ProvisionListenerStackCallback$Provision.provision (ProvisionListenerStackCallback.java:127)
 at com.google.inject.internal.ProvisionListenerStackCallback.provision (ProvisionListenerStackCallback.java:66)
 at com.google.inject.internal.ProviderInternalFactory.circularGet (ProviderInternalFactory.java:61)
 at com.google.inject.internal.InternalFactoryToInitializableAdapter.get (InternalFactoryToInitializableAdapter.java:47)
 at com.google.inject.internal.InjectorImpl$1.get (InjectorImpl.java:1050)
 at org.eclipse.sisu.inject.Guice4$1.get (Guice4.java:162)
 at org.eclipse.sisu.inject.LazyBeanEntry.getValue (LazyBeanEntry.java:81)
 at org.eclipse.sisu.plexus.LazyPlexusBean.getValue (LazyPlexusBean.java:51)
 at org.codehaus.plexus.DefaultPlexusContainer.lookup (DefaultPlexusContainer.java:263)
 at org.codehaus.plexus.DefaultPlexusContainer.lookup (DefaultPlexusContainer.java:255)
 at org.apache.maven.plugin.internal.DefaultMavenPluginManager.getConfiguredMojo (DefaultMavenPluginManager.java:520)
 at org.apache.maven.plugin.DefaultBuildPluginManager.executeMojo (DefaultBuildPluginManager.java:124)
 at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute2 (MojoExecutor.java:370)
 at org.apache.maven.lifecycle.internal.MojoExecutor.doExecute (MojoExecutor.java:351)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:215)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:171)
 at org.apache.maven.lifecycle.internal.MojoExecutor.execute (MojoExecutor.java:163)
 at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:117)
 at org.apache.maven.lifecycle.internal.LifecycleModuleBuilder.buildProject (LifecycleModuleBuilder.java:81)
 at org.apache.maven.lifecycle.internal.builder.singlethreaded.SingleThreadedBuilder.build (SingleThreadedBuilder.java:56)
 at org.apache.maven.lifecycle.internal.LifecycleStarter.execute (LifecycleStarter.java:128)
 at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:298)
 at org.apache.maven.DefaultMaven.doExecute (DefaultMaven.java:192)
 at org.apache.maven.DefaultMaven.execute (DefaultMaven.java:105)
 at org.apache.maven.cli.MavenCli.execute (MavenCli.java:960)
 at org.apache.maven.cli.MavenCli.doMain (MavenCli.java:293)
 at org.apache.maven.cli.MavenCli.main (MavenCli.java:196)
 at jdk.internal.reflect.DirectMethodHandleAccessor.invoke (DirectMethodHandleAccessor.java:103)
 at java.lang.reflect.Method.invoke (Method.java:580)
 at org.codehaus.plexus.classworlds.launcher.Launcher.launchEnhanced (Launcher.java:282)
 at org.codehaus.plexus.classworlds.launcher.Launcher.launch (Launcher.java:225)
 at org.codehaus.plexus.classworlds.launcher.Launcher.mainWithExitCode (Launcher.java:406)
 at org.codehaus.plexus.classworlds.launcher.Launcher.main (Launcher.java:347)
 [INFO] ------------------------------------------------------------------------
 [INFO] BUILD FAILURE
 [INFO] ------------------------------------------------------------------------
 [INFO] Total time:  05:58 min
 [INFO] Finished at: 2025-09-01T02:58:18+05:30
 [INFO] ------------------------------------------------------------------------
 [ERROR] Failed to execute goal org.apache.maven.plugins:maven-javadoc-plugin:2.9.1:jar (attach-javadocs) on project DataStore4J: Execution attach-javadocs of goal org.apache.maven.plugins:maven-javadoc-plugin:2.9.1:jar failed: An API incompatibility was encountered while executing org.apache.maven.plugins:maven-javadoc-plugin:2.9.1:jar: java.lang.ExceptionInInitializerError: null
 [ERROR] -----------------------------------------------------
 [ERROR] realm =    plugin>org.apache.maven.plugins:maven-javadoc-plugin:2.9.1
 [ERROR] strategy = org.codehaus.plexus.classworlds.strategy.SelfFirstStrategy
 [ERROR] urls[0] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/plugins/maven-javadoc-plugin/2.9.1/maven-javadoc-plugin-2.9.1.jar
 [ERROR] urls[1] = file:/C:/Users/GaneshNaik/.m2/repository/org/slf4j/slf4j-jdk14/1.5.6/slf4j-jdk14-1.5.6.jar
 [ERROR] urls[2] = file:/C:/Users/GaneshNaik/.m2/repository/org/slf4j/jcl-over-slf4j/1.5.6/jcl-over-slf4j-1.5.6.jar
 [ERROR] urls[3] = file:/C:/Users/GaneshNaik/.m2/repository/commons-cli/commons-cli/1.2/commons-cli-1.2.jar
 [ERROR] urls[4] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-interactivity-api/1.0-alpha-4/plexus-interactivity-api-1.0-alpha-4.jar
 [ERROR] urls[5] = file:/C:/Users/GaneshNaik/.m2/repository/org/sonatype/plexus/plexus-sec-dispatcher/1.3/plexus-sec-dispatcher-1.3.jar
 [ERROR] urls[6] = file:/C:/Users/GaneshNaik/.m2/repository/org/sonatype/plexus/plexus-cipher/1.4/plexus-cipher-1.4.jar
 [ERROR] urls[7] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-interpolation/1.11/plexus-interpolation-1.11.jar
 [ERROR] urls[8] = file:/C:/Users/GaneshNaik/.m2/repository/backport-util-concurrent/backport-util-concurrent/3.1/backport-util-concurrent-3.1.jar
 [ERROR] urls[9] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/reporting/maven-reporting-api/3.0/maven-reporting-api-3.0.jar
 [ERROR] urls[10] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/maven-archiver/2.5/maven-archiver-2.5.jar
 [ERROR] urls[11] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/shared/maven-invoker/2.0.9/maven-invoker-2.0.9.jar
 [ERROR] urls[12] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/shared/maven-common-artifact-filters/1.3/maven-common-artifact-filters-1.3.jar
 [ERROR] urls[13] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-sink-api/1.0/doxia-sink-api-1.0.jar
 [ERROR] urls[14] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-site-renderer/1.0/doxia-site-renderer-1.0.jar
 [ERROR] urls[15] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-core/1.0/doxia-core-1.0.jar
 [ERROR] urls[16] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-i18n/1.0-beta-7/plexus-i18n-1.0-beta-7.jar
 [ERROR] urls[17] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-velocity/1.1.7/plexus-velocity-1.1.7.jar
 [ERROR] urls[18] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/velocity/velocity/1.5/velocity-1.5.jar
 [ERROR] urls[19] = file:/C:/Users/GaneshNaik/.m2/repository/oro/oro/2.0.8/oro-2.0.8.jar
 [ERROR] urls[20] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-decoration-model/1.0/doxia-decoration-model-1.0.jar
 [ERROR] urls[21] = file:/C:/Users/GaneshNaik/.m2/repository/commons-collections/commons-collections/3.2/commons-collections-3.2.jar
 [ERROR] urls[22] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-module-apt/1.0/doxia-module-apt-1.0.jar
 [ERROR] urls[23] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-module-fml/1.0/doxia-module-fml-1.0.jar
 [ERROR] urls[24] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-module-xdoc/1.0/doxia-module-xdoc-1.0.jar
 [ERROR] urls[25] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/maven/doxia/doxia-module-xhtml/1.0/doxia-module-xhtml-1.0.jar
 [ERROR] urls[26] = file:/C:/Users/GaneshNaik/.m2/repository/commons-lang/commons-lang/2.4/commons-lang-2.4.jar
 [ERROR] urls[27] = file:/C:/Users/GaneshNaik/.m2/repository/commons-io/commons-io/2.2/commons-io-2.2.jar
 [ERROR] urls[28] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/httpcomponents/httpclient/4.2.3/httpclient-4.2.3.jar
 [ERROR] urls[29] = file:/C:/Users/GaneshNaik/.m2/repository/org/apache/httpcomponents/httpcore/4.2.2/httpcore-4.2.2.jar
 [ERROR] urls[30] = file:/C:/Users/GaneshNaik/.m2/repository/commons-codec/commons-codec/1.6/commons-codec-1.6.jar
 [ERROR] urls[31] = file:/C:/Users/GaneshNaik/.m2/repository/commons-logging/commons-logging/1.1.1/commons-logging-1.1.1.jar
 [ERROR] urls[32] = file:/C:/Users/GaneshNaik/.m2/repository/log4j/log4j/1.2.14/log4j-1.2.14.jar
 [ERROR] urls[33] = file:/C:/Users/GaneshNaik/.m2/repository/com/thoughtworks/qdox/qdox/1.12.1/qdox-1.12.1.jar
 [ERROR] urls[34] = file:/C:/Users/GaneshNaik/.m2/repository/junit/junit/3.8.1/junit-3.8.1.jar
 [ERROR] urls[35] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-archiver/2.1.2/plexus-archiver-2.1.2.jar
 [ERROR] urls[36] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-io/2.0.4/plexus-io-2.0.4.jar
 [ERROR] urls[37] = file:/C:/Users/GaneshNaik/.m2/repository/org/codehaus/plexus/plexus-utils/3.0.5/plexus-utils-3.0.5.jar
 [ERROR] Number of foreign imports: 1
 [ERROR] import: Entry[import  from realm ClassRealm[project>io.github.theuntamed839:DataStore4J:0.1.0, parent: ClassRealm[maven.api, parent: null]]]
 [ERROR]
 [ERROR] -----------------------------------------------------
 [ERROR] : Range [0, 3) out of bounds for length 2
 [ERROR] -> [Help 1]
 [ERROR]
 [ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
 [ERROR] Re-run Maven using the -X switch to enable full debug logging.
 [ERROR]
 [ERROR] For more information about the errors and possible solutions, please read the following articles:
 [ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/PluginContainerException
 PS C:\WorkRelated\AtomDB\AtomDB\DataStore4J>


 */;

