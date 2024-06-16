The idea around compaction.

* It about creating overlapping free and compact sst.
  * Compact SST meaning is that we should have sst which holds all the keys which lies in the min and max key.
  * By making it compact we gonna reduce the overlap. so if there is a sst having keys 10-50 range and has all the elements in it. the for searching we only gonna access this sst.
  * if there is overlapping then we will have ssts which are like 10-70 and 30-70. here for search we will need to access 2 ssts.



Plan:
table, search engine, and benchmark.
benchmark should yield super fast results. 
then we optimize and clean our existing code.
then we work compaction by planning on smallest how things will move and then implementations.
then optimize and make the code clean.
benchmark and optimize.
write improved unit test, integration test, crash test, performance test under different loads.



LevelDB compaction:
1. 4mb file is level0
2. pick one file from level L and all overlapping files from level L+1.
3. While Level 0 -> 1, we take all the overlapping files from 0 and 1 as well. since this is very special level.
4. New create a new sst for every 2 mb file.
5. we also switch to a new sst when we have grown enough to cover 10 level+2 files. (so that we wont pickup more files from l+2 for next compaction)
6. we remember greatest key of level l so that next time we pick files from greatest key.


TODO:
1. Stable compaction
2. Value updating and delete.
3. Scheduling compaction in background thread.
4. Table recreation and manifest file.
5. Search improvement (Cache of blocks)
6. Improve overall code and clean up.
7. Unit test & integration test
8. Benchmark
9. Maven deploy
10. Great readme, explaining how to install, use, benchmarks, limitations, ideas, motivation and future work, Pictorial representation of architecture and sst.
11. Handling architecture shortcoming, for example the checksum check.

For updation, we can make the iteration in sorted order of latest -> old.
and in sstPersist we can use a set, with this we will only have a unique key and old values will be discarded.

https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
https://blog.senx.io/demystifying-leveldb/
https://stackoverflow.com/questions/61684116/compaction-causes-write-to-hang-until-finished
https://tonyz93.blogspot.com/2016/11/leveldb-source-reading-3-compaction.html
https://www.speedb.io/blog-posts/understanding-leveled-compaction
https://github.com/google/leveldb/blob/main/db/version_set.cc
https://www.google.com/search?q=what+is+a+weak+key+map+guava+java&sca_esv=be2d3384baa617c2&sca_upv=1&rlz=1C1CHBF_enIN1024IN1024&biw=1536&bih=695&sxsrf=ACQVn0_daYOv836fgUD-zntx6kJ9qE1WNg%3A1712689998040&ei=TpMVZtiVAsanseMPvvmVkA8&ved=0ahUKEwjY9MW367WFAxXGU2wGHb58BfIQ4dUDCBA&uact=5&oq=what+is+a+weak+key+map+guava+java&gs_lp=Egxnd3Mtd2l6LXNlcnAaAhgCIiF3aGF0IGlzIGEgd2VhayBrZXkgbWFwIGd1YXZhIGphdmEyBxAhGAoYoAEyBxAhGAoYoAEyBxAhGAoYoAEyBxAhGAoYoAFI9RxQhQRYoRtwBHgBkAEAmAH6AaABjxOqAQUwLjUuN7gBA8gBAPgBAZgCEKACqxPCAgoQABhHGNYEGLADwgIEECMYJ8ICChAhGAoYoAEYiwPCAgQQIRgVmAMAiAYBkAYIkgcFNC41LjegB-cu&sclient=gws-wiz-serp
https://stackoverflow.com/questions/48139062/behaviour-of-caffeine-cache-asmap-views
