The idea around compaction.

* It about creating overlapping free and compact sst.
  * Compact SST meaning is that we should have sst which holds all the keys which lies in the min and max key.
  * By making it compact we gonna reduce the overlap. so if there is a sst having keys 10-50 range and has all the elements in it. the for searching we only gonna access this sst.
  * if there is overlapping then we will have ssts which are like 10-70 and 30-70. here for search we will need to access 2 ssts.



Plan:
table, search engine, and benchmark.
benchmark should yield super fast results. 
then we optimize and clean our existing code.
then we work compaction by planning on first how things will move and then implementations.
then optimize and make the code clean.
benchmark and optimize.
write improved unit test, integration test, crash test, performance test under different loads.



LevelDB compaction:
1. 4mb file is level0
2. pick one file from level L and all overlapping files from level L+1.
3. While Level 0 -> 1, we take all the overlapping files from 0 and 1 as well. since this is very special level.
4. New create a new sst for every 2 mb file.
5. we also switch to a new sst when we have grown enough to cover 10 level+2 files. (so that we wont pickup more files from l+2 for next compaction)
6. we remember last key of level l so that next time we pick files from last key.


We haven't done:
deleting of old value and keeping the latest onces. also deleting.