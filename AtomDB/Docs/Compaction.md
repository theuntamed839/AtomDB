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
