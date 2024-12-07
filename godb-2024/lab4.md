## Lab 4 Project

For my project, I modified GoDB to use a column-oriented physical layout. I used the “early materialization” design: making complete tuples while reading columns off disk and processing them with the existing query operators. Using a column-oriented physical layout helps speed up query processing in GoDB, and allows for data compression for further potential processing optimization.

To do this, I made a new DBFile interface that supports column-oriented files, making methods like insertTuple, deleteTuple, LoadFromCSV, flushPage, readPage, Iterator, etc. The iterator takes in a list of columns to read and output the tuples that have records from those columns, using BufferPool.GetPage. Similar to how there are heap page and heap file go files, I made one go file for column pages (column_store_page.go) and one go file for the column file (column_store_file.go). 

These are the test cases (column_store_test.go):
- Tests for column file methods: TestcolumnStoreFileCreateAndInsert, TestcolumnStoreFileDelete, TestcolumnStoreFilePageKey, TestcolumnStoreFileSize, TestcolumnStoreFileDirtyBit
- Tests for column page methods: TestColumnPageInsert, TestColumnPageDelete, TestColumnPageInsertTuple, TestColumnPageDeleteTuple, TestColumnPageDirty, TestColumnPageSerialization
- Tests for GoDB operations: TestIntFilterCol, TestStringFilterCol, TestJoinCol, TestProjectCol
- Performance tests: TestLoadCSVPerformance50, TestLoadCSVPerformance500, TestLoadCSVPerformance2000, TestLoadCSVPerformance10000. These evaluate the performance gain of using column store.
