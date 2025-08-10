package filecache

/*

The following modalities are supported:
  1. provide exactly one massif file and no seal file
  2. provide exactly one one seal file and no massif file
  3. single log, single massif directory, one explicit seal file (the latest checkpoint)
  3. single log, many massif directories, one explicit seal file (the latest checkpoint)
  4. multiple logs, one log per massif directory, one explicit seal file per massif directory


  type LogId []bytes

  storageLogId(storagePath string) -> LogId
  logId(storagePath string) -> LogId
*/
