part of mongo_dart;

abstract class AbstractWriteResult {
  int nMatched = 0;
  int nInserted = 0;
  int nUpserted = 0;
  int nModified = 0;
  int nRemoved = 0;

  int get totalInsert => nInserted + nUpserted;
  bool get querySucceeded => nMatched > 0;
  bool checkQueryExpectation(int expectedMatches) =>
      expectedMatches != nMatched;

  bool hasWriteError();
  bool hasWriteConcernError();
  bool get isSuccess => querySucceeded;
}
