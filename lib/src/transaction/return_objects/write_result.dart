part of mongo_dart;

class WriteResult extends AbstractWriteResult {
  dynamic id;
  WriteError writeError;
  WriteConcernError writeConcernError;

  WriteResult();

  WriteResult.fromMap(Map result) {
    nMatched = (result['n'] ?? 0) as int;
  }

  @override
  bool hasWriteError() => writeError != null;
  @override
  bool hasWriteConcernError() => writeConcernError != null;
  @override
  bool get isSuccess =>
      super.isSuccess && !hasWriteError() && !hasWriteConcernError();
}
