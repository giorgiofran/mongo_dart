part of mongo_dart;

class BulkWriteResult extends AbstractWriteResult {

  List<UpsertedInfo> upserted = [];
  List<BulkWriteError> writeErrors = [];
  WriteConcern writeConcernError;

  @override
  bool hasWriteError() => writeErrors.isNotEmpty;
  @override
  bool hasWriteConcernError() => writeConcernError != null;
  @override
  bool get isSuccess =>
      super.isSuccess && !hasWriteError() && !hasWriteConcernError();
}