part of mongo_dart;

class LastError {
  static const String mapOk = 'ok';
  static const String mapError = 'err';
  static const String mapErrMsg = 'errmsg';
  static const String mapCode = 'code';
  static const String mapConnectionId = 'connectionId';
  static const String mapLastOp = 'lastOp';
  static const String mapNumber = 'n';
  static const String mapSyncMillis = 'syncMillis';
  static const String mapShards = 'shards';
  static const String mapSingleShard = 'singleShard';
  static const String mapUpdateExisting = 'updateExisting';
  static const String mapUpserted = 'upserted';
  static const String mapWNote = 'wnote';
  static const String mapWTimeout = 'wtimeout';
  static const String mapWaited = 'waited';
  static const String mapWTime = 'wtime';
  static const String mapWrittenTo = 'writtenTo';

  /// ok is true when the getLastError command completes successfully.
  ///    Note : A value of true does not indicate that the preceding
  ///           operation did not produce an error.
  bool _ok = false;
  bool get ok => _ok;

  /// err is null unless an error occurs. When there was an error with the
  /// preceding operation, err contains a string identifying the error.
  String _err;
  String get err => _err;
  bool get isError => _err != null;

  /// New in version 2.6.
  /// errmsg contains the description of the error. errmsg only appears
  /// if there was an error with the preceding operation.
  String _errmsg;
  String get errmsg => _errmsg;

  /// code reports the preceding operation’s error code.
  /// For description of the error, see err and errmsg.
  dynamic _code;
  dynamic get code => _code;

  /// The identifier of the connection.
  int _connectionId;
  int get connectionId => _connectionId;

  /// When issued against a replica set member and the preceding operation was
  /// a write or update, lastOp is the op-time timestamp in the op-log
  /// of the change.
  dynamic _lastOp;
  dynamic get lastOp => _lastOp;

  ///     If the preceding operation was an update or a remove operation,
  ///     but not a findAndModify operation, n reports the number of documents
  ///     matched by the update or remove operation.
  ///     For a remove operation, the number of matched documents will equal
  ///     the number removed.
  ///     For an update operation, if the operation results in no change to
  ///     the document, such as setting the value of the field to its current
  ///     value, the number of matched documents may be smaller than the number
  ///     of documents actually modified. If the update includes the
  ///     upsert:true option and results in the creation of a new document,
  ///     n returns the number of documents inserted.
  ///     n is 0 if reporting on an update or remove that occurs through a
  ///     findAndModify operation.
  int _n = 0;
  int get n => _n;
  bool get somethingWentWrongOnUpdateOrDelete => !isError && _n == 0;

  /// syncMillis is the number of milliseconds spent waiting for the write
  /// to disk operation (e.g. write to journal files).
  int _syncMillis;
  int get syncMillis => _syncMillis;

  /// When issued against a sharded cluster after a write operation,
  /// shards identifies the shards targeted in the write operation.
  /// shards is present in the output only if the write operation
  /// targets multiple shards.
  dynamic _shards;
  dynamic get shards => _shards;

  /// When issued against a sharded cluster after a write operation,
  /// identifies the shard targeted in the write operation.
  /// singleShard is only present if the write operation
  /// targets exactly one shard.
  dynamic _singleShard;
  dynamic get singleShard => _singleShard;

  /// updatedExisting is true when an update affects at least one document
  /// and does not result in an upsert.
  bool _updateExisting;
  bool get updateExisting => _updateExisting;

  /// If the update results in an insert, upserted is the value of _id field
  /// of the document.
  /// Changed in version 2.6: Earlier versions of MongoDB included upserted
  /// only if _id was an ObjectId.
  dynamic _upserted;
  dynamic get upserted => _upserted;

  /// If set, wnote indicates that the preceding operation’s error relates
  /// to using the w parameter to getLastError.
  dynamic _wnote;
  dynamic get wnote => _wnote;

  /// wtimeout is true if the getLastError timed out because of the wtimeout
  /// setting to getLastError.
  bool _wtimeout;
  bool get wtimeout => _wtimeout;

  /// If the preceding operation specified a timeout using the wtimeout
  /// setting to getLastError, then waited reports the number of milliseconds
  /// getLastError waited before timing out.
  int _waited;
  int get waited => _waited;

  /// getLastError.wtime is the number of milliseconds spent waiting for
  /// the preceding operation to complete. If getLastError timed out,
  /// wtime and getLastError.waited are equal.
  int _wtime;
  int get wtime => _wtime;

  /// If writing to a replica set, writtenTo is an array that contains
  /// the hostname and port number of the members that confirmed the previous
  /// write operation, based on the value of the w field in the command.
  List _writtenTo;
  List get writtenTo => _writtenTo;

  LastError();

  LastError.fromMap(Map lastErrorMap) {
    lastErrorMap ?? {};
    _ok = (lastErrorMap[mapOk] ?? 0.0) == 1.0;
    _err = lastErrorMap[mapError] as String;
    _errmsg = lastErrorMap[mapErrMsg] as String;
    _code = lastErrorMap[mapCode];
    _connectionId = lastErrorMap[mapConnectionId] as int;
    _lastOp = lastErrorMap[mapLastOp];
    _n = lastErrorMap[mapNumber] as int;
    _syncMillis = lastErrorMap[mapSyncMillis] as int;
    _shards = lastErrorMap[mapShards];
    _singleShard = lastErrorMap[mapSingleShard];
    _updateExisting = lastErrorMap[mapUpdateExisting] as bool;
    _upserted = lastErrorMap[mapUpserted];
    _wnote = lastErrorMap[mapWNote];
    _wtimeout = lastErrorMap[mapWTimeout] as bool;
    _waited = lastErrorMap[mapWaited] as int;
    _wtime = lastErrorMap[mapWTime] as int;
    _writtenTo = lastErrorMap[mapWrittenTo] as List;
  }

  void incrementN([int num]) => _n += num ?? 1;

  String encode() => json.encode(this);
  Map<String, dynamic> toJson([bool excludeNullValues]) {
    return <String, dynamic>{
      'ok': _ok,
      'err': _err,
      'errmsg': _errmsg,
      'code': _code,
      'connectionId': _connectionId,
      'lastOp': _lastOp,
      'n': _n,
      'syncMillis': _syncMillis,
      'shards': _shards,
      'singleShard': _singleShard,
      'updateExisting': _updateExisting,
      'upserted': _upserted,
      'wnote': _wnote,
      'wtimeout': _wtimeout,
      '_waited': _waited,
      'wtime': _wtime,
      'writtenTo': _writtenTo
    };
  }

  Map<String, dynamic> decode(String jsonEncoded) =>
      json.decode(jsonEncoded) as Map<String, dynamic>;
  void fromJson(Map<String, dynamic> document) {
    _ok = document['ok'] as bool;
    _err = document['err'] as String;
    _errmsg = document['errmsg'] as String;
    _code = document['code'];
    _connectionId = document['connectionId'] as int;
    _lastOp = document['lastOp'];
    _n = document['n'] as int;
    _syncMillis = document['syncMillis'] as int;
    _shards = document['shards'];
    _singleShard = document['singleShard'];
    _updateExisting = document['updateExisting'] as bool;
    _upserted = document['upserted'];
    _wnote = document['wnote'];
    _wtimeout = document['wtimeout'] as bool;
    _waited = document['_waited'] as int;
    _wtime = document['wtime'] as int;
    _writtenTo = document['writtenTo'] as List;
  }

  ///Method revive
  ///
  /// recreates the object from a JSON.encoded string
  void revive(String jsonEncoded) {
    final Map<String, dynamic> inner = decode(jsonEncoded);
    fromJson(inner);
  }
}
