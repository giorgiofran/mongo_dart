part of mongo_dart;

const String docId = '_id';

const lkNone = 0x00; // 0000 0000 0000 0000 0000 0000
const lkEnsureRead = 0x01; // 0000 0000 0000 0000 0000 0001
const lkEnsureChanges = 0x02; // 0000 0000 0000 0000 0000 0010
const lkPreventChanges = 0x04; // 0000 0000 0000 0000 0000 0100
const lkChangesLock = 0x08; // 0000 0000 0000 0000 0000 1000
const lkExclusiveLock = 0x10; // 0000 0000 0000 0000 0001 0000
const opReadOriginal = 0x0100; // 0000 0000 0000 ‭0001 0000 0000‬
const opRead = 0x0200; // 0000 0000 0000 0010 0000 0000‬
const opInsert = 0x0400; // 0000 0000 0000 ‭0100 0000 0000‬
const opUpdate = 0x0800; // 0000 0000 0000 1000 0000 0000‬
const opDelete = 0x1000; // 0000 0000 ‭0001 0000 0000 0000
const lkExistingEnsureRead = 0x010000; // 0000 ‭0001 0000 0000 0000 0000‬
const lkExistingEnsureChanges = 0x020000; // 0000 ‭0010 0000 0000 0000 0000‬
const lkExistingPreventChanges = 0x040000; // 0000 ‭0100 0000 0000 0000 0000‬
const lkExistingChangesLock = 0x080000; // 0000 1000 0000 0000 0000 0000
const lkExistingExclusiveLock = 0x100000; // ‭0001 0000 0000 0000 0000‬ 0000

const trStateOpen = 0;
const trStateAborting = 1;
const trStateClosed = 2;

const Map<int, bool> lockCompatibilityMap = <int, bool>{
  lkExistingEnsureRead | opReadOriginal: true,
  lkExistingEnsureRead | opRead: true,
  lkExistingEnsureRead | lkEnsureRead: true,
  lkExistingEnsureRead | lkEnsureChanges: true,
  lkExistingEnsureRead | lkPreventChanges: true,
  lkExistingEnsureRead | opInsert: true,
  lkExistingEnsureRead | opUpdate: true,
  lkExistingEnsureRead | opDelete: true,
  lkExistingEnsureRead | lkChangesLock: true,
  lkExistingEnsureRead | lkExclusiveLock: false,
  lkExistingEnsureChanges | opReadOriginal: true,
  lkExistingEnsureChanges | opRead: true,
  lkExistingEnsureChanges | lkEnsureRead: true,
  lkExistingEnsureChanges | lkEnsureChanges: true,
  lkExistingEnsureChanges | lkPreventChanges: false,
  lkExistingEnsureChanges | opInsert: true,
  lkExistingEnsureChanges | opUpdate: true,
  lkExistingEnsureChanges | opDelete: true,
  lkExistingEnsureChanges | lkChangesLock: false,
  lkExistingEnsureChanges | lkExclusiveLock: false,
  lkExistingPreventChanges | opReadOriginal: true,
  lkExistingPreventChanges | opRead: true,
  lkExistingPreventChanges | lkEnsureRead: true,
  lkExistingPreventChanges | lkEnsureChanges: false,
  lkExistingPreventChanges | lkPreventChanges: true,
  lkExistingPreventChanges | opInsert: false,
  lkExistingPreventChanges | opUpdate: false,
  lkExistingPreventChanges | opDelete: false,
  lkExistingPreventChanges | lkChangesLock: false,
  lkExistingPreventChanges | lkExclusiveLock: false,
  lkExistingChangesLock | opReadOriginal: true,
  lkExistingChangesLock | opRead: true,
  lkExistingChangesLock | lkEnsureRead: true,
  lkExistingChangesLock | lkEnsureChanges: false,
  lkExistingChangesLock | lkPreventChanges: false,
  lkExistingChangesLock | opInsert: false,
  lkExistingChangesLock | opUpdate: false,
  lkExistingChangesLock | opDelete: false,
  lkExistingChangesLock | lkChangesLock: false,
  lkExistingChangesLock | lkExclusiveLock: false,
  lkExistingExclusiveLock | opReadOriginal: true,
  lkExistingExclusiveLock | opRead: false,
  lkExistingExclusiveLock | lkEnsureRead: false,
  lkExistingExclusiveLock | lkEnsureChanges: false,
  lkExistingExclusiveLock | lkPreventChanges: false,
  lkExistingExclusiveLock | opInsert: false,
  lkExistingExclusiveLock | opUpdate: false,
  lkExistingExclusiveLock | opDelete: false,
  lkExistingExclusiveLock | lkChangesLock: false,
  lkExistingExclusiveLock | lkExclusiveLock: false,
};

class TtsDb extends Db {
  static const int trTypeLock = 0;
  static const int trTypeInsert = 1;
  static const int trTypeUpdateDocument = 2;
  static const int trTypeUpdateField = 3;
  static const int trTypeDelete = 4;

  static const int trLvCollection = 0;
  static const int trLvDocument = 1;
  static const int trLvFields = 2;

  static const int trIsolationCommittedDocuments = 1;
  static const int trIsolationAllDocuments = 2;

  static const String collectionHeaderName = 'tts_header';
  static const String headerStartDate = 'startDate';
  static const String headerEndDate = 'endDate';
  static const String headerState = 'state';
  static const String headerParentTransactions = 'parentTransactions';
  static const String headerChildrenList = 'childrenList';
  static const String headerTransactionIsolation = 'transactionIsolation';
  // This collection store all locks and document changes;
  static const String collectionTransactionsName = 'tts_transactions';
  // fields: id
  //         transactionID
  static const String transactionTransactionId = 'transactionId';
  //         documentID
  static const String transactionDocumentId = 'documentId';
  //         lockType (one of: lkEnsureRead, LkEnsureChanges, etc)
  static const String transactionLockType = 'lockType';
  //         collectionName
  static const String transactionCollectionName = 'collectionName';
  //         transactionType
  static const String transactionTransactionType = 'transactionType';
  //         transactionLevel
  static const String transactionTransactionLevel = 'transactionLevel';
  //         previousDocument
  static const String transactionPreviousDocument = 'previousDocument';
  //         newDocument
  static const String transactionNewDocument = 'newDocument';
  //         date
  static const String transactionDate = 'date';
  //         transaction index: a progressive number uniquely identifying
  //                             the transaction. Used for RollBacks.
  static const String transactionIndex = 'index';

  final Map<String, Map<String, dynamic>> _openTransactions =
      SplayTreeMap<String, Map<String, dynamic>>();
  Map<String, Map> get openTransactionsList => Map.from(_openTransactions);

  TtsCollection headerCollection;
  TtsCollection transactionCollection;

  TtsDb(String uriString, [String debugInfo]) : super(uriString, debugInfo) {
    headerCollection = collection(collectionHeaderName);
    transactionCollection = collection(collectionTransactionsName);
  }
  TtsDb.pool(List<String> uriList, [String debugInfo])
      : super.pool(uriList, debugInfo) {
    headerCollection = collection(collectionHeaderName);
    transactionCollection = collection(collectionTransactionsName);
  }
  TtsDb._authDb(String databaseName) : super._authDb(databaseName) {
    headerCollection = collection(collectionHeaderName);
    transactionCollection = collection(collectionTransactionsName);
  }

  @override
  TtsCollection collection(String collectionName) =>
      TtsCollection(this, collectionName);

  void _subscribeTransaction(Map<String, dynamic> transactionMap) =>
      _openTransactions[(transactionMap[docId] as ObjectId).toHexString()] =
          transactionMap;

  void _unsubscribeTransaction(ObjectId transactionId) =>
      _openTransactions.remove(transactionId.toHexString());

  void _registerChildTransaction(Map childTransactionMap) {
    assert(childTransactionMap != null,
        "null argument in call to _registerChildTransaction");
    final ObjectId parentTransactionId =
        (childTransactionMap[headerParentTransactions] as List).last
            as ObjectId;
    (_openTransactions[parentTransactionId.toHexString()][headerChildrenList]
            as List)
        .add(childTransactionMap[docId]);
  }

  void _unRegisterChildTransaction(Map childTransactionMap) {
    assert(childTransactionMap != null,
        "null argument in call to _unRegisterChildTransaction");
    final ObjectId parentTransactionId =
        (childTransactionMap[headerParentTransactions] as List<ObjectId>).last;
    (_openTransactions[parentTransactionId.toHexString()][headerChildrenList]
            as List)
        .remove(childTransactionMap[docId]);
  }

  int _getNewTransactionIndex(ObjectId transactionId) {
    final Map<String, dynamic> transactionMap =
        _openTransactions[transactionId.toHexString()];
    if (transactionMap == null || transactionMap.isEmpty) {
      throw ArgumentError(
          'Trying to work on a not existing transaction ($transactionId)');
    }
    int index = (transactionMap[TtsDb.transactionIndex] as int) ?? 0;
    index++;
    transactionMap[TtsDb.transactionIndex] = index;
    return index;
  }

  Future<ObjectId> openTts(
      {int transactionIsolation, ObjectId parentTransactionId}) async {
    transactionIsolation ??= trIsolationCommittedDocuments;
    final ObjectId transactionId = ObjectId();
    List<ObjectId> parentTransactions = <ObjectId>[];
    if (parentTransactionId != null) {
      final Map parentHeader =
          await headerCollection.findOne(where.id(parentTransactionId));
      assert(parentHeader != null, "Missing $parentTransactionId transaction");
      for (ObjectId element
          in parentHeader[headerParentTransactions] ?? <ObjectId>[]) {
        parentTransactions.add(element);
      }
      /*parentTransactions = (parentHeader[headerParentTransactions] ??
          <ObjectId>[]) as List<ObjectId>;*/
      parentTransactions.add(parentTransactionId);
      //parentHeader[headerChildrenList].add(transactionId);
      final LastError le = await headerCollection.updateLE(
          where.id(parentHeader[docId] as ObjectId),
          modify.addToSet(headerChildrenList, transactionId));
      if (le.isError) {
        throw MongoDartError(
            "Error subscribing inner transaction "
            "$transactionId in parent ${parentHeader[docId]}. "
            "Detail Message: ${le.err}",
            errorCode: "Tdb01");
      }
    }

    final Map<String, dynamic> document = <String, dynamic>{
      docId: transactionId,
      headerStartDate: DateTime.now().toUtc(),
      headerState: trStateOpen, //TransactionState.open,
      headerTransactionIsolation: transactionIsolation,
      headerParentTransactions: parentTransactions,
      headerChildrenList: <ObjectId>[]
    };
    _subscribeTransaction(document);
    try {
      final LastError ret = await headerCollection.saveLE(document);
      if (ret.isError) {
        throw MongoDartError(
            "Error creating transaction $transactionId. "
            "Detail Message: ${ret.err}",
            errorCode: "Tdb02");
      }
      if (parentTransactionId != null) {
        _registerChildTransaction(document);
      }
    } catch (e) {
      _unsubscribeTransaction(transactionId);
      rethrow;
    }
    return transactionId;
  }

  Future closeTts(ObjectId transactionId) async {
    final Map headerMap =
        await headerCollection.findOne(where.id(transactionId));
    assert(headerMap != null, "null parameter in call to closeTts");
    if ((headerMap[headerChildrenList] as List).isNotEmpty) {
      for (ObjectId childrenTransactionId in headerMap[headerChildrenList]) {
        final Map childrenMap =
            await headerCollection.findOne(where.id(transactionId));
        if (childrenMap == null || childrenMap[headerState] != trStateClosed) {
          throw MongoDartError(
              "Children transaction still open. "
              "Cannot close the transaction",
              errorCode: "Tdb03");
        }
        await closeTts(childrenTransactionId);
      }
    }
    await headerCollection.update(
        where.id(transactionId),
        modify
          ..set(TtsDb.headerEndDate, DateTime.now().toUtc())
          ..set(TtsDb.headerState, trStateClosed));

    ObjectId parentTransactionId;
    if (headerMap[headerParentTransactions].isNotEmpty as bool) {
      parentTransactionId =
          (headerMap[headerParentTransactions] as List).last as ObjectId;
    }
    LastError ret;
    if (parentTransactionId == null) {
      ret = await transactionCollection
          .removeLE(where.eq(TtsDb.transactionTransactionId, transactionId));
    } else {
      ret = await transactionCollection.updateLE(
          where.eq(TtsDb.transactionTransactionId, transactionId),
          modify..set(TtsDb.transactionTransactionId, parentTransactionId),
          multiUpdate: true);
      if (ret.isError) {
        throw StateError('Error unlocking transaction $transactionId. '
            'Detail Error: ${ret.err}');
      }
      // renumber indexes
      final Stream<Map> transactions = transactionCollection
          .find(where.eq(TtsDb.transactionTransactionId, parentTransactionId));
      int newIndex = 1;
      await for (Map document in transactions) {
        final LastError le = await transactionCollection.updateLE(
            where.id(document[docId] as ObjectId),
            modify.set(TtsDb.transactionIndex, newIndex));
        if (le.isError) {
          throw StateError('Error unlocking transaction $transactionId. '
              'Detail Error: ${le.err}');
        }
        newIndex++;
      }

      final LastError le = await headerCollection.updateLE(
          where.id(parentTransactionId),
          modify.pull(headerChildrenList, transactionId));
      if (le.isError) {
        throw StateError('Error unlocking transaction $transactionId. '
            'Detail Error: ${le.err}');
      }
    }
    if (ret.isError) {
      throw StateError('Error unlocking transaction $transactionId. '
          'Detail Error: ${ret.err}');
    }

    final LastError retHeader =
        await headerCollection.removeLE(where.id(transactionId));
    if (retHeader.isError) {
      throw StateError('Error unlocking transaction $transactionId. '
          'Detail Error: ${retHeader.err}');
    }
    _unsubscribeTransaction(transactionId);
  }

  /// Abort a transactions
  ///
  /// For each document it restores the situation as it was at the beginning
  /// of the transaction
  Future abortTts(ObjectId transactionId) async {
    final Map headerMap =
        await headerCollection.findOne(where.id(transactionId));
    if (headerMap == null) {
      var error = 'Could not find transaction "${transactionId.toHexString()}"';
      throw StateError(error);
    }
    //assert(headerMap != null, "null header in call to abortTts");

    // needed for aborting transactions in a different session
    if (!_openTransactions.containsKey(transactionId.toHexString())) {
      _subscribeTransaction(headerMap);
    }

    await headerCollection.update(where.id(transactionId),
        modify..set(TtsDb.headerState, trStateAborting));
    if ((headerMap[headerChildrenList] as List).isNotEmpty) {
      for (ObjectId childTransactionId in headerMap[headerChildrenList]) {
        await abortTts(childTransactionId);
      }
    }

    final Map transactionDocumentIds = await transactionCollection.distinct(
        TtsDb.transactionDocumentId,
        where.eq(TtsDb.transactionTransactionId, transactionId));
    List<ObjectId> transactionIds = <ObjectId>[];
    for (ObjectId element in transactionDocumentIds['values']) {
      transactionIds.add(element);
    }
    final Map originalTransactions = await transactionCollection
        .findOriginalTransactionPerDocumentIds(transactionId, transactionIds);

    for (var documentId in originalTransactions.keys) {
      final Map originalTransaction = originalTransactions[documentId] as Map;
      assert(
          originalTransaction != null,
          "As we have found the documentId in the transaction, "
          "we necessarily have to find a transaction document");

      if (originalTransaction[transactionTransactionType] == trTypeInsert) {
        final insertCollection = collection(
            originalTransaction[transactionCollectionName] as String);
        final Map retRemove = await insertCollection.removeTts(
            where.eq(docId, documentId), transactionId);
        // It can happen that the document has been already removed in the
        // transaction so it is legal if there are no removed documents
        // in this case (retRemove['n'] == 0)
        if (retRemove['err'] != null) {
          throw MongoDartError(
              'Error aborting transaction $transactionId removing document '
              '$documentId in collection ${insertCollection.collectionName}: '
              '${retRemove['err']}',
              errorCode: "Tdb04");
        }
      } else {
        final oldCollection = collection(
            originalTransaction[transactionCollectionName] as String);
        // using update(upsert: true), in case that the document have been
        // deleted after initial update inside the transaction (if update)
        // or reinserted after initial delete (if remove).
        final Map retOldDoc = await oldCollection.updateTts(
            where.eq(docId, documentId),
            originalTransaction[transactionPreviousDocument],
            transactionId,
            upsert: true);
        if (retOldDoc['err'] != null) {
          throw MongoDartError(
              'Error aborting transaction $transactionId restoring document '
              '$documentId in collection ${oldCollection.collectionName}: '
              '${retOldDoc['err']}',
              errorCode: "Tdb05");
        }
        if (retOldDoc['n'] == 0) {
          throw MongoDartError(
              'Error aborting transaction $transactionId restoring document '
              '$documentId in collection ${oldCollection.collectionName}: '
              'Document not found',
              errorCode: "Tdb06");
        }
      }
      //Remove all transactions for that document in that transaction cycle
      final Map retClear = await transactionCollection.remove(where
        ..eq(transactionTransactionId, transactionId)
        ..eq(transactionDocumentId, documentId));
      if (retClear['err'] != null) {
        throw MongoDartError('Error aborting transaction $transactionId '
            'cleaning document transactions: ${retClear['err']}, errorCode: "Tdb07"');
      }
    }
    // remove eventual collection locks
    final ret = await transactionCollection
        .remove(where.eq(TtsDb.transactionTransactionId, transactionId));
    if (ret['err'] != null) {
      throw MongoDartError(
          'Error aborting transaction $transactionId removing table locks',
          errorCode: "Tdb08");
    }
    if (headerMap[headerParentTransactions].isNotEmpty as bool) {
      final ObjectId parentTransactionId =
          (headerMap[headerParentTransactions] as List).last as ObjectId;
      if (parentTransactionId != null) {
        final LastError le = await headerCollection.updateLE(
            where.id(parentTransactionId),
            modify.pull(headerChildrenList, transactionId));
        if (le.isError) {
          throw StateError('Error unlocking transaction $transactionId. '
              'Detail Error: ${le.err}');
        }
      }
    }

    final retHeader = await headerCollection.remove(where.id(transactionId));
    if (retHeader['err'] != null) {
      throw MongoDartError(
          'Error aborting transaction $transactionId '
          'removing header document',
          errorCode: "Tdb09");
    }
    _unsubscribeTransaction(transactionId);
  }

  Future<bool> isTransactionClosed(ObjectId transactionId) async {
    final Map headerDoc =
        await headerCollection.findOne(where.id(transactionId));
    if (headerDoc == null) {
      return true;
    }
    return headerDoc[TtsDb.headerState] == trStateClosed;
  }

  Future<bool> isTransactionFrozen(ObjectId transactionId) async {
    final Map headerDoc =
        await headerCollection.findOne(where.id(transactionId));
    if (headerDoc == null) {
      return false;
    }
    return (headerDoc[TtsDb.headerChildrenList] as List).isNotEmpty;
  }

  @override
  Future close() async {
    try {
      //Todo rollBack OpenTransactions
    } finally {
      await super.close();
    }
  }
}
