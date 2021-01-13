part of mongo_dart;

Map<String, dynamic> _selectorToMap(Object selector) {
  if (selector is SelectorBuilder) {
    return selector.map[r'$query'] as Map<String, dynamic>;
  } else if (selector is Map) {
    return selector as Map<String, dynamic>;
  }
  assert(false, 'Unknown selector type "${selector.runtimeType}"');
  return null;
}

SelectorBuilder _selectorToSelectorBuilder(Object selector) {
  if (selector is SelectorBuilder) {
    return selector;
  } else if (selector is Map) {
    return SelectorBuilder()..map[r'$query'] = selector;
  }
  assert(false, 'Unknown selector type "${selector.runtimeType}"');
  return null;
}

class TtsCollection extends TypedCollection {
  TtsCollection(TtsDb db, String collectionName) : super(db, collectionName);

  TtsDb get ttsDb => db as TtsDb;

  /// Save document when the transaction management is active
  ///
  /// The simple save function will be redirected here
  Future<Map<String, dynamic>> saveTts(
      Map<String, dynamic> document, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    if (collectionName == TtsDb.collectionHeaderName ||
        collectionName == TtsDb.collectionTransactionsName) {
      return super.save(document, writeConcern: writeConcern);
    }

    ObjectId documentId;
    if (document.containsKey(docId)) {
      documentId = document[docId] as ObjectId;
    }
    var isUpdate = true;
    if (documentId == null) {
      isUpdate = false;
      documentId = ObjectId();
      document[docId] = documentId;
    }

    final lockId = await lockDocument(transactionId, documentId);

    if (lockId == null) {
      throw MongoDartError('The table is already locked by another transaction',
          errorCode: 'Tco01');
    }
    if (transactionId == null) {
      Map returnSave;
      if (isUpdate) {
        returnSave = await super.update({docId: documentId}, document,
            upsert: true, writeConcern: writeConcern);
      } else {
        returnSave = await super.insert(document, writeConcern: writeConcern);
      }
      await unlockDocument(lockId);
      return returnSave as Map<String, dynamic>;
    }

    Map<String, dynamic> oldDocument;
    if (isUpdate) {
      oldDocument = await findOne(where.id(documentId));
      if (oldDocument != null && oldDocument.isEmpty) {
        oldDocument = null;
      }
    }
    final updateLock = await _updateLockDocument(lockId,
        previousDocument: oldDocument, newDocument: document);
    if (!updateLock) {
      //Todo What To Do?
    }

    if (isUpdate) {
      return super.update({docId: documentId}, document,
          upsert: true, writeConcern: writeConcern);
    } else {
      return super.insertAll(<Map<String, dynamic>>[document],
          writeConcern: writeConcern);
    }
  }

  Future<LastError> saveTtsLE(
      Map<String, dynamic> document, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    try {
      final Map ret =
          await saveTts(document, transactionId, writeConcern: writeConcern);
      return LastError.fromMap(ret);
    } catch (e) {
      rethrow;
    }
  }

  @override
  Future<Map<String, dynamic>> save(Map<String, dynamic> document,
          {WriteConcern writeConcern}) async =>
      saveTts(document, null, writeConcern: writeConcern);

  @override
  Future<LastError> saveLE(Map<String, dynamic> document,
          {WriteConcern writeConcern}) async =>
      saveTtsLE(document, null, writeConcern: writeConcern);

  /// Insert all the documents in a list under an open transaction
  ///
  /// The simple insertAll function will be redirected here
  Future<Map<String, dynamic>> insertAllTts(
      List<Map<String, dynamic>> documents, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    if (collectionName == TtsDb.collectionHeaderName ||
        collectionName == TtsDb.collectionTransactionsName) {
      return super.insertAll(documents, writeConcern: writeConcern);
    }

    final documentIdList = <ObjectId>[];
    ObjectId documentId;
    for (Map document in documents) {
      documentId = (document[docId] ?? ObjectId()) as ObjectId;
      document[docId] = documentId;
      documentIdList.add(documentId);
    }

    final lockIds = await lockDocuments(transactionId, documentIdList);

    if (lockIds == null) {
      throw MongoDartError('The table is already locked by another transaction',
          errorCode: 'Tco01');
    }
    if (transactionId == null) {
      final Map returnInsert =
          await super.insertAll(documents, writeConcern: writeConcern);
      await unlockDocuments(lockIds.values);
      return returnInsert as Map<String, dynamic>;
    }

    for (Map document in documents) {
      final updateLock = await _updateLockDocument(lockIds[document[docId]],
          newDocument: document);
      if (!updateLock) {
        //Todo What To Do?
      }
    }
    return await super.insertAll(documents, writeConcern: writeConcern);
  }

  Future<LastError> insertAllTtsLE(
      List<Map<String, dynamic>> documents, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    try {
      final Map ret = await insertAllTts(documents, transactionId,
          writeConcern: writeConcern);
      return LastError.fromMap(ret);
    } catch (e) {
      rethrow;
    }
  }

  @override
  Future<Map<String, dynamic>> insertAll(List<Map<String, dynamic>> documents,
          {WriteConcern writeConcern}) async =>
      insertAllTts(documents, null, writeConcern: writeConcern);

  Future<LastError> insertAllLE(List<Map<String, dynamic>> documents,
          {WriteConcern writeConcern}) async =>
      insertAllTtsLE(documents, null, writeConcern: writeConcern);

  Future<Map<String, dynamic>> _upsertTts(
      selector, document, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    // Todo extract the documentId from the selector when possible
    final ObjectId documentId = document[docId] as ObjectId ?? ObjectId();

    final ObjectId lockId = await lockDocument(transactionId, documentId);

    if (lockId == null) {
      throw MongoDartError('The table is already locked by another transaction',
          errorCode: "Tco01");
    }
    if (transactionId == null) {
      final Map returnUpsert = await super
          .update(selector, document, upsert: true, writeConcern: writeConcern);
      await unlockDocument(lockId);
      return returnUpsert as Map<String, dynamic>;
    }

    final bool updateLock =
        await _updateLockDocument(lockId, newDocument: document);
    if (!updateLock) {
      //Todo What To Do?
    }

    return await super
        .update(selector, document, upsert: true, writeConcern: writeConcern);
  }

  Future<Map<String, dynamic>> _updateAllTts(
      selector, document, ObjectId transactionId, List<Map> keyMapList,
      {WriteConcern writeConcern}) async {
    final documentIdList = <ObjectId>[];
    ObjectId documentId;
    for (Map document in keyMapList) {
      documentId = document[docId] as ObjectId;
      assert(documentId != null, 'Document Id cannot be null here');
      documentIdList.add(documentId);
    }

    final lockIds = await lockDocuments(transactionId, documentIdList);

    if (lockIds == null) {
      throw MongoDartError('The table is already locked by another transaction',
          errorCode: 'Tco01');
    }
    if (transactionId == null) {
      final Map returnUpdate = await super.update(selector, document,
          multiUpdate: true, writeConcern: writeConcern);
      await unlockDocuments(lockIds.values);
      return returnUpdate as Map<String, dynamic>;
    }

    final documents = await find(selector).toList();

    for (var oldDocument in documents) {
      final updateLock = await _updateLockDocument(lockIds[document[docId]],
          previousDocument: oldDocument, newDocument: document);
      if (!updateLock) {
        //Todo What To Do?
      }
    }
    return super.update(selector, document,
        multiUpdate: true, writeConcern: writeConcern);
  }

  Future<Map<String, dynamic>> updateTts(
      selector, document, ObjectId transactionId,
      {bool upsert, bool multiUpdate, WriteConcern writeConcern}) async {
    upsert ??= false;
    multiUpdate ??= false;
    final SelectorBuilder selectorBuilder =
        _selectorToSelectorBuilder(selector);
    final SelectorBuilder selectorLimited = where
      ..map = Map.from(selectorBuilder.map);

    if (collectionName == TtsDb.collectionHeaderName ||
        collectionName == TtsDb.collectionTransactionsName) {
      return super.update(selector, document,
          upsert: upsert, multiUpdate: multiUpdate, writeConcern: writeConcern);
    }

    if (!multiUpdate) {
      selectorLimited
        ..limit(1)
        ..fields([docId]);
    }

    final List<Map> docs = await find(selectorLimited).toList();

    if (docs.isEmpty) {
      if (!upsert) {
        return {'ok': 1.0, 'n': 0, 'err': null};
      }
      return _upsertTts(selector, document, transactionId,
          writeConcern: writeConcern);
    }
    if (docs.length > 1) {
      return _updateAllTts(selector, document, transactionId, docs,
          writeConcern: writeConcern);
    }
    final ObjectId documentId = docs.first[docId] as ObjectId;

    final ObjectId lockId = await lockDocument(transactionId, documentId);

    if (lockId == null) {
      throw MongoDartError('The table is already locked by another transaction',
          errorCode: "Tco01");
    }
    if (transactionId == null) {
      final Map returnUpdate =
          await super.update(selector, document, writeConcern: writeConcern);
      await unlockDocument(lockId);
      return returnUpdate as Map<String, dynamic>;
    }

    Map<String, dynamic> oldDocument;
    oldDocument = await findOne(where.id(documentId));
    if (oldDocument != null && oldDocument.isEmpty) {
      oldDocument = null;
    }

    final bool updateLock = await _updateLockDocument(lockId,
        previousDocument: oldDocument, newDocument: document);
    if (!updateLock) {
      //Todo What To Do?
    }

    return super.update(selector, document, writeConcern: writeConcern);
  }

  Future<LastError> updateTtsLE(selector, document, ObjectId transactionId,
      {bool upsert, bool multiUpdate, WriteConcern writeConcern}) async {
    try {
      final Map ret = await updateTts(selector, document, transactionId,
          upsert: upsert, multiUpdate: multiUpdate, writeConcern: writeConcern);
      return LastError.fromMap(ret);
    } catch (e) {
      rethrow;
    }
  }

  @override
  Future<Map<String, dynamic>> update(selector, document,
      {bool upsert = false,
      bool multiUpdate = false,
      WriteConcern writeConcern}) async {
    return updateTts(selector, document, null,
        upsert: upsert, multiUpdate: multiUpdate, writeConcern: writeConcern);
  }

  Future<LastError> updateLE(selector, document,
      {bool upsert, bool multiUpdate, WriteConcern writeConcern}) async {
    return updateTtsLE(selector, document, null,
        upsert: upsert, multiUpdate: multiUpdate, writeConcern: writeConcern);
  }

  /// Returns the document how it was at the beginning of the
  /// actual transaction.
  ///
  /// If the document have been inserted in the transaction, an empty map
  /// is returned (so we can distinguish between non existing document now
  /// under a transaction and a document that is not in any transaction,
  /// in this case the method return null).
  ///
  Future<Map<String, dynamic>> _findOriginalDocument(
      ObjectId transactionId, ObjectId documentId) async {
    final Map<String, dynamic> transactionMap =
        await _findFirstTransaction(transactionId, documentId);
    if (transactionMap == null) {
      return null;
    }
    return (transactionMap[TtsDb.transactionPreviousDocument] ??
        <String, dynamic>{}) as Map<String, dynamic>;
  }

  /// Return the first transaction. the one that contains the document how
  /// it was at the beginning of required transaction.
  Future<Map<String, dynamic>> _findFirstTransaction(
      ObjectId transactionId, ObjectId documentId) async {
    final SelectorBuilder selector = where
      ..eq(TtsDb.transactionTransactionId, transactionId)
      ..eq(TtsDb.transactionDocumentId, documentId)
      ..ne(TtsDb.transactionTransactionType, TtsDb.trTypeLock)
      ..sortBy(TtsDb.transactionDate);
    return (db as TtsDb).transactionCollection.findOne(selector);
  }

  /// This method returns a map containing a pair
  /// documentId-original Transaction, where original transaction is the
  /// one (first) containing the document in its original status
  /// (previousDocument field).
  /// A document can be registered under more than one transaction,
  /// in case of nested transactions or pending closed/aborting transactions.
  Future<Map<ObjectId, Map>> findOriginalTransactionPerDocumentIds(
      ObjectId transactionId, Iterable<ObjectId> documentIds) async {
    final Map<ObjectId, Map> ret = <ObjectId, Map>{};
    for (ObjectId documentId in documentIds) {
      ret[documentId] = await _findFirstTransaction(transactionId, documentId);
    }
    return ret;
  }

  Future<Map<String, dynamic>> insertTts(
          Map<String, dynamic> document, ObjectId transactionId,
          {WriteConcern writeConcern}) async =>
      insertAllTts([document], transactionId, writeConcern: writeConcern);

  @override
  Future<Map<String, dynamic>> insert(Map<String, dynamic> document,
          {WriteConcern writeConcern}) async =>
      insertTts(document, null, writeConcern: writeConcern);

  Future<LastError> insertTtsLE(
      Map<String, dynamic> document, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    try {
      final Map ret = await insertAllTts([document], transactionId,
          writeConcern: writeConcern);
      return LastError.fromMap(ret);
    } catch (e) {
      rethrow;
    }
  }

  Future<LastError> insertLE(
          Map<String, dynamic> document, ObjectId transactionId,
          {WriteConcern writeConcern}) async =>
      insertTtsLE(document, null, writeConcern: writeConcern);

  Future<Map<String, dynamic>> removeTts(selector, ObjectId transactionId,
      {WriteConcern writeConcern, bool allOrNothing}) async {
    allOrNothing ??= true;

    if (collectionName == TtsDb.collectionHeaderName ||
        collectionName == TtsDb.collectionTransactionsName) {
      return super.remove(selector, writeConcern: writeConcern);
    }
    var partialSelector;
    if (selector is SelectorBuilder) {
      partialSelector = selector;
      if (transactionId == null) {
        partialSelector..fields([docId]);
      }
    } else if (selector is Map) {
      partialSelector = Map.from(selector);
    } else {
      throw MongoDartError('Unknown selector type ${selector.runtimeType}',
          errorCode: "Tco06");
    }
    final Stream<Map<String, dynamic>> keyMapList = find(selector);
    final List<ObjectId> documentIdList = <ObjectId>[];
    final List<Map<String, dynamic>> selectedDocuments =
        <Map<String, dynamic>>[];
    ObjectId documentId;
    await for (Map<String, dynamic> document in keyMapList) {
      documentId = document[docId] as ObjectId;
      assert(documentId != null, "Document Id cannot be null here");
      documentIdList.add(documentId);
      if (transactionId != null) {
        selectedDocuments.add(document);
      }
    }

    final Map<ObjectId, ObjectId> lockIds = await lockDocuments(
        transactionId, documentIdList,
        allOrNothing: allOrNothing);

    if (lockIds == null) {
      throw MongoDartError(
          'At least one document is already locked by another transaction',
          errorCode: "Tco07");
    }
    if (lockIds.length < documentIdList.length && allOrNothing) {
      throw MongoDartError(
          'At least one document is already locked by another transaction',
          errorCode: "Tco08");
    }
    if (transactionId == null) {
      if (allOrNothing || documentIdList.length == lockIds.length) {
        final Map returnDelete =
            await super.remove(selector, writeConcern: writeConcern);
        await unlockDocuments(lockIds.values);
        if (returnDelete['err'] != null) {
          throw MongoDartError(
              "Error removing records. Detail: ${returnDelete['err']}",
              errorCode: "Tco09");
        }
        return returnDelete as Map<String, dynamic>;
      }
      Map returnDelete;
      String errMsg;
      for (ObjectId documentId in lockIds.keys) {
        partialSelector = where.eq(docId, documentId);
        returnDelete =
            await super.remove(partialSelector, writeConcern: writeConcern);
        if (returnDelete['err'] != null) {
          errMsg = returnDelete['err'] as String;
        }
        await unlockDocument(lockIds[documentId]);
      }
      if (errMsg != null) {
        throw MongoDartError("Error removing records. Detail: $errMsg",
            errorCode: "Tco10");
      }
      return {'ok': 1.0, 'n': lockIds.length, 'err': null};
    }

    //final List<Map> documents = await find(selector).toList();

    for (Map<String, dynamic> oldDocument in selectedDocuments) {
      final bool updateLock = await _updateLockDocument(
          lockIds[oldDocument[docId]],
          previousDocument: oldDocument,
          newDocument: null);
      if (!updateLock) {
        //Todo What To Do?
      }
    }
    if (allOrNothing || documentIdList.length == lockIds.length) {
      return await super.remove(selector, writeConcern: writeConcern);
    }
    Map returnDelete;
    String errMsg;
    for (ObjectId documentId in lockIds.keys) {
      partialSelector = where.eq(docId, documentId);
      returnDelete =
          await super.remove(partialSelector, writeConcern: writeConcern);
      if (returnDelete['err'] != null) {
        errMsg = returnDelete['err'] as String;
      }
    }
    if (errMsg != null) {
      throw MongoDartError("Error removing records. Detail: $errMsg",
          errorCode: "Tco11");
    }
    return {'ok': 1.0, 'n': lockIds.length, 'err': null};
  }

  Future<LastError> removeTtsLE(selector, ObjectId transactionId,
      {WriteConcern writeConcern}) async {
    try {
      final Map ret =
          await removeTts(selector, transactionId, writeConcern: writeConcern);
      return LastError.fromMap(ret);
    } catch (e) {
      rethrow;
    }
  }

  @override
  Future<Map<String, dynamic>> remove(selector, {WriteConcern writeConcern}) =>
      removeTts(selector, null, writeConcern: writeConcern);

  Future<LastError> removeLE(selector, {WriteConcern writeConcern}) =>
      removeTtsLE(selector, null, writeConcern: writeConcern);

  bool _isExclusiveLock(int lockType) => lockType & lkExclusiveLock > 0;
  bool _isChangesLock(int lockType) => lockType & lkChangesLock > 0;
  bool _isPreventChangesLock(int lockType) => lockType & lkPreventChanges > 0;
  bool _isEnsureChanges(int lockType) => lockType & lkEnsureChanges > 0;
  bool _isEnsureRead(int lockType) => lockType & lkEnsureRead > 0;

  // Todo Still to be implemented:
  @override
  Future<Map<String, dynamic>> findAndModify(
      {query, sort, bool remove, update, bool returnNew, fields, bool upsert}) {
    throw MongoDartError('Still to be implemented', errorCode: 'Tco12');
  }

  @override
  Future<bool> drop() =>
      throw MongoDartError('Still to be implemented', errorCode: 'Tco13');

  @override
  Future<Map<String, dynamic>> distinct(String field, [selector]) {
    if (collectionName == TtsDb.collectionHeaderName ||
        collectionName == TtsDb.collectionTransactionsName) {
      return super.distinct(field, selector);
    }
    return throw MongoDartError('distinct Still to be implemented',
        errorCode: 'Tco14');
  }

  @override
  Future<Map<String, dynamic>> aggregate(List pipeline,
          {bool allowDiskUse = false, Map<String, dynamic> cursor}) =>
      throw MongoDartError('Still to be implemented', errorCode: 'Tco15');

  @override
  Stream<Map<String, dynamic>> legacyAggregateToStream(List pipeline,
          {Map<String, dynamic> cursorOptions = const {},
          bool allowDiskUse = false}) =>
      throw MongoDartError('Still to be implemented', errorCode: 'Tco16');

  Future<bool> _checkLockCompatibility(ObjectId transactionId, int lockType,
      {ObjectId documentId, int lockLevel}) async {
    final transactions = <ObjectId, bool>{};

    final selector = where
        .eq(TtsDb.transactionCollectionName, collectionName)
        .ne(TtsDb.transactionTransactionId, transactionId);

    if (transactionId != null) {
      final Map headerMap =
          await (db as TtsDb).headerCollection.findOne(where.id(transactionId));
      assert(headerMap != null,
          'Not exixsting transaction Id in call to _checkLockCompatibility');
      if ((headerMap[TtsDb.headerParentTransactions] as List).isNotEmpty) {
        selector.and(where.nin(TtsDb.transactionTransactionId,
            headerMap[TtsDb.headerParentTransactions] as List));
      }
    }

    if (documentId != null) {
      selector.and(where.eq(TtsDb.transactionDocumentId, documentId).or(
          where.eq(TtsDb.transactionTransactionLevel, TtsDb.trLvCollection)));
    }

    final Stream<Map> ret = (db as TtsDb).transactionCollection.find(selector);
    await for (Map document in ret) {
      final transactionId =
          document[TtsDb.transactionTransactionId] as ObjectId;
      if (transactionId != null) {
        var trStatus = transactions[transactionId];
        if (trStatus == null) {
          trStatus = await (db as TtsDb).isTransactionClosed(transactionId);
          transactions[transactionId] = trStatus;
        }
        if (trStatus) {
          continue;
        }
      }
      final documentLock = (document[TtsDb.transactionLockType] as int) << 16;
      final checkResult = lockCompatibilityMap[documentLock | lockType];
      assert(
          checkResult != null,
          'Lock Combination between existing '
          '${document[TtsDb.transactionLockType]} and $lockType is missing');
      if (!checkResult) {
        return false;
      }
    }
    return true;
  }

  /// check if the operation is of type "Field Update"
  ///
  /// If all operators are of type "Field Update Operator"
  /// It is a field update type, otherwise a document update type
  bool _checkFieldUpdate(Map<String, dynamic> newUpdate) {
    for (var key in newUpdate.keys) {
      switch (key) {
        case r'$currentDate':
        case r'$inc':
        case r'$min':
        case r'$max':
        case r'$mul':
        case r'$rename':
        case r'$set':
        case r'$setOnInsert':
        case r'$unset':
          break;
        default:
          return false;
      }
    }
    return true;
  }

  int _checkTransactionType(int transactionType, Map previousDocument,
      Map<String, dynamic> newDocument) {
    if (newDocument == null && previousDocument == null) {
      if (transactionType != null && transactionType != TtsDb.trTypeLock) {
        throw MongoDartError(
            'If the transaction Type is not "lock", '
            'the new or the previous document (or both) are required',
            errorCode: 'Tco17');
      }
      return TtsDb.trTypeLock;
    }
    if (newDocument == null) {
      if (transactionType != null && transactionType != TtsDb.trTypeDelete) {
        throw MongoDartError(
            'If the transaction Type is not "delete", '
            'the new document is required',
            errorCode: 'Tco18');
      }
      return TtsDb.trTypeDelete;
    }
    if (previousDocument == null) {
      if (transactionType != null && transactionType != TtsDb.trTypeDelete) {
        throw MongoDartError(
            'If the transaction Type is not "insert", '
            'the previous document is required',
            errorCode: 'Tco19');
      }
      return TtsDb.trTypeInsert;
    }
    if (_checkFieldUpdate(newDocument)) {
      return TtsDb.trTypeUpdateField;
    }
    return TtsDb.trTypeUpdateDocument;
  }

  /// Prepare the update document to be stored.
  ///
  /// If the update document contains update commands, as "$set for instance,
  /// it cannot be stored, so we replace the "$" sign with "dbCommand",
  /// so the new key will be "dbCommandSet".
  Map<String, dynamic> _sanitizeNewDocument(Map<String, dynamic> newDocument) {
    if (newDocument == null) {
      return newDocument;
    }
    final ret = <String, dynamic>{};
    String newKey;
    for (var key in newDocument.keys) {
      if (key.startsWith(r'$')) {
        newKey = key.replaceFirst(r'$', 'dbCommand');
        ret[newKey] = newDocument[key];
      } else {
        ret[key] = newDocument[key];
      }
    }
    return ret;
  }

  Future<bool> _updateLockDocument(ObjectId lockId,
      {Map<String, dynamic> previousDocument,
      dynamic newDocument,
      int transactionType}) async {
    Map<String, dynamic> newDoc;
    if (newDocument != null) {
      if (newDocument is ModifierBuilder) {
        newDoc = newDocument.map;
      } else if (newDocument is Map<String, dynamic>) {
        newDoc = newDocument;
      } else {
        assert(false, 'Unknown modifier type ${newDocument.runtimeType}');
      }
      newDoc = _sanitizeNewDocument(newDoc);
    }
    final modifier = modify;
    if (previousDocument != null) {
      modifier.set(TtsDb.transactionPreviousDocument, previousDocument);
    }
    if (newDoc != null) {
      modifier.set(TtsDb.transactionNewDocument, newDoc);
    }
    final type =
        _checkTransactionType(transactionType, previousDocument, newDoc);
    if (type != null) {
      modifier.set(TtsDb.transactionTransactionType, type);
      if (type == TtsDb.trTypeUpdateField) {
        modifier.set(TtsDb.transactionTransactionLevel, TtsDb.trLvFields);
      }
    }

    final Map retUpdate = await (db as TtsDb)
        .transactionCollection
        .update(where.id(lockId), modifier);
    if (retUpdate['err'] != null || retUpdate['n'] != 1) {
      return false;
    }

    return true;
  }

  ///Locks many documents
  ///
  /// This lock prepares the transaction document
  /// At this point still are missing:
  /// * previous document
  /// * new document
  /// * Transaction Type, (temporarily set it to "lock")
  /// * Transaction Level, (temporarily set it to document,
  ///   but it could be also fields)
  ///
  /// returns a Map containing the pairs DocumentId - LockId
  /// if at least a lock didn't succeed and the allOrNothing
  /// flag is true, a null value is returned, otherwise
  /// the list of all successful locks
  Future<Map<ObjectId, ObjectId>> lockDocuments(
      ObjectId transactionId, List<ObjectId> documentIdList,
      {bool allOrNothing}) async {
    allOrNothing ??= true;
    ObjectId lockId;
    final ret = <ObjectId, ObjectId>{};
    for (var documentId in documentIdList) {
      lockId = await lock(transactionId,
          lockType: lkExclusiveLock, documentId: documentId);
      if (lockId == null) {
        if (allOrNothing) {
          await unlockDocuments(ret.values);
          return null;
        }
        continue;
      }
      ret[documentId] = lockId;
    }
    return ret;
  }

  ///Locks a single document
  ///
  /// This lock prepares the transaction document
  /// At this point still are missing:
  /// * previous document
  /// * new document
  /// * Transaction Type, (temporarily set it to "lock")
  /// * Transaction Level, (temporarily set it to document,
  ///   but it could be also fields)
  Future<ObjectId> lockDocument(
      ObjectId transactionId, ObjectId documentId) async {
    return lock(transactionId,
        lockType: lkExclusiveLock, documentId: documentId);
  }

  /// Locks all documents in the collection
  ///
  /// By default the lock is of type tbLkEnsureChanges
  Future<ObjectId> lock(ObjectId transactionId,
      {int lockType, ObjectId documentId}) async {
    lockType ??= lkEnsureChanges;
    final isFrozen = await (db as TtsDb).isTransactionFrozen(transactionId);

    if (isFrozen) {
      throw MongoDartError('Attempt to lock a frozen transaction',
          errorCode: 'Tco20');
    }

    final id = ObjectId();
    final document = <String, dynamic>{
      docId: id,
      // transactionId can be null (only when a documentId is specified),
      //   see below,
      // No Document Id if it is a Collection Lock
      TtsDb.transactionLockType: lockType,
      TtsDb.transactionCollectionName: collectionName,
      TtsDb.transactionTransactionType: TtsDb.trTypeLock,
      TtsDb.transactionTransactionLevel: TtsDb.trLvCollection,
      // No previous Document, if it is a Collection Lock.
      // No new Document, if it is a Collection Lock.
      TtsDb.transactionDate: DateTime.now(),
      // Transaction Index only if a transaction is present
    };
    // transaction Id is null for all those operations made
    // outside a transaction
    if (transactionId != null) {
      document[TtsDb.transactionTransactionId] = transactionId;
      document[TtsDb.transactionIndex] =
          (db as TtsDb)._getNewTransactionIndex(transactionId);
    } else {
      if (documentId == null) {
        throw MongoDartError(
            'Locking outside a transaction is allowed only '
            'for documents (documentId is null)',
            errorCode: 'Tco21');
      }
    }
    if (documentId != null) {
      document[TtsDb.transactionDocumentId] = documentId;
      document[TtsDb.transactionTransactionType] = TtsDb.trTypeLock;
      document[TtsDb.transactionTransactionLevel] = TtsDb.trLvDocument;
    }

    final ret = await (db as TtsDb).transactionCollection.save(document);
    if (ret['err'] != null || ret['n'] != 1) {
      return null;
    }
    final compatible = await _checkLockCompatibility(transactionId, lockType,
        documentId: documentId);
    if (compatible) {
      return id;
    }
    final retDel =
        await (db as TtsDb).transactionCollection.remove(where.id(id));
    if (retDel['err'] != null || retDel['n'] != 1) {
      //Todo what to do in this case?
    }

    return null;
  }

  Future unlockDocuments(Iterable<ObjectId> lockIds) async {
    for (var lockId in lockIds) {
      await unlockDocument(lockId);
    }
  }

  Future unlockDocument(ObjectId lockId) async => await unlock(lockId);

  Future unlock(ObjectId lockId) async {
    final ret =
        await (db as TtsDb).transactionCollection.remove(where.id(lockId));
    if (ret['err'] != null || ret['n'] != 1) {
      throw MongoDartError(
          'No lock to remove for collection '
          '$collectionName with "id" = $lockId',
          errorCode: 'Tco22');
    }
  }

  /// Remove one lock of the desired type for the required collection
  ///
  /// By default the lock is of type tbLkEnsureChanges
  Future unlockByTransaction(ObjectId transactionId, {int lockType}) async {
    lockType ??= lkEnsureChanges;
    final toBeRemoved = await (db as TtsDb)
        .transactionCollection
        .find(where
          ..eq(TtsDb.transactionTransactionId, transactionId)
          ..eq(TtsDb.transactionCollectionName, collectionName)
          ..eq(TtsDb.transactionLockType, lockType)
          ..limit(1))
        .toList();
    if (toBeRemoved.isEmpty) {
      throw MongoDartError(
          'No lock to remove for collection '
          '$collectionName in transaction $transactionId',
          errorCode: 'Tco23');
    }
    return await unlock(toBeRemoved.first[docId] as ObjectId);
  }
}
