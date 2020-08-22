part of mongo_dart;
/*

class CollectionSave<T> extends DbCollection {
  CollectionSave(Db db, String collectionName) : super(db, collectionName);

  Future call(Map document, {WriteConcern writeConcern}) async {
    // Todo WriteResult still is not managed
    if (T != Map && T != LastError */
/*&& T is! WriteResult*/ /*
) {
      throw new ArgumentError(
          '$T is not a Type accepted for the save function');
    }
    final Map ret = await super.save(document, writeConcern: writeConcern);
    assert(ret != null, "Save operation returned a null Map");
    if (T == Map) {
      return ret as T;
    } else if (T == LastError) {
      return new LastError.fromMap(ret) as T;
    }
    return new WriteResult.fromMap(ret) as T;
  }
}
*/

class TypedCollection extends DbCollection {
  TypedCollection(Db db, String collectionName) : super(db, collectionName);
/*

  CollectionSave get saveLastError =>
      new CollectionSave<LastError>(db, collectionName);
*/

  Future<LastError> saveLE(Map<String, dynamic> document,
      {WriteConcern writeConcern}) async {
    final Map ret = await save(document, writeConcern: writeConcern);
    return LastError.fromMap(ret);
  }

  // Todo other operations
}
