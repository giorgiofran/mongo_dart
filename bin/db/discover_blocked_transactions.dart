import 'package:bson/bson.dart';
import 'package:mongo_dart/mongo_dart.dart';

// returns a Map with key the hexTransactionId and value
// a Map with value Name and value
Future<Map<String, Map<String, dynamic>>> discoverBlockedTransactions(
    TtsDb db, String minutes) async {
  var transactionsId = <String, Map<String, dynamic>>{};

  var dateLimit =
      DateTime.now().subtract(Duration(minutes: int.parse(minutes))).toUtc();
  //final List<Map> parentHeaders = <Map>[];
  await db.headerCollection
      .find(where.lt('startDate', dateLimit))
      .forEach((element) async {
    if (element[TtsDb.headerChildrenList] != null &&
        element[TtsDb.headerChildrenList].isNotEmpty) {
      if (await _discoverActiveChildren(db, dateLimit, element["_id"])) {
        return;
      }
      if (await _discoverActiveChildren(db, dateLimit, element["_id"])) {
        return;
      }
    }
    //parentHeaders.add(element);
    var hexId = (element['_id'] as ObjectId).toHexString();
    transactionsId[hexId] = element;
  });

  return transactionsId;
}

Future<bool> _discoverActiveChildren(
    TtsDb db, DateTime dateLimit, ObjectId parentTransactionId) async {
  var ret = false;
  var elementFound = await db.headerCollection.count(where
      .eq(TtsDb.headerParentTransactions, parentTransactionId)
      .gt(TtsDb.headerStartDate, dateLimit));
  if (elementFound > 0) {
    return true;
  }
  await db.headerCollection
      .find(where.eq(TtsDb.headerParentTransactions, parentTransactionId))
      .forEach((element) async {
    if (await _discoverActiveTransactions(db, dateLimit, element['_id'])) {
      ret = true;
    }
  });
  return ret;
}

Future<bool> _discoverActiveTransactions(
    TtsDb db, DateTime dateLimit, ObjectId parentTransactionId) async {
  return await db.transactionCollection.count(where
          .eq(TtsDb.transactionTransactionId, parentTransactionId)
          .gt(TtsDb.transactionDate, dateLimit)) >
      1;
}
