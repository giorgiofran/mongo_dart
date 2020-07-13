import 'package:mongo_dart/mongo_dart.dart';

const stateOpen = '  Open  ';
const stateAborting = 'Aborting';
const stateClosed = ' Closed ';
const stateUnknown = 'Unknown ';

const isolationCommitted = 'Committed Documents';
const isolationAll = '   All Documents   ';
const isolationUnknown = '      Unknown      ';

void printReport(Iterable<Map<String, dynamic>> blockedHeaders) {
  print('\n  ****-Transaction Id-**** ******-Start Date-****** '
      '*-State-* ****-Isolation-****');
  String hexId;
  String state;
  String isolation;
  for (var element in blockedHeaders) {
    hexId = (element['_id'] as ObjectId).toHexString();
    switch (element[TtsDb.headerState]) {
      case trStateOpen:
        state = stateOpen;
        break;
      case trStateAborting:
        state = stateAborting;
        break;
      case trStateClosed:
        state = stateClosed;
        break;
      default:
        state = stateUnknown;
        break;
    }
    switch (element[TtsDb.headerTransactionIsolation]) {
      case TtsDb.trIsolationCommittedDocuments:
        isolation = isolationCommitted;
        break;
      case TtsDb.trIsolationAllDocuments:
        isolation = isolationAll;
        break;
      default:
        isolation = isolationUnknown;
        break;
    }
    print('- $hexId ${element[TtsDb.headerStartDate]}  $state $isolation');
  }
  print(' ');
}
