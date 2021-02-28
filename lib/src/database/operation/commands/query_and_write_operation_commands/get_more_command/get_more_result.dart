import 'package:mongo_dart/src/database/operation/commands/base/cursor_result.dart';
import 'package:mongo_dart/src/database/operation/commands/mixin/basic_result.dart';
import 'package:mongo_dart/src/database/operation/commands/mixin/timing_result.dart';
import 'package:mongo_dart/src/database/utils/map_keys.dart';

class GetMoreResult with BasicResult, TimingResult {
  GetMoreResult(Map<String, Object> document) {
    extractBasic(document);
    cursor = CursorResult(document[keyCursor]);
    extractTiming(document);
  }

  CursorResult cursor;
}
