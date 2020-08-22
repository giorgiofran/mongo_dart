#!/usr/bin/env dart
import 'dart:io';

import 'package:logging/logging.dart';
import 'package:mongo_dart/mongo_dart.dart';

import 'utils/arguments_abort.dart';

final log = Logger('blocked_transactions');

String formatTimeRef(DateTime time) =>
    time.toIso8601String().replaceFirst('T', ' ').split('.').first;

Future<void> main(List<String> args) async {
  Logger.root.level = Level.WARNING;
  Logger.root.onRecord.listen((record) {
    print('${record.level.name}: ${formatTimeRef(record.time)}: '
        '${record.message}');
  });

  try {
    // Create parameter structure and parses arguments
    var argResults = parseArgumentsAbort(args);
    if (argResults == null) {
      // Error message already logged in the parseArguments() method
      exit(1);
    } else if (argResults[parmHelp]) {
      return;
    } else if (argResults[parmVerbose]) {
      Logger.root.level = Level.INFO;
    } else if (argResults[parmDebug]) {
      Logger.root.level = Level.ALL;
    }

    final db = TtsDb('mongodb://${argResults[parmServer]}:'
        '${argResults[parmPort]}/${argResults[parmDb]}');
    await db.open();
    if (db.state == State.OPEN) {
      log.info('Database "${argResults[parmDb]}" opened on server '
          '${argResults[parmServer]} on port ${argResults[parmPort]}\n');
    } else {
      log.severe('Cannot open Database "${argResults[parmDb]}" on server '
          '${argResults[parmServer]} on port ${argResults[parmPort]}');
      exit(1);
    }

    try {
      await db.abortTts(ObjectId.fromHexString(argResults[parmTransactionId]));
      log.info(
          'Transaction "${argResults[parmTransactionId]}" aborted successfully\n');
    } catch (e) {
      log.severe(
          "Couldn't abort transaction \"${argResults[parmTransactionId]}\" "
          'because of the following error: \n- $e');
    }

    await db.close();
    log.info('Database "${argResults[parmDb]}" closed on server '
        '${argResults[parmServer]} on port ${argResults[parmPort]}');
  } catch (e, stack) {
    log.severe('$e\n$stack');
    exit(1);
  }
  exit(0);
}
