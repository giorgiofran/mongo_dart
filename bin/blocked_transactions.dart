#!/usr/bin/env dart
import 'dart:io';

import 'package:logging/logging.dart';
import 'package:mongo_dart/mongo_dart.dart';

import 'db/discover_blocked_transactions.dart';
import 'utils/arguments_blocked.dart';
import 'utils/report.dart';

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
    var argResults = parseArgumentsBlocked(args);
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
          '${argResults[parmServer]} on port ${argResults[parmPort]}');
    } else {
      log.severe('Cannot open Database "${argResults[parmDb]}" on server '
          '${argResults[parmServer]} on port ${argResults[parmPort]}');
      exit(1);
    }
    var transactionsIds = await discoverBlockedTransactions(db, argResults[parmMinutes]);
    printReport(transactionsIds.values);

    await db.close();
    log.info('Database "${argResults[parmDb]}" closed on server '
        '${argResults[parmServer]} on port ${argResults[parmPort]}');
  } catch (e, stack) {
    log.severe('$e\n$stack');
    exit(1);
  }
  exit(0);
}
