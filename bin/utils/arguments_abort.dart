import 'package:args/args.dart';

import '../blocked_transactions.dart';

const String parmHelp = 'help';
const String parmVerbose = 'verbose';
const String parmDebug = 'debug';
const String parmServer = 'server';
const String parmPort = 'port';
const String parmDb = 'db';
const String parmTransactionId = 'transaction';

ArgResults parseArgumentsAbort(List<String> args) {
  var parser = ArgParser();
  parser.addFlag(parmHelp,
      negatable: false, abbr: 'h', defaultsTo: false, help: 'This help.');
  parser.addFlag(parmVerbose,
      negatable: false,
      abbr: 'v',
      defaultsTo: false,
      help: 'Shows also info messages (default only warnings and errors).');
  parser.addFlag(parmDebug,
      negatable: false,
      defaultsTo: false,
      help:
          'Shows all messages (cannot be used in conjunction with --verbose)');
  parser.addOption(parmServer,
      abbr: 's', defaultsTo: '127.0.0.1', help: 'Server name (or IP address)');
  parser.addOption(parmPort,
      abbr: 'p', defaultsTo: '27017', help: 'server port (defaults to 27017)');
  parser.addOption(parmDb, abbr: 'd', help: 'Data base name');
  parser.addOption(parmTransactionId,
      abbr: 't', help: 'Transaction to be a aborted');
  ArgResults results;
  try {
    results = parser.parse(args);
    if (results[parmHelp]) {
      print(parser.usage);
    } else if (results[parmVerbose] && results[parmDebug]) {
      log.severe(
          'You cannot specify the --verbose and the --debug options together');
      return null;
    } else if (results[parmDb] == null || results[parmDb] == '') {
      log.severe('The database name is mandatory');
      return null;
    } else if (results[parmTransactionId] == null ||
        results[parmTransactionId] == '') {
      log.severe('The Transaction Id parameter is mandatory');
      return null;
    }
  } on FormatException catch (e) {
    log.severe('$e');
  }

  return results;
}
