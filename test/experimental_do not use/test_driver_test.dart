@Timeout(Duration(seconds: 300))

import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'package:test/test.dart';
import 'database_test.dart' as database;
import 'authentication_test.dart' as authentication;
import 'op_msg_commands_test.dart' as op_msg;

enum AuthMechanism { none, scram }

List<MongoDbConf> testConfigurations = <MongoDbConf>[
  MongoDbConf('Standalone 4.2', 'MongoDb.4.2.sh', 'MongoDb.4.2.end.sh'),
  MongoDbConf(
      'Standalone 4.2 User Auth', 'MongoDb.4.2.auth.sh', 'MongoDb.4.2.end.sh',
      autMechanism: AuthMechanism.scram)
];

StringBuffer jsonOut = StringBuffer();

void main() async {
  try {
    group('Global Test runner', () {
      for (var config in testConfigurations) {
        //print('Starting server ${config.name}');
        test('Testing on server ${config.name}', () async {
          await runServer(config);
        });
        //print('Finished server ${config.name}');
      }
    });
  } catch (e, stack) {
    print('Error: $e');
    print(stack);
  }
}

Future<void> runServer(MongoDbConf activeConf) async {
  var isolateToMain = ReceivePort();

  var isolate = await Isolate.spawn(myIsolate, isolateToMain.sendPort);

  // The 'echo' isolate sends it's SendPort as the first message
  var sendPort = await isolateToMain.first;

  var responsePort = ReceivePort();
  sendPort.send([activeConf.startCommand, responsePort.sendPort]);
  await Future.delayed(Duration(seconds: 2));
  // If still running, is Ok
  if (await checkForServerRunning()) {
    if (activeConf.autMechanism == AuthMechanism.none) {
      await database.main();
      //await runTest(activeConf.name, 'test/new/database_test.dart');
      await op_msg.main();
      //await runTest(activeConf.name, 'test/new/op_msg_commands_test.dart');
    } else if (activeConf.autMechanism == AuthMechanism.scram) {
      await authentication.main();
      //await runTest(activeConf.name, 'test/new/authentication_test.dart');
    }
    await stopServer(activeConf);
    isolate.kill();
  }
  var returnMessage = await responsePort.first;

  if (returnMessage is ProcessResult) {
    if (returnMessage.exitCode != 0) {
      if (returnMessage.exitCode == 48) {
        print(returnMessage.stdout);
      } else {
        print(returnMessage.stderr);
      }
    } else {
      //print('Terminated...');
    }
  } else if (returnMessage is Error) {
    print('$returnMessage');
  }
}

// isolate process

void myIsolate(SendPort mainToIsolate) async {
  // Open the ReceivePort for incoming messages.
  var port = ReceivePort();
  // Notify any other isolates what port this isolate listens to.
  mainToIsolate.send(port.sendPort);

  await for (var msg in port) {
    try {
      final result = await Process.run(msg[0], [], runInShell: true);
      msg[1].send(result);
    } catch (e) {
      msg[1].send(e);
    }
    port.close();
  }
}

Future<void> stopServer(MongoDbConf configuration) async {
  var result = await Process.run(configuration.endCommand, []);
  if (result.exitCode != 0) {
    if (result.exitCode == 1 &&
        result.stderr.startsWith(
            'There doesn\'t seem to be a server running with dbpath:')) {
      return;
    }
    throw StateError(
        'Error trying to execute command "${configuration.endCommand}"');
  }
}

Future<bool> checkForServerRunning() async {
  var result = await Process.run('pgrep', ['mongod']);
  if (result.exitCode != 0) {
    return false;
  }
  return result.stdout.isNotEmpty;
}

Future<void> runTest(String serverName, String testPath) async {
  print('Start run test');
  var result = await Process.run(
      'dart', ['run', 'test', '--reporter', 'json', testPath]);
  if (result.exitCode != 0) {
    print(result.stderr);
  }
  try {
    String res = result.stdout;
    res = res.replaceFirst(testPath, '$serverName server');
    stdout.write(res);
    //print(res);
  } catch (e) {
    print(e);
  }
}

class MongoDbConf {
  final String name;
  final String startCommand;
  final String endCommand;
  final AuthMechanism autMechanism;

  MongoDbConf(this.name, this.startCommand, this.endCommand,
      {this.autMechanism = AuthMechanism.none});
}
