import 'dart:io';
import 'dart:isolate';

import 'isolate_info.dart';
import 'mongodb_version_config.dart';
import 'run_server_isolate.dart';

Future<IsolateInfo> runServer(
    MongoDbVersionConfig activeConf, File daemon, Directory dbPath,
    {String host, String port, List<String> moreParameters}) async {
  var isolateToMain = ReceivePort();

  var isolate = await Isolate.spawn(run_server_isolate, isolateToMain.sendPort);

  // The 'echo' isolate sends it's SendPort as the first message
  var sendPort = await isolateToMain.first;

  var receivePort = ReceivePort();
  sendPort.send([
    receivePort.sendPort,
    daemon.path,
    host ?? '127.0.0.1',
    port ?? '27017',
    dbPath.path,
    moreParameters
  ]);

  return IsolateInfo(
      isolate: isolate, sendPort: sendPort, receivePort: receivePort);
/* 
  await Future.delayed(Duration(seconds: 2));
  // If still running, is Ok
  if (await checkForServerRunning()) {
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
  } */
}
