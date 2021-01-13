import 'dart:io';
import 'dart:isolate';

void run_server_isolate(SendPort mainToIsolate) async {
  // Open the ReceivePort for incoming messages.
  var port = ReceivePort();
  // Notify any other isolates what port this isolate listens to.
  mainToIsolate.send(port.sendPort);

  await for (var msg in port) {
    if (msg[0] is SendPort) {
      SendPort sendToMainProcess = msg[0];
      String daemonPath = msg[1];
      String host = msg[2];
      String hostPort = msg[3];
      String dbPath = msg[4];
      List<String> moreParameters = msg[5];
      try {
        final result = await Process.run(
            daemonPath,
            [
              '--bind_ip',
              '$host',
              '--port',
              '$hostPort',
              if (dbPath.isNotEmpty) '--dbpath',
              if (dbPath.isNotEmpty) '$dbPath',
              ...?moreParameters
            ],
            runInShell: true);
        sendToMainProcess.send(result);
      } catch (e) {
        sendToMainProcess.send(e);
      }
    }
    port.close();
  }
}
