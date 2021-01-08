// The library loaded by spawnHybridUri() can import any packages that your
// package depends on, including those that only work on the VM.
import 'dart:io';

import 'package:stream_channel/stream_channel.dart';

// Once the hybrid isolate starts, it will call the special function
// hybridMain() with a StreamChannel that's connected to the channel
// returned spawnHybridCode().
void hybridMain(StreamChannel channel) async {
  /*  // Start a WebSocket server that just sends "hello!" to its clients.
  var server = await io.serve(webSocketHandler((webSocket) {
    webSocket.sink.add('hello!');
  }), 'localhost', 0); */

  await for (var msg in channel.stream) {
    try {
      final result = await Process.run(msg[0], [], runInShell: true);
      channel.sink.add(result);
    } catch (e) {
      channel.sink.add(e);
    }
  }
}
