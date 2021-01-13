// @dart=2.9

import 'package:test/test.dart';

void main() {
  test('connects to a server', () async {
    // Each spawnHybrid function returns a StreamChannel that communicates with
    // the hybrid isolate. You can close this channel to kill the isolate.
    var channel = spawnHybridUri('test_driver_server.dart');

    channel.sink.add('MongoDb.4.2.sh');

    // Get the port for the WebSocket server from the hybrid isolate.
    var result = await channel.stream.first;

    await channel.sink.close();

    expect(result, equals('hello!'));
  });
}
