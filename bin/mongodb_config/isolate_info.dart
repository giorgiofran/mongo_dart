import 'dart:isolate';

class IsolateInfo {
  final Isolate isolate;
  final SendPort sendPort;
  final ReceivePort receivePort;

  IsolateInfo({this.isolate, this.sendPort, this.receivePort});
}
