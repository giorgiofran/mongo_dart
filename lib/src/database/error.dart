part of mongo_dart;

class MongoDartError extends Error {
  final String message;
  final String errorCode;

  MongoDartError(this.message, {this.errorCode});
  
  @override
  String toString() => 'MongoDart Error: $message';
}
