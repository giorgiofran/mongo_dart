import 'dart:io';

import 'generate_test_instances.dart';
import 'isolate_info.dart';
import 'mongodb_version_config.dart';
import 'run_server.dart';

String initiateJs2 = 'printjson(db.getCollectionNames())';
String initiateJs = "rs.initiate( { _id : 'mongo_dart_test', members: ["
    " { _id: 0, host: '127.0.0.1:27017', priority: 1000}, "
    " { _id: 1, host: '127.0.0.1:27018'}, "
    " { _id: 2, host: '127.0.0.1:27019'}]})";

/// generates a replica set with name "mongo_dart_test"
void generateReplicaSet(MongoDbVersionConfig config) async {
  // verifies that the mongodb directory exists and it is a mongoDb installation
  // (this is the parent of the bin directory)
  var installationDir = Directory(config.installationAbsolutePath);
  if (!await installationDir.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} '
        'for version ${config.versionName} does not exist');
  }

  // Check if the mongod daemon exists()
  File mongodDaemonFile;
  File mongoShell;
  if (Platform.isWindows) {
    mongodDaemonFile = File('${installationDir.path}/bin/mongod.exe');
    mongoShell = File('${installationDir.path}/bin/mongo.exe');
  } else {
    mongodDaemonFile = File('${installationDir.path}/bin/mongod');
    mongoShell = File('${installationDir.path}/bin/mongo');
  }
  if (!await mongodDaemonFile.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} does not seem to be a MongoDb '
        'installation (missing bin/mongod daemon)');
  }
  if (!await mongoShell.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} does not seem to be a MongoDb '
        'installation (missing bin/mongo shell executable)');
  }

  // generate the data directories for the instances
  var dbPathDir = Directory(config.absoluteDbPath);
  if (!await dbPathDir.exists()) {
    throw ArgumentError('The db data path '
        '${config.absoluteDbPath} '
        'for version ${config.versionName} does not exist');
  }

  // replicaset data directory (must not exist)
  var dbPathRsDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_rs');
  if (await dbPathRsDir.exists()) {
    throw ArgumentError('The db data path ${dbPathRsDir.path} already exists');
  }
  await dbPathRsDir.create();

  // replicaset 0 data directory (must not exist)
  var dbPathRs0Dir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_rs/${config.versionName}_mongo_dart_test_rs0');
  if (await dbPathRs0Dir.exists()) {
    throw ArgumentError('The db data path ${dbPathRs0Dir.path} already exists');
  }
  await dbPathRs0Dir.create();

  // replicaset 1 data directory (must not exist)
  var dbPathRs1Dir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_rs/${config.versionName}_mongo_dart_test_rs1');
  if (await dbPathRs1Dir.exists()) {
    await dbPathRs0Dir.delete();
    throw ArgumentError('The db data path ${dbPathRs1Dir.path} already exists');
  }
  await dbPathRs1Dir.create();

  // replicaset 2 data directory (must not exist)
  var dbPathRs2Dir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_rs/${config.versionName}_mongo_dart_test_rs2');
  if (await dbPathRs2Dir.exists()) {
    await dbPathRs0Dir.delete();
    await dbPathRs1Dir.delete();
    throw ArgumentError('The db data path ${dbPathRs2Dir.path} already exists');
  }
  await dbPathRs2Dir.create();

  // Start three instances on port 27017, 27018 and 27019 with reference
  // to replica set "mongo_dart_test".
  var rs0 = await runServer(config, mongodDaemonFile, dbPathRs0Dir,
      host: '127.0.0.1',
      port: '27017',
      moreParameters: ['--replSet', 'mongo_dart_test']);
  var rs1 = await runServer(config, mongodDaemonFile, dbPathRs1Dir,
      host: '127.0.0.1',
      port: '27018',
      moreParameters: ['--replSet', 'mongo_dart_test']);
  var rs2 = await runServer(config, mongodDaemonFile, dbPathRs2Dir,
      host: '127.0.0.1',
      port: '27019',
      moreParameters: ['--replSet', 'mongo_dart_test']);

// let servers wake up
  await Future.delayed(Duration(seconds: 5));

  // Initiate replica set
  final result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$initiateJs',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
  }
  print(result.stdout);

  var scriptFile = File(
      '${config.absoluteLaunchScriptPath}/mdt_${config.versionName}_rs.sh');
  if (!await scriptFile.exists()) {
    await scriptFile.create(recursive: true);
  }
  var scriptContent = StringBuffer('#!/bin/sh\n\n');
  await addServerToScript(
      scriptContent, mongodDaemonFile, dbPathRs0Dir, '27017',
      isGnomeDesktop: isGnomeDesktop, title: 'Replica 0');
  await addServerToScript(
      scriptContent, mongodDaemonFile, dbPathRs1Dir, '27018',
      isGnomeDesktop: isGnomeDesktop, title: 'Replica 1');
  await addServerToScript(
      scriptContent, mongodDaemonFile, dbPathRs2Dir, '27019',
      isGnomeDesktop: isGnomeDesktop, title: 'Replica 2');

  scriptContent.writeln('');
  // Give time to the server to start
  scriptContent.writeln('sleep 7');
  if (isGnomeDesktop) {
    scriptContent.write('gnome-terminal --title "Mongo Shell" -- ');
  }
  scriptContent
      .writeln('sh -c "${mongoShell.path} 127.0.0.1:27017/mongo-dart; bash"');

  await scriptFile.writeAsString(
    '$scriptContent',
  );
  // Makes the script executable.
  final resultScript = await Process.run(
      'chmod',
      [
        '+x',
        '${scriptFile.path}',
      ],
      runInShell: true);
  if (resultScript.exitCode != 0 ||
      (resultScript.stderr != null && resultScript.stderr.isNotEmpty)) {
    print(resultScript.stderr);
  }

  // let servers talk each other
  await Future.delayed(Duration(seconds: 5));

  print('Stopping server 1');
  await quitServer(rs0, mongodDaemonFile, dbPathRs0Dir);
  print('Stopping server 2');
  await quitServer(rs1, mongodDaemonFile, dbPathRs1Dir);
  print('Stopping server 3');
  await quitServer(rs2, mongodDaemonFile, dbPathRs2Dir);
}

Future<void> quitServer(
    IsolateInfo isolateInfo, File mongoDbDaemon, Directory dbPath) async {
  var result = await Process.run(
      mongoDbDaemon.path, ['--shutdown', '--dbpath', dbPath.path]);
  if (result.exitCode != 0) {
    if (result.exitCode == 1 &&
        result.stderr.startsWith(
            'There doesn\'t seem to be a server running with dbpath:')) {
      return;
    }
    print('Error trying to quit server');
  }
  //isolateInfo.sendPort.send(['Please close']);
  var returnMessage = await isolateInfo.receivePort.first;
  if (returnMessage is ProcessResult) {
    if (returnMessage.exitCode != 0) {
      if (returnMessage.exitCode == 48) {
        print(returnMessage.stdout);
      } else {
        print(returnMessage.stderr);
      }
    } else {
      print('Terminated server');
    }
  } else if (returnMessage is Error) {
    print('$returnMessage');
  }
  //isolateInfo.isolate.kill();
}

Future<void> addServerToScript(
    StringBuffer buffer, File mongoDbDaemon, Directory dbPath, String port,
    {bool isGnomeDesktop = true, String title = ''}) async {
  if (isGnomeDesktop) {
    buffer.write('gnome-terminal --title "$title" -- ');
  }
  buffer.writeln('sh -c "${mongoDbDaemon.path} --replSet mongo_dart_test '
      '--port $port  --dbpath ${dbPath.path} --oplogSize 128; bash"');
}
