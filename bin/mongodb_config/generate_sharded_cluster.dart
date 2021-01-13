import 'dart:io';

import 'generate_test_instances.dart';
import 'isolate_info.dart';
import 'mongodb_version_config.dart';
import 'run_server.dart';

String simpleInitiate = 'rs.initiate()';

String addShard0 = "sh.addShard('sh0/127.0.0.1:27018');";
String addShard1 = "sh.addShard('sh1/127.0.0.1:27019');";
String enableSharding = "sh.enableSharding('mongo-dart');";
String shardACollection =
    "sh.shardCollection('mongo-dart.test-data', {'test-name': 'hashed'});";
String shutdown = "db.getSiblingDB('admin').shutdownServer()";

/// generates a sharder cluster with name "mongo_dart_test"
void generateShardedCluster(MongoDbVersionConfig config) async {
  // verifies that the mongodb directory exists and it is a mongoDb installation
  // (this is the parent of the bin directory)
  var installationDir = Directory(config.installationAbsolutePath);
  if (!await installationDir.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} '
        'for version ${config.versionName} does not exist');
  }

  // Check if the mongo daemons exists()
  File mongodDaemonFile;
  File mongosDaemonFile;
  File mongoShell;
  if (Platform.isWindows) {
    mongodDaemonFile = File('${installationDir.path}/bin/mongod.exe');
    mongosDaemonFile = File('${installationDir.path}/bin/mongos.exe');
    mongoShell = File('${installationDir.path}/bin/mongo.exe');
  } else {
    mongodDaemonFile = File('${installationDir.path}/bin/mongod');
    mongosDaemonFile = File('${installationDir.path}/bin/mongos');
    mongoShell = File('${installationDir.path}/bin/mongo');
  }
  if (!await mongodDaemonFile.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} does not seem to be a MongoDb '
        'installation (missing bin/mongod daemon)');
  }
  if (!await mongosDaemonFile.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} does not seem to be a MongoDb '
        'installation (missing bin/mongos daemon)');
  }
  if (!await mongoShell.exists()) {
    throw ArgumentError('The installation path '
        '${config.installationAbsolutePath} does not seem to be a MongoDb '
        'installation (missing bin/mongo shell executable)');
  }

  // ******************  DbPath *********************************

  // check the data directories for the instances
  var dbPathDir = Directory(config.absoluteDbPath);
  if (!await dbPathDir.exists()) {
    throw ArgumentError('The db data path '
        '${config.absoluteDbPath} '
        'for version ${config.versionName} does not exist');
  }

  // sharded cluster data directory (must not exist)
  var dbPathShsDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_sh');
  if (await dbPathShsDir.exists()) {
    throw ArgumentError('The db data path ${dbPathShsDir.path} already exists');
  }
  await dbPathShsDir.create();

  // single memeber config replicaset data directory (must not exist)
  var dbPathCfgDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_sh/${config.versionName}_mongo_dart_test_cfg');
  if (await dbPathCfgDir.exists()) {
    throw ArgumentError('The db data path ${dbPathCfgDir.path} already exists');
  }
  await dbPathCfgDir.create();

  // shard 0 single member replicaset data directory (must not exist)
  var dbPathSh0RsDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_sh/${config.versionName}_mongo_dart_test_sh0_rs');
  if (await dbPathSh0RsDir.exists()) {
    await dbPathCfgDir.delete();
    throw ArgumentError(
        'The db data path ${dbPathSh0RsDir.path} already exists');
  }
  await dbPathSh0RsDir.create();

  // shard 1 single member replicaset data directory (must not exist)
  var dbPathSh1RsDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_sh/${config.versionName}_mongo_dart_test_sh1_rs');
  if (await dbPathSh1RsDir.exists()) {
    await dbPathCfgDir.delete();
    await dbPathSh0RsDir.delete();
    throw ArgumentError(
        'The db data path ${dbPathSh1RsDir.path} already exists');
  }
  await dbPathSh1RsDir.create();

  // *************************** servers **********************************
  var cfg = await runServer(config, mongodDaemonFile, dbPathCfgDir,
      host: '127.0.0.1',
      port: '27016',
      moreParameters: ['--configsvr', '--replSet', 'cfg']);

  var sh0 = await runServer(config, mongodDaemonFile, dbPathSh0RsDir,
      host: '127.0.0.1',
      port: '27018',
      moreParameters: ['--shardsvr', '--replSet', 'sh0']);

  var sh1 = await runServer(config, mongodDaemonFile, dbPathSh1RsDir,
      host: '127.0.0.1',
      port: '27019',
      moreParameters: ['--shardsvr', '--replSet', 'sh1']);

  // let servers wake up
  await Future.delayed(Duration(seconds: 3));

  // Initiate config
  var result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27016/mongo-dart',
        '--eval',
        '$simpleInitiate',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }

  // Initiate shard 0
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27018/mongo-dart',
        '--eval',
        '$simpleInitiate',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }

  // Initiate shard 1
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27019/mongo-dart',
        '--eval',
        '$simpleInitiate',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }

  // *********************** mongos ********************
  var mongos = await runServer(config, mongosDaemonFile, null,
      host: '127.0.0.1',
      port: '27017',
      moreParameters: ['--configdb', 'cfg/127.0.0.1:27016']);

  // let server wake up
  await Future.delayed(Duration(seconds: 3));

  // Add shard 0
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$addShard0',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }
  // Add shard 1
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$addShard1',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }
  // Enable sharding on "mongo-dart" database
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$enableSharding',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }

  // Enable sharding on "mongo-dart" database - collection "test-data"
  // on field "test-name"
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$shardACollection',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }
// let servers talk each other
  await Future.delayed(Duration(seconds: 5));

  // Shutdown mongos server
  result = await Process.run(
      mongoShell.path,
      [
        '127.0.0.1:27017/mongo-dart',
        '--eval',
        '$shutdown',
      ],
      runInShell: true);

  if (result.exitCode != 0 ||
      (result.stderr != null && result.stderr.isNotEmpty)) {
    print(result.stderr);
    print(result.stdout);
  }

  // *********************** script file ********************************
  var scriptFile = File(
      '${config.absoluteLaunchScriptPath}/mdt_${config.versionName}_sh.sh');
  if (!await scriptFile.exists()) {
    await scriptFile.create(recursive: true);
  }
  var scriptContent = StringBuffer('#!/bin/sh\n\n');
  await addGnomeTitle(scriptContent, isGnomeDesktop, 'Config');
  scriptContent.writeln('sh -c "${mongodDaemonFile.path} --replSet cfg '
      '--bind_ip localhost '
      '--port 27016  --dbpath ${dbPathCfgDir.path} --configsvr --oplogSize 128'
      '; bash"');
  await addGnomeTitle(scriptContent, isGnomeDesktop, 'Shard 0');
  scriptContent.writeln('sh -c "${mongodDaemonFile.path} --replSet sh0 '
      '--bind_ip localhost '
      '--port 27018  --dbpath ${dbPathSh0RsDir.path} --shardsvr --oplogSize 128'
      '; bash"');
  await addGnomeTitle(scriptContent, isGnomeDesktop, 'Shard 1');
  scriptContent.writeln('sh -c "${mongodDaemonFile.path} --replSet sh1 '
      '--bind_ip localhost '
      '--port 27019  --dbpath ${dbPathSh1RsDir.path} --shardsvr --oplogSize 128'
      '; bash"');
  await addGnomeTitle(scriptContent, isGnomeDesktop, 'Mongos');
  scriptContent.writeln('sh -c "${mongosDaemonFile.path} --bind_ip localhost '
      '--port 27017   --configdb cfg/127.0.0.1:27016'
      '; bash"');

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

  print('Stopping shard 0 server');
  await quitServer(sh0, mongodDaemonFile, dbPathSh0RsDir);
  print('Stopping shard 1 server');
  await quitServer(sh1, mongodDaemonFile, dbPathSh1RsDir);
  print('Stopping cfg server');
  await quitServer(cfg, mongodDaemonFile, dbPathCfgDir);
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

Future<void> addGnomeTitle(
    StringBuffer buffer, bool isGnomeDesktop, String title) async {
  if (isGnomeDesktop) {
    buffer.write('gnome-terminal --title "$title" -- ');
  }
}
