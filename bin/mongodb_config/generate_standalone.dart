import 'dart:io';

import 'generate_test_instances.dart';
import 'isolate_info.dart';
import 'mongodb_version_config.dart';
import 'run_server.dart';

String createAdminJs = "db.createUser({user: 'admin', pwd: 'admin',"
    "roles: [ { role: 'userAdminAnyDatabase', db: 'admin' },"
    " 'readWriteAnyDatabase' ]})";
String createUserJs =
    "db.getSiblingDB('mongodb-auth').createUser({user: 'test', pwd: 'test',"
    "roles: [ { role: 'readWrite', db: 'mongodb-auth' } ]})";

/// generates a replica set with name "mongo_dart_test"
void generateStandalone(MongoDbVersionConfig config,
    {bool withAuth = false}) async {
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

  // Check data directory
  var dbPathDir = Directory(config.absoluteDbPath);
  if (!await dbPathDir.exists()) {
    throw ArgumentError('The db data path '
        '${config.absoluteDbPath} '
        'for version ${config.versionName} does not exist');
  }

  // standalone data directory (must not exist)
  var dbPathStdDir = Directory('${dbPathDir.path}/${config.versionName}'
      '_mongo_dart_test_std${withAuth ? '_auth' : ''}');
  if (await dbPathStdDir.exists()) {
    throw ArgumentError('The db data path ${dbPathStdDir.path} already exists');
  }
  await dbPathStdDir.create();

  IsolateInfo std;
  if (withAuth) {
    std = await runServer(config, mongodDaemonFile, dbPathStdDir,
        host: '127.0.0.1', port: '27017');

    // let server wake up
    await Future.delayed(Duration(seconds: 5));

    // Create admin
    var result = await Process.run(
        mongoShell.path,
        [
          '127.0.0.1:27017/admin',
          '--eval',
          '$createAdminJs',
        ],
        runInShell: true);

    if (result.exitCode != 0 ||
        (result.stderr != null && result.stderr.isNotEmpty)) {
      print(result.stderr);
      print(result.stdout);
    }
    print('Restarting server');
    await quitServer(std, mongodDaemonFile, dbPathStdDir);

    std = await runServer(config, mongodDaemonFile, dbPathStdDir,
        host: '127.0.0.1', port: '27017', moreParameters: ['--auth']);
    // let server wake up
    await Future.delayed(Duration(seconds: 5));

    // Create user
    result = await Process.run(
        mongoShell.path,
        [
          '127.0.0.1:27017/admin',
          '-u',
          'admin',
          '-p',
          'admin',
          '--eval',
          '$createUserJs',
        ],
        runInShell: true);

    if (result.exitCode != 0 ||
        (result.stderr != null && result.stderr.isNotEmpty)) {
      print(result.stderr);
      print(result.stdout);
    }
  }

  var scriptFile = File('${config.absoluteLaunchScriptPath}/mdt_'
      '${config.versionName}_std${withAuth ? '_auth' : ''}.sh');
  if (!await scriptFile.exists()) {
    await scriptFile.create(recursive: true);
  }
  var scriptContent = StringBuffer('#!/bin/sh\n\n');
  if (withAuth) {
    await addServerToScript(
        scriptContent, mongodDaemonFile, dbPathStdDir, '27017',
        isGnomeDesktop: isGnomeDesktop,
        title: 'Standalone with Auth',
        withAuth: true);
  } else {
    await addServerToScript(
        scriptContent, mongodDaemonFile, dbPathStdDir, '27017',
        isGnomeDesktop: isGnomeDesktop, title: 'Standalone');
  }

  scriptContent.writeln('');
  // Give time to the server to start
  scriptContent.writeln('sleep 7');
  if (isGnomeDesktop) {
    scriptContent.write('gnome-terminal --title "Mongo Shell" -- ');
  }
  scriptContent.writeln('sh -c "${mongoShell.path} '
      '127.0.0.1:27017/${withAuth ? 'mongodb-auth ' : 'mongo-dart '} '
      '${withAuth ? '-u test -p test' : ''}; bash"');

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

  if (withAuth) {
    print('Stopping server 1');
    await quitServer(std, mongodDaemonFile, dbPathStdDir);
  }
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
    {bool isGnomeDesktop = true, String title = '', withAuth = false}) async {
  if (isGnomeDesktop) {
    buffer.write('gnome-terminal --title "$title" -- ');
  }
  buffer.writeln('sh -c "${mongoDbDaemon.path} ${withAuth ? '--auth ' : ''}'
      '--port $port  --dbpath ${dbPath.path} --oplogSize 128; bash"');
}
