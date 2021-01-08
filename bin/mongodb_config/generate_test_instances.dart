import 'generate_replica_set.dart';
import 'generate_standalone.dart';
import 'mongodb_version_config.dart';

/// List of versions to be considered for configuration and scripts generation
List<MongoDbVersionConfig> versions = <MongoDbVersionConfig>[
  MongoDbVersionConfig('ver-4.2', '/home/giorgio/mongoDb/mongodb-4.2.2',
      '/mnt/xfs/mongodb/data', '/home/giorgio/mongodb-test-sh')
];

// set this to true if you are running the test in a gnome desktop
// (simply gives a title to the terminal window)
bool isGnomeDesktop = true;

/// Run only on linux - Should be adapted for windows, not tested on Mac
/// Configure the environment for test generating shell scripts
/// that start the required configuration.
/// This script mut be started before running the script
/// The typology of servers generated are:
/// - Standalone (1 server)
/// - Standalone with auth (1 server)
/// - Replica Set - single server (1 server)
/// - Replica Set - single server + Tls (1 server)
/// - Replica Set - standard three server environment (3 servers)
/// - Sharded cluster (8 servers)
///
/// Each script starts the required server(s) + a mongo shell session
///
/// To simplify test management, the port of the first server (or mongos) is
/// always 20017
///
/// The sharded cluster is created with 2 shards each of 3 servers.
/// Only 1 config server is run (not suitable for production,
/// but we are only testing).
///
/// As we are managing different version of MongoDb at the same time,
/// I suggest the following:
/// - create a specifi folder in your home directory (ex. mongodb)
/// - download compressed (normally tgz) versions of mongodb and
///   uncompress in the this folder
/// - run this program to generate the scripts.
///
/// Scripts are preferably generated in a different folder.
Future<void> main() async {
  for (var config in versions) {
    // Create Standalone
    try {
      await generateStandalone(config);
    } catch (e) {
      print('Could not generate standalone environment '
          'for version "${config.versionName}"');
    }

    // Create Standalone with auth
    try {
      await generateStandalone(config, withAuth: true);
    } catch (e) {
      print('Could not generate standalone with auth environment '
          'for version "${config.versionName}"');
    }

    // Create Single Server Replica set
    // Todo to be done yet

    // Create Single Server Replica set with tls
    // Todo to be done yet

    /// Create Replica Set
    /// For recreate a configuration is needed to delete the replica set data
    /// folder. This will delete all your data (but it should be only test data)
    try {
      await generateReplicaSet(config);
    } catch (e) {
      print('Could not generate replica set environment '
          'for version "${config.versionName}"');
    }

    // Create Sharded Cluster
    // Todo to be done yet

  }
}
