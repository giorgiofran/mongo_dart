class MongoDbVersionConfig {
  final String versionName;
  final String installationAbsolutePath;
  final String absoluteDbPath;
  final String absoluteLaunchScriptPath;
  MongoDbVersionConfig(this.versionName, this.installationAbsolutePath,
      this.absoluteDbPath, this.absoluteLaunchScriptPath);
}
