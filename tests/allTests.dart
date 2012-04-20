#library("all_tests");
#import("ConnectionTest.dart",prefix:"connection");
#import("CursorTest.dart",prefix:"cursor");
#import("DbCommandTest.dart",prefix:"dbcommand");
#import("DbCollectionTest.dart",prefix:"dbcollection");
#import("DbTest.dart",prefix:"dbtest");
#import("bson/allBsonTests.dart",prefix:"bson");
#import("SelectorBuilderTest.dart",prefix:"helper");
main(){
  bson.main();
  connection.main();
  cursor.main();
  dbcommand.main();
  dbcollection.main();
  dbtest.main();
  helper.main();  
}