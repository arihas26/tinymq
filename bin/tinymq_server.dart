import 'dart:io';

import 'package:path/path.dart' as path;
import 'package:tinymq/tinymq.dart';

Future<void> main(List<String> arguments) async {
  final logDirPath = path.join(Directory.current.path, 'data', 'tinymq-demo');
  final logDir = Directory(logDirPath)..createSync(recursive: true);
  print('logDir=${logDir.path}');

  final broker = Broker(logDirectory: logDir.path);
  final server = JsonBrokerServer(broker);

  await server.start(port: 4040);
  print('tinymq server listening on 127.0.0.1:4040');
  print('send JSON per line, e.g.:');
  print('nc 127.0.0.1 4040');
  print('{"type":"createTopic","topic":"events","partitions":2}');
  print('{"type":"produce","topic":"events","value":"hello","key":"k1"}');

  ProcessSignal.sigint.watch().listen((_) async {
    await server.stop();
    exit(0);
  });
}
