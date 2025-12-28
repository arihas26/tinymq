import 'dart:io';

import 'package:path/path.dart' as path;
import 'package:tinymq/tinymq.dart';

void main(List<String> arguments) {
  final logDirPath = path.join(Directory.current.path, 'data', 'tinymq-demo');
  final logDir = Directory(logDirPath)
    ..createSync(recursive: true)
    ..listSync().whereType<File>().forEach((file) => file.deleteSync());
  print('logDir=${logDir.path}');
  final broker =
      Broker(logDirectory: logDir.path)..createTopic('events', partitions: 2);

  broker.produce('events', 'boot');
  broker.produce('events', 'user:42', key: 'user-42');
  broker.produce('events', 'user:42', key: 'user-42');
  broker.produce('events', 'click', partition: 0);
  broker.produce('events', 'user:7', key: 'user-7');
  broker.produce('events', 'logout', partition: 1);
  broker.produce('events', 'shutdown');

  final consumer = Consumer(
    broker: broker,
    topic: 'events',
    partition: 0,
    groupId: 'group-a',
  );
  final batch = consumer.poll(maxRecords: 10);

  print('partition 0 (${batch.length} records)');
  for (final record in batch) {
    print('offset=${record.offset} key=${record.key ?? "-"} value=${record.value}');
  }
  consumer.commit();

  final metrics = broker.partitionMetrics('events', 0, groupId: 'group-a');
  print(
    'metrics p0 size=${metrics.size} end=${metrics.endOffset} lag=${metrics.lag}',
  );

  final partition1 = broker.fetch('events', 1, 0, maxRecords: 10);
  print('partition 1 (${partition1.length} records)');
  for (final record in partition1) {
    print('offset=${record.offset} key=${record.key ?? "-"} value=${record.value}');
  }

  final reloaded =
      Broker(logDirectory: logDir.path)..createTopic('events', partitions: 2);
  final reloadedRecords = reloaded.fetch('events', 0, 0, maxRecords: 10);
  print('reloaded partition 0 (${reloadedRecords.length} records)');
}
