import 'package:tinymq/tinymq.dart';

void main(List<String> arguments) {
  final broker = Broker()..createTopic('events', partitions: 2);

  broker.produce('events', 'boot');
  broker.produce('events', 'user:42', key: 'user-42');
  broker.produce('events', 'user:42', key: 'user-42');
  broker.produce('events', 'click', partition: 0);
  broker.produce('events', 'user:7', key: 'user-7');
  broker.produce('events', 'logout', partition: 1);
  broker.produce('events', 'shutdown');

  final consumer = Consumer(broker: broker, topic: 'events', partition: 0);
  final batch = consumer.poll(maxRecords: 10);

  print('partition 0 (${batch.length} records)');
  for (final record in batch) {
    print('offset=${record.offset} key=${record.key ?? "-"} value=${record.value}');
  }

  final partition1 = broker.fetch('events', 1, 0, maxRecords: 10);
  print('partition 1 (${partition1.length} records)');
  for (final record in partition1) {
    print('offset=${record.offset} key=${record.key ?? "-"} value=${record.value}');
  }
}
