import 'dart:io';

import 'package:test/test.dart';
import 'package:tinymq/tinymq.dart';

void main() {
  test('produce and fetch from explicit partition', () {
    final broker = Broker()..createTopic('events', partitions: 2);
    broker.produce('events', 'hello', partition: 1);
    broker.produce('events', 'world', partition: 1);

    final records = broker.fetch('events', 1, 0, maxRecords: 10);

    expect(records.length, 2);
    expect(records.first.value, 'hello');
    expect(records.last.offset, 1);
  });

  test('consumer advances offset on poll', () {
    final broker = Broker()..createTopic('metrics', partitions: 1);
    broker.produce('metrics', 'm1');
    broker.produce('metrics', 'm2');

    final consumer = Consumer(broker: broker, topic: 'metrics', partition: 0);
    final batch = consumer.poll(maxRecords: 1);

    expect(batch.single.value, 'm1');
    expect(consumer.nextOffset, 1);

    final remaining = consumer.poll(maxRecords: 10);
    expect(remaining.single.value, 'm2');
    expect(consumer.nextOffset, 2);
  });

  test('consumer group resumes from committed offset', () {
    final broker = Broker()..createTopic('stream', partitions: 1);
    broker.produce('stream', 'a');
    broker.produce('stream', 'b');

    final consumer =
        Consumer(broker: broker, topic: 'stream', partition: 0, groupId: 'g1');
    final batch = consumer.poll(maxRecords: 1);
    expect(batch.single.value, 'a');
    consumer.commit();

    final resumed =
        Consumer(broker: broker, topic: 'stream', partition: 0, groupId: 'g1');
    final remaining = resumed.poll(maxRecords: 10);
    expect(remaining.single.value, 'b');
    expect(resumed.nextOffset, 2);
  });

  test('file-backed log reloads records', () {
    final tempDir = Directory.systemTemp.createTempSync('tinymq-');
    addTearDown(() => tempDir.deleteSync(recursive: true));

    final broker = Broker(logDirectory: tempDir.path)
      ..createTopic('events', partitions: 1);
    broker.produce('events', 'alpha');
    broker.produce('events', 'beta');

    final reloaded = Broker(logDirectory: tempDir.path)
      ..createTopic('events', partitions: 1);
    final records = reloaded.fetch('events', 0, 0, maxRecords: 10);

    expect(records.length, 2);
    expect(records.first.value, 'alpha');
  });

  test('broker can add partitions and delete topics', () {
    final broker = Broker()..createTopic('alpha', partitions: 1);
    final count = broker.addPartitions('alpha', 2);

    expect(count, 3);
    expect(broker.deleteTopic('alpha'), isTrue);
    expect(broker.deleteTopic('alpha'), isFalse);
  });

  test('partition metrics include lag', () {
    final broker = Broker()..createTopic('metrics', partitions: 1);
    broker.produce('metrics', 'x');
    broker.produce('metrics', 'y');

    final initial = broker.partitionMetrics('metrics', 0);
    expect(initial.size, 2);
    expect(initial.endOffset, 2);
    expect(initial.lag, 2);

    broker.commitOffset('g1', 'metrics', 0, 1);
    final withGroup =
        broker.partitionMetrics('metrics', 0, groupId: 'g1');
    expect(withGroup.lag, 1);
  });
}
