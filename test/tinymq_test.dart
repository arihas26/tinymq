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
}
