import 'broker.dart';
import 'message_record.dart';

class Consumer {
  Consumer({
    required this.broker,
    required this.topic,
    required this.partition,
    int initialOffset = 0,
  }) : _nextOffset = initialOffset;

  final Broker broker;
  final String topic;
  final int partition;
  int _nextOffset;

  int get nextOffset => _nextOffset;

  List<MessageRecord> poll({int maxRecords = 100}) {
    final records = broker.fetch(topic, partition, _nextOffset, maxRecords: maxRecords);
    if (records.isNotEmpty) {
      _nextOffset = records.last.offset + 1;
    }
    return records;
  }

  void seek(int offset) {
    _nextOffset = offset;
  }
}
