import 'broker.dart';
import 'message_record.dart';

class Consumer {
  Consumer({
    required this.broker,
    required this.topic,
    required this.partition,
    this.groupId,
    int? initialOffset,
  }) : _nextOffset = initialOffset ??
            (groupId == null
                ? 0
                : broker.committedOffset(groupId, topic, partition) ?? 0);

  final Broker broker;
  final String topic;
  final int partition;
  final String? groupId;
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

  void seekToBeginning() {
    _nextOffset = 0;
  }

  void seekToEnd() {
    _nextOffset = broker.endOffset(topic, partition);
  }

  int? committedOffset() {
    final id = groupId;
    if (id == null) {
      return null;
    }
    return broker.committedOffset(id, topic, partition);
  }

  void seekToCommitted({int fallback = 0}) {
    final id = groupId;
    if (id == null) {
      throw StateError('Consumer has no groupId');
    }
    _nextOffset = broker.committedOffset(id, topic, partition) ?? fallback;
  }

  void commit() {
    final id = groupId;
    if (id == null) {
      throw StateError('Consumer has no groupId');
    }
    broker.commitOffset(id, topic, partition, _nextOffset);
  }
}
