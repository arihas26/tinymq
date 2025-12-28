import 'dart:collection';

import 'errors.dart';
import 'log_storage.dart';
import 'message_record.dart';

class PartitionLog {
  PartitionLog(this.partitionId, {LogStorage? storage}) : _storage = storage {
    final loaded = _storage?.load() ?? <MessageRecord>[];
    if (loaded.isNotEmpty) {
      _records.addAll(loaded);
      _nextOffset = _records.last.offset + 1;
    }
  }

  final int partitionId;
  final List<MessageRecord> _records = <MessageRecord>[];
  final LogStorage? _storage;
  int _nextOffset = 0;

  int get size => _records.length;
  int get nextOffset => _nextOffset;

  MessageRecord append({required String value, String? key, DateTime? timestamp}) {
    final record = MessageRecord(
      offset: _nextOffset,
      timestamp: timestamp ?? DateTime.now().toUtc(),
      value: value,
      key: key,
    );
    _records.add(record);
    _nextOffset += 1;
    _storage?.append(record);
    return record;
  }

  List<MessageRecord> readFrom(int offset, {int maxRecords = 100}) {
    if (offset < 0) {
      throw InvalidOffset(offset);
    }
    if (offset >= _records.length) {
      return const <MessageRecord>[];
    }
    final end = (offset + maxRecords).clamp(0, _records.length);
    return UnmodifiableListView(_records.sublist(offset, end));
  }
}
