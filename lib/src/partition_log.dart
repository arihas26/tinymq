import 'dart:collection';

import 'errors.dart';
import 'log_storage.dart';
import 'message_record.dart';

class PartitionLog {
  PartitionLog(this.partitionId, {LogStorage? storage}) : _storage = storage {
    final loaded = _storage?.load() ?? <MessageRecord>[];
    if (loaded.isNotEmpty) {
      _records.addAll(loaded);
      _baseOffset = _records.first.offset;
      _nextOffset = _records.last.offset + 1;
    }
  }

  final int partitionId;
  final List<MessageRecord> _records = <MessageRecord>[];
  final LogStorage? _storage;
  int _baseOffset = 0;
  int _nextOffset = 0;

  int get size => _records.length;
  int get baseOffset => _baseOffset;
  int get nextOffset => _nextOffset;

  MessageRecord append({required String value, String? key, DateTime? timestamp}) {
    final record = MessageRecord(
      offset: _nextOffset,
      timestamp: timestamp ?? DateTime.now().toUtc(),
      value: value,
      key: key,
    );
    _records.add(record);
    if (_records.length == 1) {
      _baseOffset = record.offset;
    }
    _nextOffset += 1;
    _storage?.append(record);
    return record;
  }

  List<MessageRecord> readFrom(int offset, {int maxRecords = 100}) {
    if (offset < 0) {
      throw InvalidOffset(offset);
    }
    final startIndex = offset - _baseOffset;
    if (startIndex < 0) {
      throw InvalidOffset(offset);
    }
    if (startIndex >= _records.length) {
      return const <MessageRecord>[];
    }
    final end = (startIndex + maxRecords).clamp(0, _records.length);
    return UnmodifiableListView(_records.sublist(startIndex, end));
  }
}
