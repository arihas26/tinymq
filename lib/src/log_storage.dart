import 'dart:convert';
import 'dart:io';

import 'message_record.dart';

abstract class LogStorage {
  List<MessageRecord> load();
  void append(MessageRecord record);
}

class FileLogStorage implements LogStorage {
  FileLogStorage(this.path);

  final String path;

  @override
  List<MessageRecord> load() {
    final file = File(path);
    if (!file.existsSync()) {
      return <MessageRecord>[];
    }
    final records = <MessageRecord>[];
    final lines = file.readAsLinesSync();
    for (final line in lines) {
      if (line.trim().isEmpty) {
        continue;
      }
      final map = jsonDecode(line) as Map<String, dynamic>;
      records.add(
        MessageRecord(
          offset: map['offset'] as int,
          timestamp: DateTime.parse(map['timestamp'] as String),
          value: map['value'] as String,
          key: map['key'] as String?,
        ),
      );
    }
    return records;
  }

  @override
  void append(MessageRecord record) {
    final file = File(path);
    file.parent.createSync(recursive: true);
    final payload = jsonEncode(<String, dynamic>{
      'offset': record.offset,
      'timestamp': record.timestamp.toIso8601String(),
      'value': record.value,
      'key': record.key,
    });
    file.writeAsStringSync('$payload\n', mode: FileMode.append, flush: true);
  }
}
