class MessageRecord {
  MessageRecord({
    required this.offset,
    required this.timestamp,
    required this.value,
    this.key,
  });

  final int offset;
  final DateTime timestamp;
  final String value;
  final String? key;
}
