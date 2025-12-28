class TopicNotFound implements Exception {
  TopicNotFound(this.topic);

  final String topic;

  @override
  String toString() => 'TopicNotFound: $topic';
}

class PartitionNotFound implements Exception {
  PartitionNotFound(this.topic, this.partition);

  final String topic;
  final int partition;

  @override
  String toString() => 'PartitionNotFound: $topic/$partition';
}

class InvalidOffset implements Exception {
  InvalidOffset(this.offset);

  final int offset;

  @override
  String toString() => 'InvalidOffset: $offset';
}
