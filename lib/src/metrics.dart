class PartitionMetrics {
  PartitionMetrics({
    required this.beginOffset,
    required this.committedOffset,
    required this.size,
    required this.endOffset,
    required this.lag,
  });

  final int beginOffset;
  final int? committedOffset;
  final int size;
  final int endOffset;
  final int lag;
}
