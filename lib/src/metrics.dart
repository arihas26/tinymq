class PartitionMetrics {
  PartitionMetrics({
    required this.size,
    required this.endOffset,
    required this.lag,
  });

  final int size;
  final int endOffset;
  final int lag;
}
