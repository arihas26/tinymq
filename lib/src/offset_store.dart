class OffsetStore {
  final Map<String, Map<String, Map<int, int>>> _offsets =
      <String, Map<String, Map<int, int>>>{};

  int? getOffset(String groupId, String topic, int partition) {
    return _offsets[groupId]?[topic]?[partition];
  }

  void commit(String groupId, String topic, int partition, int offset) {
    _offsets.putIfAbsent(groupId, () => <String, Map<int, int>>{});
    final topicOffsets =
        _offsets[groupId]!.putIfAbsent(topic, () => <int, int>{});
    topicOffsets[partition] = offset;
  }
}
