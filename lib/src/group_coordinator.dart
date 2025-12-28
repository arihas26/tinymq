class GroupCoordinator {
  final Map<String, GroupState> _groups = <String, GroupState>{};

  List<int> join({
    required String groupId,
    required String topic,
    required String consumerId,
    required int partitionCount,
  }) {
    final state = _groups.putIfAbsent(
      groupId,
      () => GroupState(topic: topic),
    );
    state.topic = topic;
    state.partitionCount = partitionCount;
    state.members.add(consumerId);
    return state.assignmentFor(consumerId);
  }
}

class GroupState {
  GroupState({required this.topic});

  String topic;
  int partitionCount = 0;
  final Set<String> members = <String>{};

  List<int> assignmentFor(String consumerId) {
    if (partitionCount == 0) {
      return const <int>[];
    }
    final sortedMembers = members.toList()..sort();
    if (!sortedMembers.contains(consumerId)) {
      return const <int>[];
    }
    final assignments = <int>[];
    for (var partition = 0; partition < partitionCount; partition += 1) {
      final owner = sortedMembers[partition % sortedMembers.length];
      if (owner == consumerId) {
        assignments.add(partition);
      }
    }
    return assignments;
  }
}
