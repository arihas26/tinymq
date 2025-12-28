class GroupCoordinator {
  GroupCoordinator({this.sessionTimeout = const Duration(seconds: 10)});

  final Duration sessionTimeout;
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
    state.touch(consumerId);
    state.expireOlderThan(DateTime.now().toUtc().subtract(sessionTimeout));
    return state.assignmentFor(consumerId);
  }

  bool heartbeat({
    required String groupId,
    required String consumerId,
  }) {
    final state = _groups[groupId];
    if (state == null) {
      return false;
    }
    state.touch(consumerId);
    state.expireOlderThan(DateTime.now().toUtc().subtract(sessionTimeout));
    return true;
  }

  void leave({
    required String groupId,
    required String consumerId,
  }) {
    final state = _groups[groupId];
    if (state == null) {
      return;
    }
    state.remove(consumerId);
    if (state.members.isEmpty) {
      _groups.remove(groupId);
    }
  }
}

class GroupState {
  GroupState({required this.topic});

  String topic;
  int partitionCount = 0;
  final Map<String, DateTime> members = <String, DateTime>{};

  void touch(String consumerId) {
    members[consumerId] = DateTime.now().toUtc();
  }

  void remove(String consumerId) {
    members.remove(consumerId);
  }

  void expireOlderThan(DateTime threshold) {
    members.removeWhere((_, lastSeen) => lastSeen.isBefore(threshold));
  }

  List<int> assignmentFor(String consumerId) {
    if (partitionCount == 0) {
      return const <int>[];
    }
    final sortedMembers = members.keys.toList()..sort();
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
