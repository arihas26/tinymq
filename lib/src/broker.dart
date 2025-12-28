import 'package:path/path.dart' as path;

import 'errors.dart';
import 'group_coordinator.dart';
import 'log_storage.dart';
import 'message_record.dart';
import 'metrics.dart';
import 'offset_store.dart';
import 'topic.dart';

class Broker {
  Broker({this.logDirectory});

  final Map<String, Topic> _topics = <String, Topic>{};
  final OffsetStore _offsetStore = OffsetStore();
  final GroupCoordinator _groupCoordinator = GroupCoordinator();
  final String? logDirectory;

  Iterable<String> get topics => _topics.keys;

  bool deleteTopic(String name) {
    return _topics.remove(name) != null;
  }

  Topic createTopic(String name, {int partitions = 1}) {
    if (partitions <= 0) {
      throw ArgumentError.value(partitions, 'partitions');
    }
    final topic = Topic(
      name,
      partitions,
      logStorageFactory: _logStorageFactory(),
    );
    _topics[name] = topic;
    return topic;
  }

  int addPartitions(String topicName, int count) {
    final topic = _topicOrThrow(topicName);
    topic.addPartitions(count);
    return topic.partitionCount;
  }

  MessageRecord produce(
    String topicName,
    String value, {
    String? key,
    int? partition,
    DateTime? timestamp,
  }) {
    final topic = _topicOrThrow(topicName);
    final targetPartition = partition ?? topic.choosePartition(key: key);
    return topic.partition(targetPartition).append(value: value, key: key, timestamp: timestamp);
  }

  List<MessageRecord> fetch(String topicName, int partition, int offset, {int maxRecords = 100}) {
    final topic = _topicOrThrow(topicName);
    return topic.partition(partition).readFrom(offset, maxRecords: maxRecords);
  }

  int endOffset(String topicName, int partition) {
    final topic = _topicOrThrow(topicName);
    return topic.partition(partition).nextOffset;
  }

  int partitionSize(String topicName, int partition) {
    final topic = _topicOrThrow(topicName);
    return topic.partition(partition).size;
  }

  PartitionMetrics partitionMetrics(
    String topicName,
    int partition, {
    String? groupId,
  }) {
    final topic = _topicOrThrow(topicName);
    final log = topic.partition(partition);
    final beginOffset = log.baseOffset;
    final endOffset = log.nextOffset;
    final committed = groupId == null
        ? null
        : _offsetStore.getOffset(groupId, topicName, partition);
    final lag = endOffset - (committed ?? 0);
    return PartitionMetrics(
      beginOffset: beginOffset,
      size: log.size,
      endOffset: endOffset,
      lag: lag,
    );
  }

  List<int> joinGroup(
    String groupId,
    String topicName,
    String consumerId,
  ) {
    final topic = _topicOrThrow(topicName);
    return _groupCoordinator.join(
      groupId: groupId,
      topic: topicName,
      consumerId: consumerId,
      partitionCount: topic.partitionCount,
    );
  }

  void leaveGroup(String groupId, String consumerId) {
    _groupCoordinator.leave(groupId: groupId, consumerId: consumerId);
  }

  int? committedOffset(String groupId, String topicName, int partition) {
    _topicOrThrow(topicName).partition(partition);
    return _offsetStore.getOffset(groupId, topicName, partition);
  }

  void commitOffset(String groupId, String topicName, int partition, int offset) {
    _topicOrThrow(topicName).partition(partition);
    _offsetStore.commit(groupId, topicName, partition, offset);
  }

  Topic _topicOrThrow(String name) {
    final topic = _topics[name];
    if (topic == null) {
      throw TopicNotFound(name);
    }
    return topic;
  }

  LogStorageFactory? _logStorageFactory() {
    final root = logDirectory;
    if (root == null) {
      return null;
    }
    return (String topicName, int partitionId) {
      final filePath =
          path.join(root, topicName, 'partition-$partitionId.log');
      return FileLogStorage(filePath);
    };
  }
}
