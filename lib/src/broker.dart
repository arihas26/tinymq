import 'errors.dart';
import 'log.dart';

class Topic {
  Topic(this.name, int partitions)
    : _partitions = List<PartitionLog>.generate(partitions, (index) => PartitionLog(index));

  final String name;
  final List<PartitionLog> _partitions;
  int _roundRobin = 0;

  int get partitionCount => _partitions.length;

  PartitionLog partition(int id) {
    if (id < 0 || id >= _partitions.length) {
      throw PartitionNotFound(name, id);
    }
    return _partitions[id];
  }

  int choosePartition({String? key}) {
    if (key == null || key.isEmpty) {
      final choice = _roundRobin % _partitions.length;
      _roundRobin += 1;
      return choice;
    }
    return key.hashCode.abs() % _partitions.length;
  }
}

class Broker {
  final Map<String, Topic> _topics = <String, Topic>{};

  Iterable<String> get topics => _topics.keys;

  Topic createTopic(String name, {int partitions = 1}) {
    if (partitions <= 0) {
      throw ArgumentError.value(partitions, 'partitions');
    }
    final topic = Topic(name, partitions);
    _topics[name] = topic;
    return topic;
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

  Topic _topicOrThrow(String name) {
    final topic = _topics[name];
    if (topic == null) {
      throw TopicNotFound(name);
    }
    return topic;
  }
}
