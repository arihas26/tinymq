import 'message_record.dart';
import 'errors.dart';
import 'topic.dart';

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
