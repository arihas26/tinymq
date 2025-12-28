import 'errors.dart';
import 'log_storage.dart';
import 'partition_log.dart';

typedef LogStorageFactory = LogStorage? Function(String topicName, int partitionId);

class Topic {
  Topic(this.name, int partitions, {LogStorageFactory? logStorageFactory})
    : _logStorageFactory = logStorageFactory,
      _partitions = List<PartitionLog>.generate(
        partitions,
        (index) => PartitionLog(
          index,
          storage: logStorageFactory?.call(name, index),
        ),
      );

  final String name;
  final List<PartitionLog> _partitions;
  final LogStorageFactory? _logStorageFactory;
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

  void addPartitions(int count) {
    if (count <= 0) {
      throw ArgumentError.value(count, 'count');
    }
    final start = _partitions.length;
    for (var index = 0; index < count; index += 1) {
      final partitionId = start + index;
      _partitions.add(
        PartitionLog(
          partitionId,
          storage: _logStorageFactory?.call(name, partitionId),
        ),
      );
    }
  }
}
