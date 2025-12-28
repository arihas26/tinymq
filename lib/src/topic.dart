import 'errors.dart';
import 'log.dart';

class Topic {
  Topic(this.name, int partitions)
    : _partitions = List<PartitionLog>.generate(
        partitions,
        (index) => PartitionLog(index),
      );

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
