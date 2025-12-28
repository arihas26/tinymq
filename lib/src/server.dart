import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'broker.dart';
import 'message_record.dart';

class JsonBrokerServer {
  JsonBrokerServer(this.broker);

  final Broker broker;
  ServerSocket? _server;

  Future<void> start({String host = '127.0.0.1', int port = 4040}) async {
    _server = await ServerSocket.bind(host, port);
    _server!.listen(_handleClient);
  }

  Future<void> stop() async {
    await _server?.close();
    _server = null;
  }

  void _handleClient(Socket socket) {
    final lines = utf8.decoder
        .bind(socket.cast<List<int>>())
        .transform(const LineSplitter());
    lines.listen(
          (line) {
            if (line.trim().isEmpty) {
              return;
            }
            Map<String, dynamic> response;
            try {
              final request = jsonDecode(line) as Map<String, dynamic>;
              response = _dispatch(request);
            } catch (error) {
              response = <String, dynamic>{'ok': false, 'error': error.toString()};
            }
            socket.write('${jsonEncode(response)}\n');
          },
          onError: (_) => socket.destroy(),
          onDone: socket.destroy,
        );
  }

  Map<String, dynamic> _dispatch(Map<String, dynamic> request) {
    final type = request['type'] as String?;
    if (type == null) {
      return _error('Missing type');
    }
    switch (type) {
      case 'createTopic':
        return _createTopic(request);
      case 'produce':
        return _produce(request);
      case 'fetch':
        return _fetch(request);
      case 'commit':
        return _commit(request);
      case 'metrics':
        return _metrics(request);
      case 'listTopics':
        return _listTopics();
      case 'joinGroup':
        return _joinGroup(request);
      default:
        return _error('Unknown type: $type');
    }
  }

  Map<String, dynamic> _createTopic(Map<String, dynamic> request) {
    final name = request['topic'] as String?;
    final partitions = (request['partitions'] as int?) ?? 1;
    if (name == null || name.isEmpty) {
      return _error('Missing topic');
    }
    broker.createTopic(name, partitions: partitions);
    return _ok(<String, dynamic>{'topic': name, 'partitions': partitions});
  }

  Map<String, dynamic> _produce(Map<String, dynamic> request) {
    final topic = request['topic'] as String?;
    final value = request['value'] as String?;
    if (topic == null || value == null) {
      return _error('Missing topic or value');
    }
    final record = broker.produce(
      topic,
      value,
      key: request['key'] as String?,
      partition: request['partition'] as int?,
    );
    return _ok(_recordToJson(record));
  }

  Map<String, dynamic> _fetch(Map<String, dynamic> request) {
    final topic = request['topic'] as String?;
    final partition = request['partition'] as int?;
    final offset = request['offset'] as int?;
    if (topic == null || partition == null || offset == null) {
      return _error('Missing topic/partition/offset');
    }
    final max = (request['max'] as int?) ?? 100;
    final records = broker.fetch(topic, partition, offset, maxRecords: max);
    return _ok(<String, dynamic>{'records': records.map(_recordToJson).toList()});
  }

  Map<String, dynamic> _commit(Map<String, dynamic> request) {
    final groupId = request['groupId'] as String?;
    final topic = request['topic'] as String?;
    final partition = request['partition'] as int?;
    final offset = request['offset'] as int?;
    if (groupId == null || topic == null || partition == null || offset == null) {
      return _error('Missing groupId/topic/partition/offset');
    }
    broker.commitOffset(groupId, topic, partition, offset);
    return _ok(<String, dynamic>{'groupId': groupId});
  }

  Map<String, dynamic> _metrics(Map<String, dynamic> request) {
    final topic = request['topic'] as String?;
    final partition = request['partition'] as int?;
    if (topic == null || partition == null) {
      return _error('Missing topic/partition');
    }
    final groupId = request['groupId'] as String?;
    final metrics = broker.partitionMetrics(topic, partition, groupId: groupId);
    return _ok(<String, dynamic>{
      'beginOffset': metrics.beginOffset,
      'size': metrics.size,
      'endOffset': metrics.endOffset,
      'lag': metrics.lag,
    });
  }

  Map<String, dynamic> _listTopics() {
    return _ok(<String, dynamic>{'topics': broker.topics.toList()});
  }

  Map<String, dynamic> _joinGroup(Map<String, dynamic> request) {
    final groupId = request['groupId'] as String?;
    final topic = request['topic'] as String?;
    final consumerId = request['consumerId'] as String?;
    if (groupId == null || topic == null || consumerId == null) {
      return _error('Missing groupId/topic/consumerId');
    }
    final partitions = broker.joinGroup(groupId, topic, consumerId);
    return _ok(<String, dynamic>{'partitions': partitions});
  }

  Map<String, dynamic> _ok(Map<String, dynamic> data) {
    return <String, dynamic>{'ok': true, 'data': data};
  }

  Map<String, dynamic> _error(String message) {
    return <String, dynamic>{'ok': false, 'error': message};
  }

  Map<String, dynamic> _recordToJson(MessageRecord record) {
    return <String, dynamic>{
      'offset': record.offset,
      'timestamp': record.timestamp.toIso8601String(),
      'value': record.value,
      'key': record.key,
    };
  }
}
