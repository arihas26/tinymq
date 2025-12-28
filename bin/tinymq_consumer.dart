import 'dart:async';
import 'dart:io';

import 'package:tinymq/tinymq.dart';

Future<void> main(List<String> arguments) async {
  final client = JsonBrokerClient();
  final topic = arguments.isNotEmpty ? arguments[0] : 'events';
  final groupId = arguments.length > 1 ? arguments[1] : 'group-a';
  final consumerId = arguments.length > 2 ? arguments[2] : 'consumer-1';
  final offsets = <int, int>{};
  var assignments = <int>[];

  print('consumer connected to 127.0.0.1:4040');
  print('topic=$topic group=$groupId consumerId=$consumerId');

  Future<bool> refreshAssignments() async {
    final joinResponse = await client.request(<String, dynamic>{
      'type': 'joinGroup',
      'groupId': groupId,
      'topic': topic,
      'consumerId': consumerId,
    });
    if (joinResponse['ok'] != true) {
      print('joinGroup error: ${joinResponse['error']}');
      return false;
    }
    final joinData = joinResponse['data'] as Map<String, dynamic>;
    final partitions = (joinData['partitions'] as List<dynamic>).cast<int>();
    if (!_sameAssignments(assignments, partitions)) {
      final previous = assignments;
      assignments = partitions;
      print('assigned partitions: $assignments');
      final previousSet = previous.toSet();
      final currentSet = assignments.toSet();
      for (final removed in previousSet.difference(currentSet)) {
        offsets.remove(removed);
      }
      for (final partition in currentSet.difference(previousSet)) {
        final response = await client.request(<String, dynamic>{
          'type': 'metrics',
          'topic': topic,
          'partition': partition,
          'groupId': groupId,
        });
        if (response['ok'] != true) {
          print('metrics error: ${response['error']}');
          continue;
        }
        final data = response['data'] as Map<String, dynamic>;
        final committed = data['committedOffset'] as int?;
        final begin = data['beginOffset'] as int;
        offsets[partition] = committed ?? begin;
      }
    }
    return true;
  }

  if (!await refreshAssignments()) {
    await client.close();
    return;
  }

  ProcessSignal.sigint.watch().listen((_) async {
    await client.request(<String, dynamic>{
      'type': 'leaveGroup',
      'groupId': groupId,
      'consumerId': consumerId,
    });
    await client.close();
    exit(0);
  });

  var lastJoin = DateTime.now().toUtc();
  var lastHeartbeat = DateTime.now().toUtc();
  while (true) {
    final now = DateTime.now().toUtc();
    if (now.difference(lastHeartbeat) > const Duration(seconds: 2)) {
      final heartbeat = await client.request(<String, dynamic>{
        'type': 'heartbeat',
        'groupId': groupId,
        'consumerId': consumerId,
      });
      if (heartbeat['ok'] != true) {
        await refreshAssignments();
        lastJoin = now;
      }
      lastHeartbeat = now;
    }
    if (now.difference(lastJoin) > const Duration(seconds: 6)) {
      await refreshAssignments();
      lastJoin = now;
    }
    var processedAny = false;
    for (final partition in assignments) {
      final nextOffset = offsets[partition] ?? 0;
      final response = await client.request(<String, dynamic>{
        'type': 'fetch',
        'topic': topic,
        'partition': partition,
        'offset': nextOffset,
        'max': 10,
      });

      if (response['ok'] != true) {
        print('fetch error: ${response['error']}');
        await Future<void>.delayed(const Duration(seconds: 1));
        continue;
      }

      final data = response['data'] as Map<String, dynamic>;
      final records = data['records'] as List<dynamic>;
      if (records.isEmpty) {
        continue;
      }

      processedAny = true;
      for (final record in records) {
        final map = record as Map<String, dynamic>;
        final value = map['value'];
        final offset = map['offset'] as int;
        print('processed partition=$partition offset=$offset value=$value');
        offsets[partition] = offset + 1;
      }

      final commitResponse = await client.request(<String, dynamic>{
        'type': 'commit',
        'groupId': groupId,
        'topic': topic,
        'partition': partition,
        'offset': offsets[partition],
      });
      if (commitResponse['ok'] != true) {
        print('commit error: ${commitResponse['error']}');
      }
    }

    if (!processedAny) {
      await Future<void>.delayed(const Duration(milliseconds: 500));
    }
  }
}

bool _sameAssignments(List<int> a, List<int> b) {
  if (a.length != b.length) {
    return false;
  }
  for (var i = 0; i < a.length; i += 1) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}
