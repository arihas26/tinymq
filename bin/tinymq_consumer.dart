import 'dart:async';
import 'dart:io';

import 'package:tinymq/tinymq.dart';

Future<void> main(List<String> arguments) async {
  final client = JsonBrokerClient();
  final topic = 'events';
  final partition = 0;
  final groupId = 'worker-1';
  var nextOffset = 0;

  print('consumer connected to 127.0.0.1:4040');
  print('topic=$topic partition=$partition group=$groupId');

  ProcessSignal.sigint.watch().listen((_) async {
    await client.close();
    exit(0);
  });

  while (true) {
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
      await Future<void>.delayed(const Duration(milliseconds: 500));
      continue;
    }

    for (final record in records) {
      final map = record as Map<String, dynamic>;
      final value = map['value'];
      final offset = map['offset'] as int;
      print('processed offset=$offset value=$value');
      nextOffset = offset + 1;
    }

    final commitResponse = await client.request(<String, dynamic>{
      'type': 'commit',
      'groupId': groupId,
      'topic': topic,
      'partition': partition,
      'offset': nextOffset,
    });
    if (commitResponse['ok'] != true) {
      print('commit error: ${commitResponse['error']}');
    }
  }
}
