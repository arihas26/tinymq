# TinyMQ

Tiny message queue inspired by Kafka.

## Simple system diagram

```mermaid
flowchart LR
  Producer((Producer)) -->|produce| Broker[Broker]
  Broker --> Topic[Topic]
  Topic --> P0[Partition 0 Log]
  Topic --> P1[Partition 1 Log]
  P0 <-->|fetch/poll| Consumer0((Consumer))
  P1 <-->|fetch/poll| Consumer1((Consumer))
```

## JSON over TCP (learning mode)

Run the server:

```bash
dart run bin/tinymq_server.dart
```

Connect with netcat:

```bash
nc 127.0.0.1 4040
```

Requests are one-line JSON objects. Responses are JSON with `ok` and `data` or `error`.

Create topic:

```json
{"type":"createTopic","topic":"events","partitions":2}
```

Produce:

```json
{"type":"produce","topic":"events","value":"hello","key":"k1"}
```

Fetch:

```json
{"type":"fetch","topic":"events","partition":0,"offset":0,"max":10}
```

Commit:

```json
{"type":"commit","groupId":"g1","topic":"events","partition":0,"offset":1}
```

Metrics:

```json
{"type":"metrics","topic":"events","partition":0,"groupId":"g1"}
```

List topics:

```json
{"type":"listTopics"}
```

Sample session (copy/paste line by line into `nc`):

```json
{"type":"createTopic","topic":"events","partitions":2}
{"type":"produce","topic":"events","value":"boot"}
{"type":"produce","topic":"events","value":"user:42","key":"user-42"}
{"type":"produce","topic":"events","value":"click","partition":0}
{"type":"fetch","topic":"events","partition":0,"offset":0,"max":10}
{"type":"commit","groupId":"g1","topic":"events","partition":0,"offset":3}
{"type":"metrics","topic":"events","partition":0,"groupId":"g1"}
{"type":"listTopics"}
```

## Consumer process (learning mode)

Run the broker server:

```bash
dart run bin/tinymq_server.dart
```

Run a separate consumer process:

```bash
dart run bin/tinymq_consumer.dart
```

The consumer joins a group and polls its assigned partitions.

Multiple consumers (same group, different consumerId):

```bash
dart run bin/tinymq_consumer.dart events group-a consumer-1
dart run bin/tinymq_consumer.dart events group-a consumer-2
```

Consumers with different groups will each receive all partitions.
