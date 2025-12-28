import 'dart:async';
import 'dart:convert';
import 'dart:io';

class JsonBrokerClient {
  JsonBrokerClient({this.host = '127.0.0.1', this.port = 4040});

  final String host;
  final int port;
  final List<Completer<Map<String, dynamic>>> _pending =
      <Completer<Map<String, dynamic>>>[];

  Socket? _socket;
  StreamSubscription<String>? _subscription;

  Future<void> connect() async {
    if (_socket != null) {
      return;
    }
    _socket = await Socket.connect(host, port);
    final lines = utf8.decoder
        .bind(_socket!.cast<List<int>>())
        .transform(const LineSplitter());
    _subscription =
        lines.listen(_handleLine, onError: _handleError, onDone: _handleDone);
  }

  Future<void> close() async {
    await _subscription?.cancel();
    _subscription = null;
    await _socket?.close();
    _socket = null;
  }

  Future<Map<String, dynamic>> request(Map<String, dynamic> payload) async {
    await connect();
    final completer = Completer<Map<String, dynamic>>();
    _pending.add(completer);
    _socket!.write('${jsonEncode(payload)}\n');
    return completer.future;
  }

  void _handleLine(String line) {
    if (_pending.isEmpty) {
      return;
    }
    final completer = _pending.removeAt(0);
    try {
      final decoded = jsonDecode(line) as Map<String, dynamic>;
      completer.complete(decoded);
    } catch (error, stackTrace) {
      completer.completeError(error, stackTrace);
    }
  }

  void _handleError(Object error) {
    while (_pending.isNotEmpty) {
      _pending.removeAt(0).completeError(error);
    }
  }

  void _handleDone() {
    _handleError(StateError('Connection closed'));
  }
}
