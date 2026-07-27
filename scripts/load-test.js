import ws from 'k6/ws';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '30s', target: 500 },  // Ramp up to 500 concurrent users
    { duration: '1m',  target: 1000 }, // Ramp up to 1000 concurrent users
    { duration: '2m',  target: 1000 }, // Hold load at 1000 active users
    { duration: '30s', target: 0 },    // Ramp down to 0
  ],
  thresholds: {
    'ws_connecting': ['p(95)<1000'],   // 95% of connections established within 1s
  },
};

export default function () {
  const serverUrl = __ENV.WS_SERVER_URL || 'ws://localhost:8081';
  const docId = `doc_${(Math.floor(Math.random() * 10) + 1)}`; // Distribute users across 10 documents
  const userId = `user_${__VU}_${__ITER}`;
  const url = `${serverUrl}/${docId}?userId=${userId}`;

  const params = { tags: { scenario: 'presence_load_test' } };

  const res = ws.connect(url, params, function (socket) {
    socket.on('open', function () {
      // Send initial join_document event
      socket.send(JSON.stringify({
        type: 'join_document',
        documentId: docId,
        userId: userId,
      }));

      // Periodic heartbeat every 10 seconds
      socket.setInterval(function () {
        socket.send(JSON.stringify({
          type: 'heartbeat',
          documentId: docId,
          userId: userId,
        }));
      }, 10000);

      // Periodic cursor movement every 5 seconds
      socket.setInterval(function () {
        socket.send(JSON.stringify({
          type: 'cursor_move',
          documentId: docId,
          userId: userId,
          position: {
            line: Math.floor(Math.random() * 100) + 1,
            column: Math.floor(Math.random() * 80) + 1,
          },
        }));
      }, 5000);

      // Periodic typing indicator every 8 seconds
      socket.setInterval(function () {
        socket.send(JSON.stringify({
          type: 'typing_start',
          documentId: docId,
          userId: userId,
          status: true,
        }));
      }, 8000);
    });

    socket.on('message', function (data) {
      check(data, { 'message received': (d) => d.length > 0 });
    });

    socket.setTimeout(function () {
      socket.close();
    }, 45000);
  });

  check(res, { 'connected successfully': (r) => r && r.status === 101 });
}
