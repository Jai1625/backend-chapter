from cassandra.cluster import Cluster
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from flask_sse import sse
import uuid
import json
import datetime
import time
from kafka import KafkaProducer, KafkaConsumer


cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS chatapp
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
""")
session.execute("""
    CREATE TABLE IF NOT EXISTS chatapp.messages (
        id UUID PRIMARY KEY,
        sender text,
        recipient text,
        message text,
        timestamp timestamp,
        room_id text
    );
""")


app = Flask(__name__)
CORS(app)
app.register_blueprint(sse, url_prefix="/stream")

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
consumer = KafkaConsumer('chatapp', bootstrap_servers=['localhost:9092'], group_id='my-group')


@app.route('/messages', methods=['POST'])
def handle_message():
    """
    input format
    {
        "sender": "sender detail ",
        "recipient": "recipient detail ",
        "message": "message to be displayed",
        "id": id,
        "room_id": "room_id"
    }
    :return:
    """
    data = request.get_json()
    id = uuid.UUID(int=data.get('id', 0))
    room_id = data['room_id']
    print(id)
    timestamp = datetime.datetime.now()
    session.execute("""
        INSERT INTO chatapp.messages (id, sender, recipient, message, timestamp, room_id)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (id, data['sender'], data['recipient'], data['message'], timestamp, room_id))
    message = {
        'id': str(id),
        'sender': data['sender'],
        'recipient': data['recipient'],
        'message': data['message'],
        'timestamp': str(timestamp),
        'room_id': room_id
    }
    producer.send('chatapp', json.dumps(message).encode())
    return jsonify({'success': True})


# @app.route('/stream/<room_id>')
# def stream(room_id):
#     print('working')
#
#     def generate():
#         for message in consumer:
#             data = json.loads(message.value.decode())
#             print('working data')
#             print(data)
#             if data['room_id'] == room_id:
#                 response = {
#                     'id': data['id'],
#                     'sender': data['sender'],
#                     'message': data['message'],
#                     'timestamp': data['timestamp'],
#                     'room_id': data['room_id'],
#                 }
#                 yield f"data: {json.dumps(response)}\n\n"
#             time.sleep(1)
#     return Response(generate(), mimetype="text/event-stream", headers={
#         "Cache-Control": "no-cache",
#         "Connection": "keep-alive",
#         "Access-Control-Allow-Origin": "*",
#     })


import threading


@app.route('/stream/<room_id>')
def stream(room_id):
    print('working')
    def generate():
        while True:
            # Poll for new messages from the Kafka consumer
            msg_list = consumer.poll(timeout_ms=10000)
            for tp, messages in msg_list.items():
                for message in messages:
                    data = json.loads(message.value.decode())
                    print('working data')
                    print(data)
                    if data['room_id'] == room_id:
                        response = {
                            'id': data['id'],
                            'sender': data['sender'],
                            'message': data['message'],
                            'timestamp': data['timestamp'],
                            'room_id': data['room_id'],
                        }
                        print(response)
                        yield f"data: {json.dumps(response)}\n\n"

            # Send a heartbeat event to keep the connection alive
            # yield "event: heartbeat\ndata: {}\n\n"
    return Response(generate(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Access-Control-Allow-Origin": "*",
    })


@app.route('/stream/<room_id>/old/<last_message_id>')
def stream_old_message(room_id, last_message_id):
    print('working old messages')

    def generate():
        last_id = uuid.UUID(int=int(last_message_id))
        while True:
            rows = session.execute("""
                SELECT  id, sender, message,  room_id, timestamp
                FROM chatapp.messages
                WHERE id < {} AND room_id = '{}' LIMIT 10
                ALLOW FILTERING
            """.format(last_id, room_id))
            for row in rows:
                last_id = row.id
                message = {
                    'id': str(row.id),
                    'sender': row.sender,
                    'message': row.message,
                    'timestamp': str(row.timestamp)
                }
                yield f"data: {json.dumps(message)}\n\n"
    return Response(generate(), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run(port=5001, debug=True)
