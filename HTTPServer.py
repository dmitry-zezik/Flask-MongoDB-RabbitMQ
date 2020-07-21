from flask import Flask, request
import pika
import uuid
import threading
import json

app = Flask(__name__)
queue = {}


def thread(my_func):
    """ Runs a function in a separate thread. """

    def wrapper(*args, **kwargs):
        my_thread = threading.Thread(target=my_func, args=args, kwargs=kwargs)
        my_thread.start()

    return wrapper


class FMSystem(object):
    """
        This class exists to start and stop the trip tracking microservice. To perform DB CRUD operations
        A thread is created with an exclusive queue. As soon as we get the answer, the operation is completed.

    """

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
            # If we get here - the operation is completed

    @thread
    def call(self, user_dict):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        queue[self.corr_id] = None
        self.channel.basic_publish(
            exchange='',
            routing_key='fm_actions_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=user_dict)
        while self.response is None:
            self.connection.process_data_events()
        queue[self.corr_id] = self.response
        print(self.response)
        return str(self.response)


@app.route("/user_create", methods=['POST'])
def create_user():
    q_dict = request.args.to_dict()

    user_dict = {
        'action': 'create_user',
        'username': '',
        'body': q_dict
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "creating user " + json.dumps(q_dict)

@app.route("/user_read", methods=['GET'])
def read_user():
    username = request.args.get('username', None)

    user_dict = {
        'action': 'read_user',
        'username': username,
        'body': {}
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "reading user " + str(username)

@app.route("/user_update", methods=['POST'])
def update_user():
    q_dict = request.args.to_dict()

    username = q_dict['username']
    del q_dict['username']

    user_dict = {
        'action': 'update_user',
        'username': username,
        'body': q_dict
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "updating user " + str(username)

@app.route("/user_delete", methods=['DELETE'])
def delete_user():
    username = request.args.get('username', None)

    user_dict = {
        'action': 'delete_user',
        'username': username,
        'body': {}
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "deleting user " + str(username)

@app.route("/start_trip", methods=['POST'])
def start_trip():
    q_dict = request.args.to_dict()

    username = q_dict['username']
    title = q_dict['title']
    content = q_dict['content']

    user_dict = {
        'action': 'start_trip',
        'username': username,
        'body': {
            'title': title,
            'content': content,
        }
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "start trip " + str(username)

@app.route("/stop_trip", methods=['GET'])
def stop_trip():
    username = request.args.get('username', None)

    user_dict = {
        'action': 'stop_trip',
        'username': username,
        'body': {}
    }

    fms = FMSystem()
    fms.call(json.dumps(user_dict))
    return "stop trip " + str(username)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9006)