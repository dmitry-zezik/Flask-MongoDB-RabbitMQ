import os
import json
import pika
import threading
import random
import time
import uuid
from datetime import datetime
from mongoengine import *

activeTrips = {}


def thread(my_func):
    """ Runs a function in a separate thread. """

    def wrapper(*args, **kwargs):
        my_thread = threading.Thread(target=my_func, args=args, kwargs=kwargs)
        my_thread.start()

    return wrapper

@thread
def on_request(ch, method, props, body):
    user_dict = json.loads(body)
    user_name = user_dict['username']
    user_body = user_dict['body']
    action = user_dict['action']

    print(f'Starting DB {action} action')

    if action == 'create_user':
        try:
            print("user_body: ", user_body)
            response = create_user(user_body)
        except Exception as e:
            print(e)
            response = "Oops. Failed to create user"
    elif action == 'read_user':
        try:
            response = read_user(user_body, user_name)
        except Exception as e:
            print(e)
            response = "Oops. Failed to get user data"
    elif action == 'update_user':
        try:
            response = update_user(user_body, user_name)
        except Exception as e:
            print(e)
            response = "Oops. Failed to update user"
    elif action == 'delete_user':
        try:
            response = delete_user(user_body, user_name)
        except Exception as e:
            print(e)
            response = "Oops. Failed to delete user"
    elif action == 'start_trip':
        try:
            response = start_trip(user_name, user_body, props.correlation_id)
        except Exception as e:
            print(e)
            response = "Oops. Failed to start trip"
    elif action == 'stop_trip':
        try:
            trip_id = user_body['trip_id']
            response = stop_trip(user_name, trip_id)
        except Exception as e:
            print(e)
            response = "Oops. Failed to stop trip"

    print(f'Ended DB {action} action. Response: {response}')

    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id=props.correlation_id),
                     body=str(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


# *** FMS METHODS ***
def create_user(body):
    User(
        username=body['username'],
        email=body['email'],
        password=body['password'], # Dev only! Do not store the password in the unencrypted form. Store as a hash.
        age=body['age'],
        car=body['car'],
        driving_exp=body['driving_exp']
    ).save()
    return f'User {body["username"]} was created successfully!'

def read_user(body, name):
    try:
        user = User.objects(username=name).get()
        user_dict = {
            'username': user['username'],
            'email': user['email'],
            'password': user['password'],
            'age': user['age'],
            'car': user['car'],
            'driving_exp': user['driving_exp'],
            'date_created': user['date_created']
        }
        response = user_dict
    except:
        response = 'Oops. Something went wrong'
    return response

def update_user(body, name):
    try:
        user = User.objects(username=name).get()
        email = user['email']
        password = user['password']
        age = user['age']
        car = user['car']
        driving_exp = user['driving_exp']

        print("body:", body)

        for item in body.items():
            if item[0] == 'email':
                email = item[1]
            elif item[0] == 'password':
                password = item[1]
            elif item[0] == 'age':
                age = item[1]
            elif item[0] == 'car':
                car = item[1]
            elif item[0] == 'driving_exp':
                driving_exp = item[1]

        user.update(
            email=email,
            password=password,
            age=age,
            car=car,
            driving_exp=driving_exp
        )
        response = f'User {name} was updated successfully!'
    except Exception as e:
        print(e)
        response = 'Oops. Something went wrong'
    return response

def delete_user(body, name):
    try:
        user = User.objects(username=name).get()
        user.delete()
        response = f'User {name} was deleted!'
    except DoesNotExist:
        response = f'User {name} not found!'
    return response

def start_trip(name, body, trip_id):
    trip = HeartBeatTrips()
    trip.initTrip(name, body, trip_id)
    return f'User {name} has started trip!'

def stop_trip(name, trip_id):
    global activeTrips
    activeTrips[trip_id]['running'] = False
    # write to DB
    return f'User {name} has stopped trip!'


# *** TRIP CLASSES ***
class HeartBeatTrips():

    @thread
    def initTrip(self, name, body, trip_id):
        global activeTrips

        self.trip_id = trip_id
        self.username = name
        self.title = body['title']
        self.content = body['content']

        activeTrips[trip_id] = {
            'running': True,
            'username': name,
            'title': self.title,
            'content': self.content
        }

        try:
            user = User.objects(username=name).get()
            Trip(
                title=self.title,
                content=self.content,
                driver=user
            ).save()
        except DoesNotExist:
            print("User ", name, " not found")

        self.runTrip()

    def runTrip(self):
        global activeTrips
        tripIsActive = True
        while tripIsActive:
            tripIsActive = activeTrips[self.trip_id]['running']

            """We are not working with car GPS. Instead, we generate a random number of meters to the supposedly 
                        next point on the path of the car."""
            randomTripValue = random.randint(0, 30)
            carSpeed = randomTripValue * 3.6

            # Checking if user violated the rules. Car speed > 60.

            if carSpeed > 60 and carSpeed < 81:
                penaltiePoints = 1
            elif carSpeed > 80 and carSpeed < 101:
                penaltiePoints = 2
            elif carSpeed > 100:
                penaltiePoints = 5
            else:
                penaltiePoints = 0

            tripDict = {
                'tripID': self.trip_id,
                'tripTime': str(datetime.utcnow()),
                'carSpeed': carSpeed,
                'penaltiePoints': penaltiePoints
            }

            user = User.objects(username=self.username).get()
            trip = Trip.objects(driver=user).get
            trip.trip_gps.append(tripDict)

            time.sleep(1)


# *** DB CLASSES ***
class User(Document):
    """
        This class exists to work with DB USER instance.
        USERNAME, EMAIL must be unique values otherwise the data will not be entered into the database.
        All the values except DATE_CREATED are required.

    """
    username = StringField(required=True, unique=True)
    email = EmailField(required=True, unique=True)
    password = StringField(required=True)
    age = IntField(required=True)
    car = StringField(required=True)
    driving_exp = IntField(required=True)
    date_created = DateTimeField(default=datetime.utcnow)

    meta = {
        'indexes': ['username', 'email'],
        'ordering': ['-date_created']
    }


class Trip(DynamicDocument):
    title = StringField()
    content = StringField()
    driver = ReferenceField(User)
    trip_gps = ListField()
    date_created = DateTimeField(default=datetime.utcnow)

    meta = {
        'indexes': ['title', 'driver'],
        'ordering': ['-date_created']
    }




if __name__ == '__main__':
    connect('mongo-db')

    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='fm_actions_queue')
    channel.basic_consume(queue='fm_actions_queue', on_message_callback=on_request)
    print(" [x] Awaiting RPC requests")
    channel.start_consuming()
