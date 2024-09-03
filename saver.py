import cv2
import numpy as np
import sys
import os
import time

from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

# Default RabbitMQ server URI
rabbit_url = os.environ.get('RABBIT_URL', 'amqp://hypernode:hypernode@localhost:5672//')

# Kombu Message Consuming Worker
class Worker(ConsumerMixin):
    def __init__(self, connection, queues):
        self.connection = connection
        self.queues = queues
        self.frame_count = 0
        self.output_dir = "output_frames"
        self.previous_frame = None
        self.previous_timestamp = None
        self.threshold = 1000000  # Adjust this threshold as needed
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=self.queues,
                         callbacks=[self.on_message],
                         accept=['image/jpeg'])]

    def on_message(self, body, message):
        # get the original jpeg byte array size
        size = sys.getsizeof(body) - 33
        # jpeg-encoded byte array into numpy array
        np_array = np.frombuffer(body, dtype=np.uint8)
        np_array = np_array.reshape((size, 1))
        # decode jpeg-encoded numpy array 
        image = cv2.imdecode(np_array, 1)

        # Get the current timestamp from message headers
        current_timestamp = int(message.headers.get('timestamp', 0))

        # Compare with the previous frame and timestamp
        if self.previous_frame is not None and self.previous_timestamp is not None:
            time_diff = current_timestamp - self.previous_timestamp
            difference = cv2.absdiff(self.previous_frame, image)
            non_zero_count = np.count_nonzero(difference)
            if time_diff >= 1 and non_zero_count > self.threshold:
                # Save the image to disk
                filename = os.path.join(self.output_dir, f"frame_{self.frame_count:06d}.jpg")
                print(f"Saving frame to {filename}")
                cv2.imwrite(filename, image)
                self.frame_count += 1
                self.previous_timestamp = current_timestamp
                self.previous_frame = image
        else:
            # Initialize the previous frame and timestamp
            self.previous_timestamp = current_timestamp
            self.previous_frame = image

        # send message ack
        message.ack()

def run():
    exchange = Exchange("video-exchange", type="fanout")
    queues = [Queue("video-queue", exchange, routing_key="video")]
    with Connection(rabbit_url, heartbeat=4) as conn:
            worker = Worker(conn, queues)
            worker.run()

if __name__ == "__main__":
    run()