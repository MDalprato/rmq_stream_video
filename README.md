#streaming

Streaming video using rabbitMQ and a static FullHD video on a fanout queue

![alt text](img1.png "Multiwindow")

Rmq on docker (macos):

![alt text](img2.png "Performace on docker")


saver.py -> save snapshot every 1 second
consumer.py -> consume video queue
producer.py -> generate video on queue
multi.sh -> run multiple istance of consumer.py