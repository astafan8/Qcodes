import zmq
from time import sleep

from numpy.random import rand

topic = "TEST"
port = 5555

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect(f"tcp://127.0.0.1:{port}")
topicfilter = topic.encode('utf-8')
socket.setsockopt(zmq.SUBSCRIBE, topicfilter)
# according to "the book", we should sleep here
sleep(1)
# now signal back that we are ready to receive published data
req_socket = context.socket(zmq.REQ)
req_socket.connect(f"tcp://127.0.0.1:{port+1}")

# make a PUSH socket to PUSH back status about where we are
push_sock = context.socket(zmq.PUSH)
push_sock.bind(f"tcp://127.0.0.1:{port+2}")

if __name__ == "__main__":

    # send synchronisation request
    req_socket.send(b"")
    # wait for publisher to ping back that it is ready
    req_socket.recv()

    time_to_stop = False
    messages_read = 0

    while not time_to_stop:
        raw_mssg = socket.recv().decode('utf-8')
        # The raw message is {topic},{message}
        topiclength = len(topic)
        mssg = raw_mssg[topiclength+1:]
        print(f'Got message: {mssg}')
        # do some slow an unpredictable work
        sleep(2*rand())
        # then report back that a message was read
        messages_read += 1
        status_mssg = f"{messages_read}"
        push_sock.send(status_mssg.encode("utf-8"))
        if mssg == "KILL":
            time_to_stop = True
            print('Reader signing off')

    for sock in [req_socket, push_sock, socket]:
        sock.close()