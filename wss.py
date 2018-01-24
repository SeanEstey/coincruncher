# For deprecated btfxwss websocket package
import threading, logging, time, requests
from queue import Queue
from app.timer import Timer
logging.getLogger("requests").setLevel(logging.ERROR)
ticker_q = None
log = logging.getLogger(__name__)

class DataFetcher(object):
    """ Threading example class
    The run() method will be started and it will run in the background
    until the application exits.
    """

    def __init__(self, name, interval=1):
        """ Constructor
        :type interval: int
        :param interval: Check interval, in seconds
        """
        self.interval = interval
        self.name = name

        self.thread = threading.Thread(target=self.run, name=name, args=())
        self.thread.daemon = True                            # Daemonize thread
        self.thread.start()                                  # Start the execution

    def run(self):
        """ Method that runs forever """
        while True:

            response = requests.get("https://www.google.ca")
            log.info("Adding data to queue")
            ticker_q.put("From %s" % self.name) #response.text)

            time.sleep(self.interval)

#----------------------------------------------------------------------
if __name__ == "__main__":
    ticker_q = Queue()
    datadaemon = DataFetcher("datadaemon")
    restdaemon = DataFetcher("RESTdaemon")

    while True:
        if not datadaemon.thread.is_alive():
            log.error("datadaemon is dead!")
            break
        if not restdaemon.thread.is_alive():
            log.error("RESTdaemon is dead!")
            break

        if ticker_q.empty():
            time.sleep(1)
            continue

        n=1
        while not ticker_q.empty():
            try:
                item = ticker_q.get(block=False)
            except queue.Empty as e:
                log.info("None value found in queue. Quitting")
                break
            else:
                log.info("Queue item #%s received: %s", n, item)
                n+=1

    datadaemon.join()
    restdaemon.join()
    exit()
