import logging
import grpc
import gen.iam_pb2_grpc as iam_grpc
import gen.iam_pb2 as iam_pb
import uuid
import time
import threading
import Queue
import argparse
from PermissionModel import Permission

def timed(fn):
    def timed_fn(*args, **kwargs):
        start_time = time.time()
        fn(*args, **kwargs)
        logging.info("%s took %f secs", fn.__name__, time.time()-start_time)

    return timed_fn


class PermissionsCreator(threading.Thread):
    """
    Permission creating thread. It will create a new IAM client connection and
    start consuming permissions information from the queue to form its create
    request
    """

    HOST = "localhost:9091"
    GRPC_TIMEOUT = 1

    def __init__(self, work_q, response_q, id):
        super(PermissionsCreator, self).__init__()
        self.work_q = work_q
        self.response_q = response_q
        self.stop_condition = threading.Event()
        self.log_pfx = id

        self.channel = grpc.insecure_channel(PermissionsCreator.HOST)
        self.stub = iam_grpc.IamServiceStub(self.channel)

    def create_permission(self,
                          subject_aui,
                          object_aui,
                          role_aui,
                          requesting_subject_aui, timeout=None):
        response = self.stub.createPermission(
                            iam_pb.CreatePermissionRequest(
                                subject_aui=subject_aui,
                                object_aui=object_aui,
                                role_aui=role_aui,
                                requesting_subject_aui=requesting_subject_aui),
                            timeout=timeout)
        return response

    def run(self):
        while not self.stop_condition.is_set():
            try:
                permission = self.work_q.get(True, 0.05)
                self.create_permission(permission.subject_aui,
                                       permission.object_aui,
                                       permission.role_aui,
                                       permission.requestor_aui,
                                       PermissionsCreator.GRPC_TIMEOUT)
                self.response_q.put(True)

            except Queue.Empty:
                continue

    def join(self, timeout=None):
        self.stop_condition.set()
        super(PermissionsCreator, self).join(timeout)


class PermissionsProducer:
    USER_PREFIX = "aui:iam:user/stress-test-"
    ASSET_PREFIX = "aui:asset:vehicle/stress-test-"
    GRPC_TIMEOUT = 1

    def __init__(self):
        pass

    def __call__(self, args):
        work_q = Queue.Queue()
        response_q = Queue.Queue()

        thread_pool = [PermissionsCreator(work_q, response_q, cid) for cid in xrange(0, args.thread)]

        # Start all permission creating threads
        for thread in thread_pool:
            thread.start()

        start_time = time.time()
        total_sent = 0
        user_aui = 'aui:iam:user/userfoo'
        while (time.time() - start_time) < args.duration:
            for _ in xrange(0, args.pers_per_sec):
                work_q.put(Permission(user_aui,
                                      PermissionsProducer.ASSET_PREFIX + str(uuid.uuid4()),
                                      "aui:iam:role/vehicle-driver",
                                      "aui:iam:user/stress-test-runner"))
                total_sent = total_sent + 1
            time.sleep(1)

        elapsed = time.time() - start_time
        print ("Elapsed: " + str(elapsed) + " seconds")
        print ("Sent: " + str(total_sent) + " permissions")
        print ("Effective: " + str(total_sent/elapsed) + " permissions/sec")

        for _ in xrange(0, total_sent):
            response_q.get()

        for thread in thread_pool:
            thread.join()

        print("Bye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('duration', action='store', help='test duration in secs', type=float)
    parser.add_argument('pers_per_sec', action='store', help='permission to create per second', type=int)
    parser.add_argument('thread', action='store', help='number of creator threads', type=int)

    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    PermissionsProducer()(args)
