#!/usr/bin/env python

###
# Autonomic Proprietary 1.0
#
# Copyright (C) 2018 Autonomic, LLC - All rights reserved
#
# Proprietary and confidential.
#
# NOTICE:  All information contained herein is, and remains the property of
# Autonomic, LLC and its suppliers, if any.  The intellectual and technical
# concepts contained herein are proprietary to Autonomic, LLC and its suppliers
# and may be covered by U.S. and Foreign Patents, patents in process, and are
# protected by trade secret or copyright law. Dissemination of this information
# or reproduction of this material is strictly forbidden unless prior written
# permission is obtained from Autonomic, LLC.
#
# Unauthorized copy of this file, via any medium is strictly prohibited.
#
# ###

import logging
import grpc
import gen.iam_pb2_grpc as iam_grpc
import gen.iam_pb2 as iam_pb
import uuid
import time
import threading
import Queue
import argparse
from permission_model import Permission


class PermissionsCreator(threading.Thread):
    """
    Permission creating thread. It will create a new IAM client connection and
    start consuming permissions information from the queue to form its create
    request. Each thread simulates a distinct application that might be invoking
    IAM RPC methods.
    """

    HOST = "localhost:9091"
    GRPC_TIMEOUT = 10

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

        """
        Below is an attempt to perform a rate-driven load. Every seconds we 
        'produce' a fixed number of permissions and those are added to the work
        queue. The consumers will pick up the permission information from the 
        work queue and actually invoke the IAM RPC create permissions method.
        
        This is an imperfect way to generate permissions at a desired rate 
        because each iteration takes a finite time to perform the queue put and 
        other operations. Over time it will add up to the skew. Ideally we want a watchdog
        that timer that wakes up 
        """
        while (time.time() - start_time) < args.duration:
            for _ in xrange(0, args.pers_per_sec):
                work_q.put(Permission(user_aui,
                                      PermissionsProducer.ASSET_PREFIX + str(uuid.uuid4()),
                                      "aui:iam:role/vehicle-driver",
                                      "aui:iam:user/stress-test-runner"))
                total_sent = total_sent + 1
            time.sleep(1)

        elapsed = time.time() - start_time

        print ("Dispatched all create requests. Waiting for all creators to complete...")
        # Wait for all permission creator threads to complete
        for _ in xrange(0, total_sent):
            response_q.get()

        # Ask all the permission creator threads to die
        for thread in thread_pool:
            thread.join()

        print ("Elapsed: " + str(elapsed) + " seconds")
        print ("Sent: " + str(total_sent) + " permissions")
        print ("Effective: " + str(total_sent/elapsed) + " permissions/sec")

        print("Bye!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('duration', action='store', help='test duration in secs', type=float)
    parser.add_argument('pers_per_sec', action='store', help='permission to create per second', type=int)
    parser.add_argument('thread', action='store', help='number of creator threads', type=int)

    args = parser.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    PermissionsProducer()(args)
