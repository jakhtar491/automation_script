import multiprocessing.pool
import boto3
from urllib.parse import urlparse
from multiprocessing import Process
from datetime import datetime


class NoDaemonProcess(Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class Pool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


def dateparse(string):
    if string is None or string == '' or not isinstance(string, str):
        return ''
    return datetime.strptime(string, '%Y-%m-%d %H:%M:%S.0000000')

def resolve_s3_location(s3_path):
        s3_res = urlparse(s3_path)
        return s3_res.netloc, s3_res.path[1:]

def get_size(s3_path):
    """
    Returns:
        size in bytes
    """

    bucket_name, file_key = resolve_s3_location(s3_path)
    s3_resource = boto3.resource('s3', 'us-west-2')
    object_summary = s3_resource.ObjectSummary(bucket_name, file_key)
    return object_summary.size