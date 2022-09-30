#By agothi (Arpit Gothi)

import sys, getopt
import os
import time
import boto3
import shlex
import logging
import subprocess
from logging import FileHandler
from logging.handlers import SysLogHandler
import concurrent.futures
import threading
import traceback
from enum import Enum

FORMATTER = logging.Formatter("%(asctime)s — %(levelname)s - %(name)s — %(message)s")
SYSLOG_FORMATTER = logging.Formatter("%(levelname)s - %(name)s — %(message)s")
LOG_FILE = "/Users/agothi/modinputs.log"
CONFIGS_DIR = "/opt/splunk/var/lib/splunk/modinputs"
#S3_STATE_BUCKET="state-victor-vpc-02fdb68c7b52a16df-us-west-2-lve"
#STACK_ID="saveonfoods"


def run_command(cmd):
    process = subprocess.Popen(
        shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    process_output, stderr = process.communicate()
    return (process_output, process.returncode, stderr)

def get_facter_output(facter):
    cmd = "facter -p " + facter
    process_output, returncode, stderr = run_command(cmd)
    process_output = process_output.rsplit(b"\n", 1)[0]
    return process_output

def get_bucket_idm_path(bucket,location):
    cmd = "aws s3 ls " + str(bucket+"/"+location)
    process_output, returncode, stderr = run_command(cmd)
    process_output = process_output.rsplit(b"\n", 1)[0]
    process_output = os.path.join(location,process_output.decode().split(' ')[-1].replace("/", ""),"modinputs")
    return process_output

PATH_IDM=get_bucket_idm_path(S3_STATE_BUCKET, os.path.join(STACK_ID, 'noah-migration','backups','idm_modinputs') + "/")


def get_console_handler():
  console_handler = logging.StreamHandler(sys.stdout)
  console_handler.setFormatter(FORMATTER)
  return console_handler

def get_syslog_file_handler():
  handler = SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_DAEMON, address="/dev/log")
  handler.setFormatter(SYSLOG_FORMATTER)
  return handler

def get_migration_log_file_handler():
  file_handler = FileHandler(LOG_FILE)
  file_handler.setFormatter(FORMATTER)
  return file_handler


def get_logger(logger_name):
  logger = logging.getLogger(logger_name)
  logger.setLevel(logging.DEBUG)
  logger.addHandler(get_console_handler())
  logger.addHandler(get_migration_log_file_handler())
  logger.addHandler(get_syslog_file_handler())
  logger.propagate = False
  return logger

logger = get_logger("noah_migrate_apps")

LOG_LEVEL_INFO = 'info'
LOG_LEVEL_WARN = 'warn'
LOG_LEVEL_ERROR = 'error'


def log(message, level = LOG_LEVEL_INFO):
  if level == LOG_LEVEL_INFO:
    logger.info(message)
  elif level == LOG_LEVEL_WARN:
    logger.warning(message)
  elif level == LOG_LEVEL_ERROR:
    logger.error(message)


def download_configs(remote_configs_dir_key):
  global S3_STATE_BUCKET
  global CONFIGS_DIR

  download_dir(S3_STATE_BUCKET, remote_configs_dir_key, CONFIGS_DIR, "download_classic_configs")
  log("Finished downloading configs from s3 bucket={0}, prefix={1} to local directory {2}".format(S3_STATE_BUCKET, remote_configs_dir_key, CONFIGS_DIR))


def download_dir(bucket, prefix, local, context_string, num_workers = 60):
  global LOG_LEVEL_ERROR
  keys, dirs = list_keys_dirs(bucket, prefix)
  for d in dirs:
    dest_pathname = os.path.join(local, d)
    os.makedirs(os.path.dirname(dest_pathname), exist_ok=True)
  num_workers = min(num_workers, len(keys))
  sz = len(keys) // num_workers
  group_keys = [keys[i : i + sz] for i in range(0, len(keys), sz)]
  log("{0}: Total of {1} objects to download by {2} workers".format(context_string, len(keys), num_workers))
  total_downloaded_count = 0
  start = time.time()
  with concurrent.futures.ThreadPoolExecutor(max_workers = num_workers) as executor:
    future_to_key = {executor.submit(download_objs, bucket, prefix, local, _keys) : _keys for _keys in group_keys}
    for future in concurrent.futures.as_completed(future_to_key):
      _keys = future_to_key[future]
      try:
        total_downloaded_count += future.result()
      except Exception as exc:
        stack_trace = traceback.format_exc(limit=None, chain=True)
        log('error: download_dir exception={0}, stack_trace={1}, context={2}'.format(exc, stack_trace, context_string), LOG_LEVEL_ERROR)
        raise exc
  time_taken = time.time() - start
  log("{:s}: total no. of files downloaded={:d}, time_taken={:.1f} seconds".format(context_string, total_downloaded_count, time_taken))


def download_objs(bucket, prefix, local, keys):
  download_count = 0
  session = boto3.session.Session()
  s3 = session.resource('s3')
  for k in keys:
    dest_pathname = os.path.join(local, os.path.relpath(k, prefix))
    os.makedirs(os.path.dirname(dest_pathname), exist_ok=True)
    s3.meta.client.download_file(bucket, k, dest_pathname)
    download_count += 1
    if download_count % 500 == 0:
      log("Thread {0} finished downloading {1} files...".format(threading.current_thread().name, download_count))
  log("Thread {0} downloaded {1} files in total".format(threading.current_thread().name, download_count))
  return download_count

def list_keys_dirs(bucket, prefix):
  keys = []
  dirs = []
  next_token = ''
  s3_client = boto3.client('s3')
  base_kwargs = {'Bucket' : bucket, 'Prefix' : prefix,}
  log("Listing the contents in the bucket={0}, prefix={1}".format(bucket, prefix))
  start = time.time()
  loop_count = 0
  while next_token is not None:
    kwargs = base_kwargs.copy()
    if next_token != '':
      kwargs.update({'ContinuationToken' : next_token})
    results = s3_client.list_objects_v2(**kwargs)
    contents = results.get('Contents') if results.get('KeyCount') > 0 else []
    for i in contents:
      k = i.get('Key')
      if k[-1] != '/':
        keys.append(k)
      else:
        dirs.append(k)
    next_token = results.get('NextContinuationToken')
    loop_count += 1
    if loop_count % 5 == 0:
      log("so far listed total_keys={0}, total_dirs={1}".format(len(keys), len(dirs)))
  listing_time = time.time() - start
  log("Finished listing the contents, found total_keys={0}, total_dirs={1}, bucket={2}, prefix={3}, listing_time={4}".format(len(keys), len(dirs), bucket, prefix, listing_time))
  return keys, dirs

def chown_files(owner, path):
    log("Run: chown -R %s:%s %s" % (owner, owner, path))
    try:
        tmp_value = subprocess.check_output(["chown", "-R", "%s:%s" % (owner,owner), path], stderr=subprocess.STDOUT)
        tmp_value = tmp_value.decode("UTF-8")
        log(tmp_value)
    except subprocess.CalledProcessError as error:
        log(error.output, LOG_LEVEL_ERROR)
        exit(1)

#path = os.path.join('opt','splunk', 'var', 'lib', 'splunk', 'modinputs')

S3_STATE_BUCKET=get_facter_output("s3_splunk_stack_state_bucket_name").decode()
STACK_ID=get_facter_output("stackid").decode()
is_primary_sh = get_facter_output("is_primary_sh").decode()

if is_primary_sh == "true":
  download_configs(PATH_IDM)
  chown_files("splunk", CONFIGS_DIR)
else:
  print("This is not Primary SH")



