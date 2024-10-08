import boto3
import logging
import sys
from datetime import datetime
from pytz import timezone
from utils import newrelic


class BaseJob:
    def parse_arguments(self):
        """
        :return: dict of argument names defined by the argparser
        """
        raise NotImplementedError('Job classes must define how to get arguments')

    def execute(self):
        raise NotImplementedError('Job classes must define execute function')


def GluePythonShellLogger(name=__name__):
    msg_format = '%(asctime)s %(levelname)s %(name)s: %(message)s'
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)s [%(levelname)s] [%(threadName)s] %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    newrelic_handler = newrelic.LogHandler()
    newrelic_handler.setFormatter(formatter)
    newrelic_handler.setLevel(logging.ERROR)
    logger.addHandler(newrelic_handler)

    return logger


def datetime_to_epoch(dt, Float=False):
    # python 3.6 or newer
    stamp = dt.timestamp()
    if Float:
        return stamp
    return int(stamp)


def date_str_pt_to_epoch(dt_string):
    # input: a string in "%Y-%m-%d %H:%M:%S" format
    # output: timestamp
    # The string is assumed to be in Pacific time, standard or daylight
    # as appropriate for the date and time specified
    for dt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            datetime_obj_naive = datetime.strptime(dt_string, dt)
            datetime_obj_pacific = timezone('US/Pacific').localize(datetime_obj_naive)
            offset = datetime_to_epoch(datetime_obj_pacific)
            return offset
        except ValueError:
            pass
    raise ValueError('no valid date format found')


def epoch_date_to_zulu(epoch_string):
    # Direct conversion from UTC epoch time to zulu timestamp
    converted_date = datetime.fromtimestamp(epoch_string).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return converted_date


def naive_date_to_epoch_offset(date):
    # date is a string in the format "YYYY-mm-dd"
    # this function returns start_time and end_time in epoch seconds for
    # midnight at the start of this day to midnight at the end
    # thus any timestamp t occurs on that day if:
    #  midnight <= t < next_midnight
    # Note that, for example, '2020-11-01' contains 25 hours
    midnight = date_str_pt_to_epoch(date + " 00:00:00")
    next_midnight = 1 + date_str_pt_to_epoch(date + " 23:59:59")
    return (midnight, next_midnight)


def now_pt():
    # aws glue naive dates default to UTC so we need to explicitly specify PT
    nowpt = datetime.now(tz=timezone('US/Pacific'))
    return nowpt


def query(athena_runner,
          query_str: str,
          job_name: str,
          logger=None):
    """
    Run an Athena query, and return the location (in s3) of the results (csv)
    """
    athena_result = athena_runner.run_one(query_str, name=job_name)
    if not athena_result:
        msg = 'athena query failed'
        if logger:
            logger.error(msg)
        raise RuntimeError(msg)
    if logger:
        logger.info(f'query result id {athena_result}')
    r = athena_runner.last_query_response
    location = r['QueryExecution']['ResultConfiguration']['OutputLocation']
    if logger:
        logger.info(f'query result location {location}')
    return location


def query_to(athena_runner,
             query_str: str,
             job_name: str,
             target_key: str,
             target_bucket: str = None,
             s3=None,
             logger=None):
    """
    Run an athena query, and copy the results to a new target.
    If target_bucket is not given, assume the same bucket as the query results.
    """
    location = query(athena_runner, query_str, job_name, logger=logger)
    if not location:
        raise RuntimeError(f'query for {job_name} failed, no output location')

    # the location is an S3 url - break it into bucket and key
    source_bucket, source_key = location.replace("s3://", "").split("/", 1)
    copy_source = {
        'Bucket': source_bucket,
        'Key': source_key
    }
    target_bucket = target_bucket or source_bucket
    target_location = f's3://{target_bucket}/{target_key}'
    s3 = s3 or boto3.client('s3')
    if logger:
        logger.info(
            f's3 copy '
            f'from s3://{source_bucket}/{source_key} '
            f'to {target_location}')
    # NOTE: if this is EU then this copy crosses accounts to the US
    s3.copy(copy_source,
            target_bucket,
            target_key,
            ExtraArgs={'ServerSideEncryption': 'AES256', 'ACL': 'bucket-owner-full-control'})
    return target_location
