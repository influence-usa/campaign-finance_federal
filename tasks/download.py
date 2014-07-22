import os
import sys
import re
import logging
import time
import json
import urlparse
from glob import glob, iglob
from datetime import datetime
from ftplib import error_perm

from itertools import product
from multiprocessing.dummy import Pool as ThreadPool

import requests

from settings import CACHE_DIR, TRANS_DIR
from .utils import mkdir_p, ftp_connect
from .log import set_up_logging

log = set_up_logging('download', loglevel=logging.DEBUG)


# GENERAL DOWNLOAD FUNCTIONS
def response_download(response, output_loc):
    if response.ok:
        try:
            with open(output_loc, 'wb') as output_file:
                for chunk in response.iter_content():
                    output_file.write(chunk)
            return response.headers['content-length']
        except Exception as e:
            log.error(e)
    else:
        log.error('response not okay: '+response.reason)
        raise Exception('didn''t work, trying again')


def log_result(result):
    if result[0] == 'success':
        url, loc, content_length = result[1:]
        log.info(
            'success: {source} => {dest}({size})'.format(
                source=url, dest=loc, size=content_length))
    elif result[0] == 'failure':
        url, loc, exception = result[1:]
        log.info(
            'failure: {source} => {dest}\n {e}'.format(
                source=url, dest=loc, e=str(exception)))
    else:
        raise Exception


def download_http(val, get_response_loc_pair):
    for i in xrange(5):
        _response, _loc = get_response_loc_pair(val)
        _url = _response.url
        if is_not_cached_http(_response, _loc):
            try:
                content_length = response_download(_response, _loc)
                return ('success', _url, _loc, content_length)
            except Exception:
                log.warn('{url} something went wrong, trying again ({code} - {reason})'.format(
                             url=_response.url,
                             code=_response.status_code,
                             reason=_response.reason))
                time.sleep(5)
        else:
            log.info('cached, not re-downloading')
            return('success', _url, _loc, 'cached')
    return ('failure', _response.url, _loc, '[{code}] {reason}'.format(
        code=_response.status_code, reason=_response.reason))


def download_all_http(vals, get_response_loc_pair, options):
    threaded = options.get('threaded', False)
    thread_num = options.get('thread_num', 4)

    if threaded:
        log.info("starting threaded download")
        pool = ThreadPool(thread_num)
        for val in vals:
            log.debug("async start for {}".format(str(val)))
            pool.apply_async(download_http, args=(val, get_response_loc_pair),
                             callback=log_result)
        pool.close()
        pool.join()
    else:
        for val in vals:
            log_result(download_http(val, get_response_loc_pair))


def is_not_cached_http(response, output_loc):
    response, output_loc
    if os.path.exists(output_loc):
        downloaded_size = int(os.path.getsize(output_loc))
        log.debug(
            'found {output_loc}: {size}'.format(
                output_loc=output_loc,
                size=downloaded_size))
        size_on_server = int(response.headers['content-length'])
        if downloaded_size != size_on_server:
            log.debug(
                're-downloading {url}: {size}'.format(
                    url=response.url,
                    size=size_on_server))
            return True
        else:
            response.close()
            return False
    else:
        return True


def is_not_cached_ftp(connection, file_name, outloc):
    if os.path.exists(outloc):
        mTime = connection.sendcmd('MDTM ' + file_name)
        mTime = datetime.strptime(mTime[4:], "%Y%m%d%H%M%S")
        cached_mTime = datetime.fromtimestamp(os.path.getmtime(outloc))
        if mTime > cached_mTime:
            return True
        else:
            return False
    else:
        return True


def download_all_ftp(url, download_fct, download_args, options):
    threaded = options.get('threaded', False)
    thread_num = options.get('thread_num', 4)

    if threaded:
        log.info("starting threaded download")
        pool = ThreadPool(thread_num)
        for val in download_args:
            log.debug("async start for {}".format(str(val)))
            pool.apply_async(download_fct, args=(val,),
                             callback=log_result)
        pool.close()
        pool.join()
    else:
        for val in download_args:
            log_result(download_fct(val))


# SPECIFIC TASKS
def download_fec_itemized(options):
    fec_filetypes = ['ccl', 'oth', 'pas2', 'cn', 'cm', 'indiv']

    if options.get('loglevel', None):
        log.setLevel(options['loglevel'])

    def _download_year(year):
        #TODO Make this into a function that returns (remote_loc, local_loc) pairs
        ftp_conn = ftp_connect('ftp.fec.gov')
        ftp_conn.cwd('/FEC/{year}'.format(year=year))
        # TODO factor this up
        for ftype in fec_filetypes:
            fname = ftype + year[2:] + '.zip'
            log.info('downloading {fname}'.format(fname=fname))
            outdir = os.path.join(CACHE_DIR, year)
            if not os.path.exists(outdir):
                mkdir_p(outdir)
            outloc = os.path.join(outdir, fname)
            if is_not_cached_ftp(ftp_conn, fname, outloc):
                outfile = open(outloc, 'wb')
                try:
                    ftp_conn.retrbinary('RETR {fn}'.format(fn=fname),
                                        outfile.write)
                except error_perm as e:
                    return ('failure', fname, outloc, str(e))
                except Exception as e:
                    return ('failure', fname, outloc, str(e))
            else:
                return('success', fname, outloc, 'no change')
        ftp_conn.close()
        return ('success', year, outdir, 'downloaded')

    _url = 'ftp.fec.gov'

    _years = [str(year) for year in xrange(2008, 2015, 2)]

    # response_loc_pairs = (_get_response_loc_pair(url) for url in _urls)
    download_all_ftp(_url, _download_year, _years, options)


