from __future__ import (absolute_import, division, print_function, unicode_literals)
import logging
import datetime
import time
import sys
import urllib2
import json

import boundary_plugin
import boundary_accumulator

"""
If getting statistics fails, we will retry up to this number of times before
giving up and aborting the plugin.  Use 0 for unlimited retries.
"""
PLUGIN_RETRY_COUNT = 0
"""
If getting statistics fails, we will wait this long (in seconds) before retrying.
"""
PLUGIN_RETRY_DELAY = 5


class CouchDBPlugin(object):
    def __init__(self, boundary_metric_prefix):
        self.boundary_metric_prefix = boundary_metric_prefix
        self.settings = boundary_plugin.parse_params()
        self.accumulator = boundary_accumulator

    def get_stats(self):
        req = urllib2.urlopen(self.settings.get("stats_url", "http://127.0.0.1:5984/_stats"))
        res = req.read()
        req.close()

        data = json.loads(res)
        return data

    def get_stats_with_retries(self, *args, **kwargs):
        """
        Calls the get_stats function, taking into account retry configuration.
        """
        retry_range = xrange(PLUGIN_RETRY_COUNT) if PLUGIN_RETRY_COUNT > 0 else iter(int, 1)
        for _ in retry_range:
            try:
                return self.get_stats(*args, **kwargs)
            except Exception as e:
                logging.error("Error retrieving data: %s" % e)
                time.sleep(PLUGIN_RETRY_DELAY)

        logging.fatal("Max retries exceeded retrieving data")
        raise Exception("Max retries exceeded retrieving data")

    @staticmethod
    def get_metric_list():
        return (
            ('httpd_status_codes', '201', 'COUCHDB_HTTPD_201', True),
            ('httpd_status_codes', '200', 'COUCHDB_HTTPD_200', True),
            ('httpd_status_codes', '202', 'COUCHDB_HTTPD_202', True),
            ('httpd_status_codes', '401', 'COUCHDB_HTTPD_401', True),
            ('httpd_status_codes', '301', 'COUCHDB_HTTPD_301', True),
            ('httpd_status_codes', '304', 'COUCHDB_HTTPD_304', True),
            ('httpd_status_codes', '405', 'COUCHDB_HTTPD_405', True),
            ('httpd_status_codes', '404', 'COUCHDB_HTTPD_404', True),
            ('httpd_status_codes', '403', 'COUCHDB_HTTPD_403', True),
            ('httpd_status_codes', '500', 'COUCHDB_HTTPD_500', True),
            ('httpd_status_codes', '412', 'COUCHDB_HTTPD_412', True),
            ('httpd_status_codes', '400', 'COUCHDB_HTTPD_400', True),
            ('httpd_status_codes', '409', 'COUCHDB_HTTPD_409', True),
            ('httpd', 'bulk_requests', 'COUCHDB_HTTPD_BULK_REQUESTS', True),
            ('httpd', 'clients_requesting_changes', 'COUCHDB_CLIENTS_REQUESTING_CHANGES', True),
            ('httpd', 'view_reads', 'COUCHDB_VIEW_READS', True),
            ('httpd', 'requests', 'COUCHDB_REQUESTS', True),
            ('httpd', 'temporary_view_reads', 'COUCHDB_TEMPORARY_VIEW_READS', True),
            ('couchdb', 'open_os_files', 'COUCHDB_OPEN_OS_FILES', False),
            ('couchdb', 'auth_cache_hits', 'COUCHDB_AUTH_CACHE_HITS', True),
            ('couchdb', 'database_reads', 'COUCHDB_DATABASE_READS', True),
            ('couchdb', 'open_databases', 'COUCHDB_OPEN_DATABASES', False),
            ('couchdb', 'auth_cache_misses', 'COUCHDB_AUTH_CACHE_MISSES', True),
            ('couchdb', 'database_writes', 'COUCHDB_DATABASE_WRITES', True),
            ('couchdb', 'request_time', 'COUCHDB_REQUEST_TIME', False),
            ('httpd_request_methods', 'HEAD', 'COUCHDB_REQUEST_HEAD', True),
            ('httpd_request_methods', 'GET', 'COUCHDB_REQUEST_GET', True),
            ('httpd_request_methods', 'PUT', 'COUCHDB_REQUEST_PUT', True),
            ('httpd_request_methods', 'POST', 'COUCHDB_REQUEST_POST', True),
            ('httpd_request_methods', 'COPY', 'COUCHDB_REQUEST_COPY', True),
            ('httpd_request_methods', 'DELETE', 'COUCHDB_REQUEST_DELETE', True),
        )

    def handle_metrics(self, data):
        for metric_item in self.get_metric_list():
            category_name, metric_name, boundary_name, accumulate = metric_item[:4]
            metric_data = data.get(category_name, {}).get(metric_name, None)
            if not metric_data or not metric_data.get('current'):
                # If certain metrics do not exist or have no value
                # (e.g. disabled in the CouchDB server or just inactive) - skip them.
                continue
            if accumulate:
                value = self.accumulator.accumulate((category_name, metric_name), metric_data['current'])
            else:
                value = metric_data['current']
            boundary_plugin.boundary_report_metric(self.boundary_metric_prefix + boundary_name, value)

    def main(self):
        logging.basicConfig(level=logging.ERROR, filename=self.settings.get('log_file', None))
        reports_log = self.settings.get('report_log_file', None)
        if reports_log:
            boundary_plugin.log_metrics_to_file(reports_log)
        boundary_plugin.start_keepalive_subprocess()

        while True:
            data = self.get_stats_with_retries()
            self.handle_metrics(data)
            boundary_plugin.sleep_interval()


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '-v':
        logging.basicConfig(level=logging.INFO)

    plugin = CouchDBPlugin('')
    plugin.main()
