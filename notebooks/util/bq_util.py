#!/usr/bin/python
# -*- coding: utf-8 -*-
"""Modified version of tom's bigquery utilities. TODO(amy) merge with tom's so
that we don't have multiple versions floating around."""

import IPython.core.magic as magic
import IPython.core.display as display
from optparse import OptionParser, make_option

import cgi
import datetime
import httplib2
import json
import os
import re
import sys
import time
import logging

import oauth2client.client
import oauth2client.file
import oauth2client.tools
import googleapiclient
from googleapiclient.discovery import build

import boto
import gcs_oauth2_boto_plugin

import numpy as np
from pandas.core.api import DataFrame

try:
    import secrets
except ImportError:
    logging.warn("Missing ecrets.py. ")


class MockStorage():
    """Mock storage class for using on AWS when we might not want to store the
       bigquery credential in a local file."""
    def put(self, contents):
        pass


def _get_fresh_credentials(cred_type="bigquery"):
    """Get a fresh set of google credentials (and don't store locally)."""
    storage = MockStorage()
    credentials = None

    if credentials is None or credentials.invalid:
        flow = oauth2client.client.OAuth2WebServerFlow(
            secrets.BIGQUERY_CLIENT_ID,
            secrets.BIGQUERY_CLIENT_SECRET,
            'https://www.googleapis.com/auth/%s' % cred_type)
        import argparse
        flags = (
            argparse.ArgumentParser(parents=[oauth2client.tools.argparser])
            .parse_args(["--noauth_local_webserver"]))
        credentials = oauth2client.tools.run_flow(flow, storage, flags)
    return credentials


def _get_bigquery_client():
    """Return an authorized bigquery client."""
    credentials = _get_fresh_credentials(cred_type="bigquery")
    http = httplib2.Http()
    http = credentials.authorize(http)
    return build('bigquery', 'v2', http=http)


def upload_files_to_gcs(client, filenames, localdir, gspath, log=False):
    """Use boto client to upload local files to GCS bucket."""
    components = gspath.split('/')
    for filename in filenames:
        if log:
            print "Uploading to GCS: %s" % filename
        localname = os.path.join(localdir, filename)
        if len(components) > 1:
            destname = os.path.join('/'.join(components[1:]), filename)
        else:
            destname = filename
        bucket = client.storage_uri(components[0], "gs").get_bucket()
        new_file = bucket.new_key(destname)
        new_file.set_contents_from_filename(localname)

        
def upload_df_to_bq(bq_service,
                    dataframe,
                    project_id,
                    dataset_id,
                    table_id,
                    chunksize=1000):
    """Upload pandas dataframe to bigquery table."""
    from apiclient.errors import HttpError
    import uuid
<<<<<<< HEAD
    import sleep

=======
    
>>>>>>> 7dd2b25dbaeb54088aa144530ad27d2018e03c10
    job_id = uuid.uuid4().hex
    rows = []
    remaining_rows = len(dataframe)

    total_rows = remaining_rows

    for index, row in dataframe.reset_index(drop=True).iterrows():
        row_dict = dict()
        row_dict['json'] = json.loads(row.to_json(force_ascii=False,
                                                  date_unit='s',
                                                  date_format='iso'))
        row_dict['insertId'] = job_id + str(index)
        rows.append(row_dict)
        remaining_rows -= 1

        if (len(rows) % chunksize == 0) or (remaining_rows == 0):
            print "\rStreaming Insert is {0}% Complete".format(
                ((total_rows - remaining_rows) * 100) / total_rows)

            body = {'rows': rows}

            try:
                response = bq_service.tabledata().insertAll(
                    projectId=project_id,
                    datasetId=dataset_id,
                    tableId=table_id,
                    body=body).execute()
            except HttpError as ex:
                raise RuntimeError("Https errors: %s" % ex)

            # For streaming inserts, even if you receive a success HTTP
            # response code, you'll need to check the insertErrors property
            # of the response to determine if the row insertions were
            # successful, because it's possible that BigQuery was only
            # partially successful at inserting the rows.  See the `Success
            # HTTP Response Codes
            # <https://cloud.google.com/bigquery/
            #       streaming-data-into-bigquery#troubleshooting>`__
            # section

            insert_errors = response.get('insertErrors', None)
            if insert_errors:
                raise RuntimeError("Insertion errors: %s" % insert_errors)

            sleep(1)  # Maintains the inserts "per second" rate per API
            rows = []
    print ""
        

def download_files_from_gcs(client, gspath, localpath=None, log=False):
    """Use boto client to download gcs path to a local file."""
    components = gspath.split('/')
    if localpath is None:
        localpath = components[-1]
    obj = client.storage_uri(gspath, "gs")
    if log:
        print "Downloading from GCS: %s" % gspath
    with open(localpath, "w") as f:
        obj.get_contents_to_file(f)


def list_files_in_gcs_bucket(client, gspath):
    """List all GCS files (csv, txt and json) matching the provided path."""
    components = gspath.split('/')
    bucket = client.storage_uri(components[0], "gs")
    if len(components) == 1:
        filekeys = bucket.get_bucket().get_all_keys()
    else:
        filekeys = bucket.get_bucket().get_all_keys(
            prefix='/'.join(components[1:]))
    files = [f.name for f in filekeys if f.name.endswith('.csv') or
             f.name.endswith('.json') or f.name.endswith('.txt')]
    return files

def query_to_df(query, bq=None):
    """Execute query and return result as a pandas dataframe."""
    result = exec_and_return(query, bq)
    return result.rows

def get_authed_clients():
    """Return authenticated clients for BigQuery and GCS."""
    print "Authenticating with BigQuery and GCS..."
    bq = _get_bigquery_client()
    gcs = _login_to_gcs()
    return bq, gcs


def _login_to_gcs():
    """Helper to ensure we have login credentials to GCS in boto."""
    gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret(
            secrets.BIGQUERY_CLIENT_ID,
            secrets.BIGQUERY_CLIENT_SECRET)

    try:
        uri = boto.storage_uri('', 'gs')
        uri.get_all_buckets(
            headers={"x-goog-project-id": secrets.BIGQUERY_PROJECT_ID})

    except:
        plugin = gcs_oauth2_boto_plugin
        oauth2_refresh_token = (
            plugin.oauth2_helper.OAuth2ApprovalFlow(
                plugin.oauth2_helper.OAuth2ClientFromBotoConfig(
                    boto.config, "Oauth 2.0 User Account"),
            ['https://www.googleapis.com/auth/devstorage.full_control'],
            False))

        try:
            boto.config.add_section('Credentials')
        except:
            pass

        boto.config.set("Credentials", "gs_oauth2_refresh_token",
                oauth2_refresh_token)

        with open(os.path.expanduser("~/.boto"), "w") as f:
            boto.config.write(f)
    return boto


def do_auth():
    """Authenticate with BigQuery and GCS."""
    display.clear_output()
    print "Authenticating with BigQuery..."
    sys.stdout.flush()
    _get_bigquery_service()

    display.clear_output()
    print "Authenticating with Google Cloud Storage..."
    sys.stdout.flush()
    _login_to_gcs()

    class AuthResult(object):
        def _repr_html_(self):
            return "<span style='color: #33CC33'>✓</span> Authenticated."

        def _repr_latex_(self):
            return ""

    display.clear_output()
    return AuthResult()


def _get_bigquery_service():
    """Helper to load BigQuery credentials and do OAuth for us."""
    flow = oauth2client.client.OAuth2WebServerFlow(
            secrets.BIGQUERY_CLIENT_ID,
            secrets.BIGQUERY_CLIENT_SECRET,
            'https://www.googleapis.com/auth/bigquery')

    cred_path = os.path.dirname(os.path.abspath(__file__))
    storage = oauth2client.file.Storage(
            '%s/bigquery_credentials.dat' % cred_path)
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        import argparse
        flags = (
            argparse.ArgumentParser(parents=[oauth2client.tools.argparser])
            .parse_args(["--noauth_local_webserver"]))
        credentials = oauth2client.tools.run_flow(flow, storage, flags)

    http = httplib2.Http()
    http = credentials.authorize(http)
    return build('bigquery', 'v2', http=http)


def _sizeof_fmt(num, suffix='B'):
    """Helper to turn byte counts into human-readable amounts, e.g. GiB."""
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def _download_from_gcs(gcs_filename, local_filename):
    """Download a file from a GCS bucket to the local filesystem.
    
    gcs_filename - the bucket and filename, e.g. ka_users/foo.txt (not
        including the gs:// prefix)
    """
    _login_to_gcs()
    uri = boto.storage_uri(gcs_filename, "gs")

    with open(local_filename, "w") as out_file:
        # The unintuitively-named get_file() doesn't return the object
        # contents; instead, it actually writes the contents to
        # the file.
        uri.get_key().get_file(out_file)

        
class BigQueryResult(object):
    """An object containing query result data that renders itself in HTML."""

    def __init__(self, job_id):
        self.job_id = job_id
        self.status = "UNSTARTED"
        self.start_time = datetime.datetime.now()
        self.error = None
        self.rows = None
        self.type = "inprogress"

    def animate(self):
        display.clear_output()
        display.display(self)
        sys.stdout.flush()

    @staticmethod
    def _parse_entry(field_value, field_type):
        """Stolen directly from pandas/io/gbq.py."""
        if field_value is None or field_value == 'null':
            return None
        if field_type == 'INTEGER' or field_type == 'FLOAT':
            return float(field_value)
        elif field_type == 'TIMESTAMP':
            timestamp = datetime.utcfromtimestamp(float(field_value))
            return np.datetime64(timestamp)
        elif field_type == 'BOOLEAN':
            return field_value == 'true'
        return field_value

    def update_status(self, status):
        self.status = status
        self.animate()

    def job_complete(self):
        self.end_time = datetime.datetime.now()
        self.duration = self.end_time - self.start_time

    def job_error(self, error):
        """Report a query that failed with some error."""
        self.job_complete()
        self.error = error
        self.animate()

    def extract_success(self, uri):
        self.update_status("DOWNLOADING")

        gcs_filename = uri[5:]
        local_filename = "./%s" % uri.split("/")[-1]
        _download_from_gcs(gcs_filename, local_filename)

        self.job_complete()
        self.type = "extract"
        self.output_uri = uri
        self.output_file = local_filename
        self.animate()

    def query_results(self, jobs, results):
        """Parse query results and turn them into a NumPy DataFrame."""
        # see: http://pandas.pydata.org/pandas-docs/dev/missing_data.html#missing-data-casting-rules-and-indexing
        dtype_map = {
                'INTEGER': np.dtype(float),
                'FLOAT': np.dtype(float),
                'TIMESTAMP': 'M8[ns]'}     # This seems to be buggy without
                                           # nanosecond indicator

        # This might take some time, so let the user know the query is done
        self.update_status("PROCESSING")

        self.job_complete()

        self.type = "query"
        self.total_rows = int(results['totalRows'])
        self.bytes_processed = int(results['totalBytesProcessed'])

        fields = results['schema']['fields']
        col_types = [field['type'] for field in fields]
        col_names = [
                field['name'].encode('ascii', 'ignore') for field in fields]
        col_dtypes = [
                dtype_map.get(field['type'], object) for field in fields]
        row_array = np.zeros((self.total_rows,),
            dtype=zip(col_names, col_dtypes))

        row_num = 0

        while 'rows' in results and row_num < self.total_rows:
            for row in results['rows']:
                entries = row.get('f', [])
                for col_num, field_type in enumerate(col_types):
                    field_value = BigQueryResult._parse_entry(
                            entries[col_num].get('v', ''),
                            field_type)
                    row_array[row_num][col_num] = field_value

                row_num += 1

            page_token = results.get('pageToken', None)

            results = jobs.getQueryResults(
                    projectId=secrets.BIGQUERY_PROJECT_ID,
                    jobId=self.job_id,
                    pageToken=page_token).execute()

        self.rows = DataFrame(row_array)

        self.animate()

    def _get_repr_table(self):
        headings = []
        values = []

        if self.error:
            headings = ["Job ID", "End time", "Duration", "Error"]
            values = [self.job_id, str(self.end_time)[:-7],
                self.duration.seconds, self.error]

        elif self.type == "inprogress":
            headings = ["Job ID", "Start time", "Duration", "Status"]
            duration = datetime.datetime.now() - self.start_time
            values = [self.job_id, str(self.start_time)[:-7],
                duration.seconds, self.status]

        elif self.type == "query":
            headings = ["Job ID", "End time", "Duration", "Rows",
                "Data size", "Cost"]
            # $0.005 per GiB
            gb = self.bytes_processed / 1073741824
            cost_in_cents = round(0.5 * gb * 10) / 10
            values = [self.job_id, str(self.end_time)[:-7],
                self.duration.seconds, format(self.total_rows, ",d"),
                _sizeof_fmt(self.bytes_processed), "%s¢" % cost_in_cents]

        elif self.type == "extract":
            headings = ["Job ID", "End time", "Duration", "GCS uri",
                    "filename"]
            values = [self.job_id, str(self.end_time)[:-7],
                self.duration.seconds, self.output_uri, self.output_file]

        return headings, map(str, values)

    def _repr_html_(self):
        """Return a nicely formatted table with metadata about the query."""
        headings, values = self._get_repr_table()

        # Sanitize for HTML
        values = [cgi.escape(v) for v in values]

        return ("<table><tr>" +
                "".join(map(
                    lambda heading: "<th>%s</th>" % heading, headings)) +
                "</tr><tr>" +
                "".join(map(
                    lambda value: "<td>%s</td>" % value, values)) +
                "</tr></table>")

    def _repr_latex_(self):
        headings, values = self._get_repr_table()

        # Sanitize for LaTeX
        values = [v.replace("_", "\\_").replace("¢", "") for v in values]

        return ("\\begin{tabular}{" +
                "".join(["| l " for x in headings]) + "|}\n" +
                " & ".join(headings) +
                "\\\\\n" +
                " & ".join(values) +
                "\\\\\n" +
                "\\end{tabular}")


def _run_and_wait(jobs, job_data):
    """Run the job described by job_data and wait for it to complete.
    
    jobs - The bq.jobs() service
    job_data - A data structure that contains the parameters of the job. For
      reference, see:
      https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html#insert
    """
    query_reply = jobs.insert(
        projectId=secrets.BIGQUERY_PROJECT_ID,
        body=job_data).execute()

    job_reference = query_reply['jobReference']
    result = BigQueryResult(job_reference['jobId'])

    try:
        wait_count = 600
        while True:
            status = jobs.get(
                projectId=secrets.BIGQUERY_PROJECT_ID,
                jobId=job_reference['jobId']).execute()

            # Are we done?
            if status["status"]["state"] not in ("RUNNING", "PENDING"):
                break

            # Do we give up?
            wait_count -= 1
            if wait_count < 0:
                result.job_error("Timed out waiting for query completion")
                return result

            # Show the user an updated value
            result.update_status(status["status"]["state"])

            time.sleep(1)

        if status["status"]["state"] not in ("SUCCESS", "DONE"):
            result.job_error(status["status"]["state"] + ": " +
                ", ".join([
                    "%s %s" % (e.get("message"),
                        " (at %s)" % e.get('location')
                        if e.get('location') else "")
                    for e in status["status"].get("errors", [])]))
            return result

        if 'query' in job_data["configuration"]:
            query_reply = jobs.getQueryResults(
                projectId=job_reference['projectId'],
                jobId=job_reference['jobId']).execute()

            result.query_results(jobs, query_reply)

        elif 'extract' in job_data["configuration"]:
            result.extract_success(
                    job_data["configuration"]["extract"]["destinationUris"][0])

    except googleapiclient.errors.HttpError as ex:

        result.job_error(str(json.loads(ex.content)['error']['message']))

    return result


def exec_and_return(query, bq=None):
    """Run the passed-in query and return the row data to Python.
    
    query - The query text
    """
    if bq is None:
        bq = _get_bigquery_service()
    jobs = bq.jobs()

    return _run_and_wait(jobs, {
        'configuration': {
            'query': {
                'priority': 'INTERACTIVE',
                'query': query
            }
        }
    })


def exec_and_save(dataset_id, table_id, write_disposition, query):
    """Run the passed-in query and save the results in a table.

    Careful! The table is overwritten each time in order to be able to run
    the same cell repeatedly.

    table - The dataset and table name, i.e. DATASET.TABLE
    query - The query text
    """
    bq = _get_bigquery_service()
    jobs = bq.jobs()

    return _run_and_wait(jobs, {
        'configuration': {
            'query': {
                'destinationTable': {
                    'projectId': secrets.BIGQUERY_PROJECT_ID,
                    'tableId': table_id,
                    'datasetId': dataset_id
                },
                'allowLargeResults': True,
                'writeDisposition': write_disposition,
                'priority': 'INTERACTIVE',
                'query': query
            }
        }
    })


def export_table(dataset_id, table_id):
    """Save the passed-in BigQuery table to GCS.
    
    The file name is automatically generated and returned in the BigQueryResult
    object.

    table - The table name to export, i.e. DATASET.TABLE.
    """
    bq = _get_bigquery_service()
    jobs = bq.jobs()

    # Generate a unique URI
    uri = "gs://ka_bigquery_table_exports/%s-%s.csv.gz" % (
            table_id, datetime.datetime.now().strftime('%Y%m%d_%H%M'))

    return _run_and_wait(jobs, {
        'configuration': {
            'extract': {
                'sourceTable': {
                    'projectId': secrets.BIGQUERY_PROJECT_ID,
                    'tableId': table_id,
                    'datasetId': dataset_id
                },
                'destinationUris': [uri],
                'compression': 'GZIP',
                'destinationFormat': 'CSV'
            }
        }
    })


@magic.magics_class
class BigQueryMagics(magic.Magics):
    """Cell magics that store BigQuery queries and run them.
    
    Before sending the queries to BigQuery we also interpolate any variables
    in the local namespace that are invoked via mustache notation, for
    instance (where `table` was defined in an earlier cell):
        
        SELECT * FROM {{table}}
    
    """

    # The last returned value from BigQueryToPython
    last_result = None

    def _interpolate(self, string):
        return re.sub("{{(.*?)}}", lambda m:
                      self.shell.user_ns.get(m.group(1), ""), string)

    @magic.cell_magic
    def BigQueryToPython(self, line, cell):
        """A magic method to run a BigQuery query and return the results."""
        try:
            query = self._interpolate(cell)
        except TypeError, e:
            return ("TypeError while interpolating variables in query:\n" +
                str(e))
        BigQueryMagics.last_result = exec_and_return(query)

    @magic.cell_magic
    def BigQueryToTable(self, line, cell):
        """A magic method to run a BigQuery query and save results to a table.
        """
        # Parse `line` using optparse
        parser = OptionParser(option_list=[
            make_option("--append", action="store_const",
                        const="WRITE_APPEND", dest="write_disposition"),
            make_option("--overwrite", action="store_const",
                        const='WRITE_TRUNCATE', dest="write_disposition"),
            make_option("--if-empty", action="store_const",
                        const='WRITE_EMPTY', dest="write_disposition"),
        ])
        parser.set_defaults(write_disposition="WRITE_TRUNCATE")
        options, args = parser.parse_args(args=line.split(" "))
        args = filter(lambda x: x != "", args)

        if len(args) < 1 or len(args[0].split(".")) < 2:
            print "Must specify output table in first line, e.g.:"
            print "%%BigQueryToTable my_dataset.my_table"
            return

        dataset_id, table_id = args[0].split(".")
        
        try:
            query = self._interpolate(cell)
        except TypeError, e:
            return ("TypeError while interpolating variables in query:\n" +
                str(e))
        BigQueryMagics.last_result = exec_and_save(
            dataset_id, table_id, options.write_disposition, query)

    @magic.line_magic
    def BigQueryDownloadTable(self, line):
        """A magic method to download a BigQuery table by copying it to GCS.
        """
        args = line.split(" ")
        if len(args) < 1 or len(args[0].split(".")) != 2:
            print "Must specify output table in first line, e.g.:"
            print "%%BigQueryDownloadTable my_dataset.my_table"
            return

        dataset_id, table_id = args[0].split(".")

        BigQueryMagics.last_result = export_table(dataset_id, table_id)


def get_last_results():
    """Retrieve the DataFrame for the result of the last query that was run."""
    if (BigQueryMagics.last_result and
        BigQueryMagics.last_result.type == "query"):
        return BigQueryMagics.last_result.rows

    return None


def get_last_filename():
    if (BigQueryMagics.last_result and
        BigQueryMagics.last_result.type == "extract"):
        return BigQueryMagics.last_result.output_file

    return None


# Register the magics
import __builtin__
__builtin__.get_ipython().register_magics(BigQueryMagics)
