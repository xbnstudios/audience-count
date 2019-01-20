#!/usr/bin/env python3

# Collects the audience statistics for the last minute
# Run once per minute by cron
# DO NOT RENAME without also updating cron

import psycopg2
import psycopg2.extras
import requests
from subprocess import run, PIPE
from sys import argv, stdout
import xmltodict
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

dbconf = config['database']
db_string = "host={} user={} password={} dbname={}".format(
    dbconf['host'],
    dbconf['username'],
    dbconf['password'],
    dbconf['db_name']
)

conn = psycopg2.connect(db_string)
cur = conn.cursor()

s = requests.Session()
s.headers = {
    "User-Agent": config['general']['user_agent']
}

debug = "--debug" in argv
verbose = "--verbose" in argv or "-v" in argv or debug
rollback = "--rollback-all-tx" in argv or '-r' in argv


def vprint(*args):
    """verbose print. Only print if 'verbose' is set."""
    if verbose:
        print(*args)


def dprint(*args, **kwargs):
    """debug print. Only print if 'debug' is set."""
    if debug:
        print(*args, **kwargs)


class ViewerMetric:
    """A single metric about viewership."""

    def __init__(self, source: str, stream: str, viewers: int):
        """Create a new instance.
        :source: The source from which this metric comes (icecast, youtube,
        rtmp, hls)
        :stream: The slug for the stream that this metric came from (xbn-360,
        xbn-1080, xbn-flac, etc.)
        :viewers: The number of viewers watching the stream
        """
        self.source = source
        self.stream = stream
        self.viewers = viewers

    def __repr__(self):
        return "<ViewerMetric for '{}' '{}'>".format(self.source, self.stream)

    def __hash__(self):
        return "{}:{}".format(self.source, self.stream).__hash__()

    def __eq__(self, other):
        return self.source == other.source and self.stream == other.stream


class ViewerMetricSet:
    """A collection of ViewerMetric objects.

    This collection can only contain one ViewerMetric with any given (source,
    stream) pair."""

    def __init__(self):
        self.__metrics = set()

    def __iter__(self):
        return self.__metrics.__iter__()

    def fill_required(self):
        """Fill in the required minimum set of metrics with zeroes."""
        required = [
            ("youtube", "live"),
            ("hls", "xbn-1080"),
            ("hls", "xbn-720"),
            ("hls", "xbn-360"),
            ("rtmp", "xbn-1080"),
            ("rtmp", "xbn-720"),
            ("rtmp", "xbn-360"),
            ("icecast", "xbn-32"),
            ("icecast", "xbn-128"),
            ("icecast", "xbn-320"),
            ("icecast", "xbn-flac"),
            ("icecast", "xbn-mp")
        ]
        for source, stream in required:
            self.__metrics.add(ViewerMetric(source, stream, 0))

    def add(self, metric: ViewerMetric):
        self.__metrics.discard(metric)
        self.__metrics.add(metric)


# An array of ViewerMetric objects
zbx_metrics = ViewerMetricSet()
zbx_metrics.fill_required()

###
# Aggregate data from various sources
###

# HLS
if True:
    try:
        vprint("Running queries for HLS (no output)")
        # Aggregates playlist loads into the various "views last minute" methods
        # Note: Probably incorrect (low) for the *current minute*. viewers_latest is correct.
        cur.execute("""
            INSERT INTO viewcount (timestamp, source, stream, count, method)
            SELECT * FROM (
                SELECT
                    minute,
                    'hls'::viewcount_source AS source,
                    stream,
                    COUNT(*),
                    'ip'::viewcount_method AS method
                FROM (
                    SELECT DISTINCT
                        DATE_TRUNC('minute', timestamp) as minute,
                        ip,
                        stream
                    FROM hls_pl_loads
                ) AS tmp GROUP BY minute, stream
            ) AS tmp
            UNION
            SELECT * FROM (
                SELECT
                    minute,
                    'hls'::viewcount_source AS source,
                    stream,
                    COUNT(*),
                    'ip+uid'::viewcount_method AS method
                FROM (
                    SELECT DISTINCT
                        DATE_TRUNC('minute', timestamp) as minute,
                        ip,
                        uid,
                        stream
                    FROM hls_pl_loads
                ) AS tmp
                GROUP BY minute, stream
            ) AS tmp
            UNION
            SELECT * FROM (
                SELECT
                    minute,
                    'hls'::viewcount_source AS source,
                    stream,
                    COUNT(*),
                    'uid'::viewcount_method AS method
                FROM (
                    SELECT DISTINCT
                        DATE_TRUNC('minute', timestamp) as minute,
                        uid,
                        stream
                    FROM hls_pl_loads
                ) AS tmp
                GROUP BY minute, stream
            ) AS tmp
            ORDER BY minute
            ON CONFLICT ON CONSTRAINT viewcount_pkey DO
                UPDATE SET count = EXCLUDED.count
            """)
        if rollback:
            conn.rollback()
        conn.commit()
        # Don't keep IP info longer than we need
        cur.execute("""
            DELETE FROM hls_pl_loads
            WHERE timestamp < NOW() - INTERVAL '12 hours'""")

        # Copies the latest one into viewers_latest
        cur.execute("DELETE FROM viewers_latest WHERE source = 'hls'")
        cur.execute("""
            INSERT INTO viewers_latest (source, stream, method, count)
            SELECT
                'hls'::viewcount_source AS source,
                stream,
                'ip'::viewcount_method AS method,
                COUNT(*) AS count
            FROM (
                SELECT DISTINCT
                    ip,
                    stream
                FROM hls_pl_loads
                WHERE timestamp >= NOW() - INTERVAL '1 minute'
            ) AS tmp
            GROUP BY stream
            UNION
            SELECT
                'hls'::viewcount_source AS source,
                stream,
                'ip+uid'::viewcount_method AS method,
                COUNT(*) AS count
            FROM (
                SELECT DISTINCT
                    ip,
                    uid,
                    stream FROM hls_pl_loads
                WHERE timestamp >= NOW() - INTERVAL '1 minute'
            ) AS tmp
            GROUP BY stream
            UNION
            SELECT
                'hls'::viewcount_source AS source,
                stream,
                'uid'::viewcount_method AS method,
                COUNT(*) AS count
            FROM (
                SELECT DISTINCT
                    uid,
                    stream
                FROM hls_pl_loads
                WHERE timestamp >= NOW() - INTERVAL '1 minute'
            ) AS tmp
            GROUP BY stream
            """)
        if rollback:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        cur.execute("""
            SELECT stream, count FROM viewers_latest
            WHERE timestamp > NOW() - INTERVAL '10 minutes'
                AND source = 'hls'
                AND method=%s
            """, (config['hls']['method'],))
        for row in cur:
            zbx_metrics.add(ViewerMetric("hls", *row))
    except Exception as e:
        conn.rollback()
        print(e)

# RTMP
rtmp_app = "live"
if True:
    try:
        streamcounts = {}
        edges = config['rtmp']['edges'].split(',')
        for edge in edges:
            vprint("Fetching RTMP stats for", edge)
            r = s.get(
                "https://" + edge + '/rtmp_status',
                auth=(
                    config['rtmp']['http_basic_username'],
                    config['rtmp']['http_basic_password']
                )
            )
            apps = xmltodict.parse(r.text)['rtmp']['server']['application']
            for app in apps:
                if app['name'] != rtmp_app:
                    continue
                if 'live' not in app or 'stream' not in app['live']:
                    continue
                streams = app['live']['stream']
                # If there's only one stream, it's just the object
                if isinstance(streams, dict):
                    streams = [streams]
                for stream in streams:
                    # -1 is to remove the client that's publishing the stream
                    viewers = int(stream['nclients']) - 1
                    if viewers < 1:
                        continue
                    vprint("Viewers for rtmp://{}/{}/{} - {}".format(
                        edge,
                        app['name'],
                        stream['name'],
                        viewers
                    ))
                    if stream['name'] not in streamcounts:
                        streamcounts[stream['name']] = 0
                    streamcounts[stream['name']] += viewers
        # History table
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO viewcount (source, stream, count, method) VALUES %s",
            streamcounts.items(),
            template="('rtmp', %s, %s, 'live')"
        )
        if rollback:
            conn.rollback()
        conn.commit()
        # Current table
        cur.execute("DELETE FROM viewers_latest WHERE source = 'rtmp'")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO viewers_latest (source, stream, method, count)
            VALUES %s
            """,
            streamcounts.items(),
            template="('rtmp', %s, 'live', %s)"
        )
        if rollback:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        for stream, count in streamcounts.items():
            zbx_metrics.add(ViewerMetric("rtmp", stream, count))
    except Exception as e:
        conn.rollback()
        print(e)

# Icecast
if True:
    try:
        icecast_server = config['icecast']['server']
        vprint("Fetching stats for", icecast_server)
        r = s.get(icecast_server + '/status-json.xsl')
        sources = r.json()['icestats']['source']
        streamcounts = {}
        # If there's only one stream, it's just the object
        if isinstance(sources, dict):
            sources = [sources]
        for source in sources:
            stream = source['listenurl'].split("/")[-1]
            viewers = source['listeners']
            vprint("Listeners for {}:".format(source['listenurl']), viewers)
            if viewers > 0:
                streamcounts[stream] = viewers
        # History table
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO viewcount (source, stream, count, method) VALUES %s",
            streamcounts.items(),
            template="('icecast', %s, %s, 'live')"
        )
        if rollback:
            conn.rollback()
        conn.commit()
        # Current table
        cur.execute("DELETE FROM viewers_latest WHERE source = 'icecast'")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO viewers_latest (source, stream, method, count)
            VALUES %s
            """,
            streamcounts.items(),
            template="('icecast', %s, 'live', %s)"
        )
        if rollback:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        for stream, count in streamcounts.items():
            zbx_metrics.add(ViewerMetric("icecast", stream, count))
    except Exception as e:
        conn.rollback()
        print(e)

# Youtube
if True:
    try:
        ytconf = config['youtube']
        vprint("Fetching live videos for yt channel "
               "https://youtube.com/channel/" + ytconf['channel_id'])
        r = s.get(
            ('https://www.googleapis.com/youtube/v3/search?'
             'part=id'
             '&channelId={}'
             '&eventType=live'
             '&type=video'
             '&key={}').format(
                 ytconf['channel_id'],
                 ytconf['api_key']
            ),
            timeout=int(config['global']['timeout'])
        )
        videos = [item['id']['videoId'] for item in r.json()['items']]
        streamcounts = {}
        for video in videos:
            vprint("Fetching info for https://youtu.be/"+video)
            r = s.get(
                ('https://www.googleapis.com/youtube/v3/videos?'
                 'id={}'
                 '&part=liveStreamingDetails'
                 '&key={}').format(
                     video,
                     ytconf['api_key']
                ),
                timeout=int(config['global']['timeout'])
            )
            # Slightly odd linebreaking here, but this is the same as if
            # all of the bracketed strings were one after the other. Written
            # like this to keep the lines <79 chars, as per PEP8.
            viewers = int(
                r.json()
                ['items']
                [0]
                ['liveStreamingDetails']
                ['concurrentViewers']
            )
            vprint("Viewers of https://youtu.be/{}:".format(video), viewers)
            if viewers > 0:
                streamcounts[video] = viewers
        # History table
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO viewcount (source, stream, count, method) VALUES %s",
            streamcounts.items(),
            template="('youtube', %s, %s, 'live')"
        )
        conn.commit()
        # Current table
        cur.execute("DELETE FROM viewers_latest WHERE source = 'youtube'")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO viewers_latest (source, stream, method, count)
            VALUES %s
            """,
            streamcounts.items(),
            template="('youtube', %s, 'live', %s)"
        )
        if rollback:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        zbx_metrics.add(ViewerMetric(
            "youtube",
            "live",
            sum(streamcounts.values())
        ))
    except Exception as e:
        conn.rollback()
        print(e)

###
# Publish to Zabbix
###
zbx_data = ""
for metric in list(zbx_metrics):
    # Create metric for the viewer count
    zbx_data += '"{}" trapper.streams.viewers[{},{}] {}\n'.format(
        config['zabbix']['client_hostname'],
        metric.source,
        metric.stream,
        metric.viewers
    )

dprint("Zabbix batch data:", zbx_data, sep="\n")

# Shell out to zabbix_sender, because we use encryption, and that seems to be
# beyond the capabilities of any of the native Python libraries for Zabbix.
pipe = run(
    ["zabbix_sender", "-c", "/etc/zabbix/zabbix_agentd.conf", "-i", "-"],
    stdout=(stdout if debug else PIPE),
    input=zbx_data.encode('utf-8')
)

# vim: set ts=8 sw=4 tw=79 et :
