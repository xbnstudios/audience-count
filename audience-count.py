#!/usr/bin/env python3

# Collects the audience statistics for the last minute

import configparser
from datetime import datetime
import logging
import psycopg2
import psycopg2.extras
import requests
from subprocess import run, PIPE
from sys import argv, stdout
import xmltodict

config = configparser.ConfigParser()
config.read('config.ini')

valid_flags = {
        "--help": "Show this text",
        "--debug": "Fill your screen with noise",
        "--dry-run": "Don't make any changes",
        "--yt-update": "Don't use cached YT search results",
        "--cron": "Be quiet and don't explode on failures"
        }
for flag in argv[1:]:
    if flag not in valid_flags:
        print("Unknown flag '{}', try --help".format(flag))
        exit(1)

if "--help" in argv:
    print("Usage: {} [arguments]".format(argv[0]))
    for item in valid_flags.items():
        print("\t{: <12}\t{}".format(*item))
    exit(0)

cron = "--cron" in argv
dry_run = "--dry-run" in argv
yt_force_update = "--yt-update" in argv

log = logging.getLogger(__name__)
if "--debug" in argv:
    log.setLevel(logging.DEBUG)
elif cron:
    log.setLevel(logging.WARNING)
else:
    log.setLevel(logging.INFO)
logging.basicConfig(level=log.getEffectiveLevel())

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

class ViewerMetric:
    """A single metric about viewership."""

    def __init__(self, source: str, stream: str, viewers: int):
        """Create a new instance.
        :source: The source from which this metric comes
            (icecast, youtube, rtmp, hls)
        :stream: The slug for the stream that this metric came from
            (xbn-360, xbn-1080, xbn-flac, etc.)
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
            ("icecast", "xbn-flac"),
            ("icecast", "xbn-320"),
            ("icecast", "xbn-128"),
            ("icecast", "xbn-32"),
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

# {{{ HLS
if "hls" in config:
    try:
        log.info("Running queries for HLS (no output)")
        # {{{ Aggregate PL loads into various "views last minute" methods
        # Note: Probably wrong for *current minute*. viewers_latest is correct
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
        # }}}
        if dry_run:
            conn.rollback()
        conn.commit()
        # Don't keep IP info longer than we need
        cur.execute("""
            DELETE FROM hls_pl_loads
            WHERE timestamp < NOW() - INTERVAL '12 hours'""")

        # {{{ Copy the latest stats into viewers_latest
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
        # }}}
        if dry_run:
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
        if cron:
            log.error("Exception: %s", e)
        else:
            raise e
# }}}

# {{{ RTMP
rtmp_app = "live"
if "rtmp" in config:
    try:
        streamcounts = {}
        edges = config['rtmp']['edges'].split(',')
        for edge in edges:
            log.info("Fetching RTMP stats for %s", edge)
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
                    log.info("Viewers for rtmp://{}/{}/{} - {}".format(
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
        if dry_run:
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
        if dry_run:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        for stream, count in streamcounts.items():
            zbx_metrics.add(ViewerMetric("rtmp", stream, count))
    except Exception as e:
        conn.rollback()
        if cron:
            log.error("Exception: %s", e)
        else:
            raise e
# }}}

# {{{ Icecast
if "icecast" in config:
    try:
        icecast_server = config['icecast']['server']
        log.info("Fetching stats for %s", icecast_server)
        r = s.get(icecast_server + '/status-json.xsl')
        sources = r.json()['icestats'].get('source', [])
        streamcounts = {}
        # If there's only one stream, it's just the object
        if isinstance(sources, dict):
            sources = [sources]
        for source in sources:
            stream = source['listenurl'].split("/")[-1]
            viewers = source['listeners']
            log.info("Listeners for %s: %s", source['listenurl'], viewers)
            if viewers > 0:
                streamcounts[stream] = viewers
        # History table
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO viewcount (source, stream, count, method) VALUES %s",
            streamcounts.items(),
            template="('icecast', %s, %s, 'live')"
        )
        if dry_run:
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
        if dry_run:
            conn.rollback()
        conn.commit()

        # Build stats for zabbix
        for stream, count in streamcounts.items():
            zbx_metrics.add(ViewerMetric("icecast", stream, count))
    except Exception as e:
        conn.rollback()
        if cron:
            log.error("Exception: %s", e)
        else:
            raise e
# }}}

# {{{ Youtube
if "youtube" in config:
    try:
        ytconf = config['youtube']
        if datetime.now().minute % 5 == 0 or yt_force_update:
            log.info("Fetching live videos for yt channel "
                   "https://youtube.com/channel/" + ytconf['channel_id'])
            r = s.get(
                ('https://www.googleapis.com/youtube/v3/search?'
                 'part=id'
                 '&safeSearch=none'
                 '&channelId={}'
                 '&eventType=live'
                 '&type=video'
                 '&key={}').format(
                     ytconf['channel_id'],
                     ytconf['api_key']
                ),
                timeout=int(config['general']['timeout'])
            )
            if r.status_code != 200:
                raise Exception(("Unexpected response from YouTube Search API: "
                        "{}\n{}".format(r.status_code, r.text)))
            videos = [item['id']['videoId'] for item in r.json()['items']]
        else: # Use cached video list
            log.info("Using cached YouTube video list")
            cur.execute(("SELECT stream FROM viewers_latest WHERE source='youtube' "
                        "AND timestamp > NOW() - INTERVAL '10 minutes'"))
            videos = [x[0] for x in cur]
        streamcounts = {}
        for video in videos:
            log.info("Fetching info for https://youtu.be/%s", video)
            r = s.get(
                ('https://www.googleapis.com/youtube/v3/videos?'
                 'id={}'
                 '&part=liveStreamingDetails'
                 '&key={}').format(
                     video,
                     ytconf['api_key']
                ),
                timeout=int(config['general']['timeout'])
            )
            if r.status_code != 200:
                raise Exception(("Unexpected response from YouTube Videos API: "
                        "{} {}".format(r.status_code, r.text)))

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
            log.info("Viewers of https://youtu.be/%s - %s", video, viewers)
            if viewers > 0:
                streamcounts[video] = viewers
        # History table
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO viewcount (source, stream, count, method) VALUES %s",
            streamcounts.items(),
            template="('youtube', %s, %s, 'live')"
        )
        if dry_run:
            conn.rollback()
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
        if dry_run:
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
        if cron:
            log.error("Exception: %s", e)
        else:
            raise e
# }}}

###
# {{{ Publish to Zabbix
###
if "zabbix" in config:
    zbx_data = ""
    for metric in list(zbx_metrics):
        # Create metric for the viewer count
        zbx_data += '"{}" trapper.streams.viewers[{},{}] {}\n'.format(
            config['zabbix']['client_hostname'],
            metric.source,
            metric.stream,
            metric.viewers
        )

    log.debug("Zabbix batch data:\n%s", zbx_data)

    # Shell out to zabbix_sender, because we use encryption, and
    # that seems to be beyond the capabilities of any of the native
    # Python libraries for Zabbix / SSL.
    if not dry_run:
        pipe = run(
            ["zabbix_sender",
                "-c", "/etc/zabbix/zabbix_agentd.conf",
                "-i", "-"],
            stdout=(stdout if log.isEnabledFor(logging.DEBUG) else PIPE),
            input=zbx_data.encode('utf-8')
    )
# }}}

# vim: set ts=8 sw=4 tw=79 et :
