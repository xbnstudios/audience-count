# Audience Count
Track and present audience count from HLS, RTMP, Icecast, and Youtube

This project consists of two parts:
- The python script: Aggregates m3u8 & live data into history table and zabbix
- The hls branch: Tracks m3u8 loads, presents current data


## How to use
Create a postgres database and user, and import the schema.
Copy `config.ini.example` to `config.ini` and set up the credentials, and
add the following crontab entry to run every minute:
```crontab
# m h dom mon dow command
* * * * * /path/to/audience-count/audience-count.py --cron
```

Clone the hls branch, and configure nginx to use the php script
for all HLS playlist requests:
```nginx
    # Actually loads the playlist file
    location /real/hls {
        internal;
        alias /tmp/hls;
    }
    # Serves video segments
    location /hls {
        root /tmp;
    }
    # Grabs playlist loads for PHP
    location ~ ^/hls/.*/.*\.m3u8(?:\?.*)?$ {
        rewrite ^ /audience-count/hls.php;
    }
```

And each edge needs an rtmp status endpoint:
```nginx
    location /rtmp_status {
        rtmp_stat all;
        auth_basic "Login";
        auth_basic_user_file /etc/nginx/htpasswd_rtmp_status;
    }
```
