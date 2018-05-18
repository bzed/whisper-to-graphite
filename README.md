# whisper-to-graphite [![Build Status](https://api.travis-ci.org/bzed/whisper-to-graphite.svg?branch=master)](https://travis-ci.org/bzed/whisper-to-graphite/)
Read and send metrics from whisper files to graphite - Used to migrate to different graphite backends

Basically this little helper calculated the metric name from the filename of a whisper file
and sends all timestamp/value tuples using the graphite protocol. TCP and UDP are supported.
Additionally there is a NOP protocol which logs all data instead of sending it.

## Usage

```
% ./whisper-to-graphite -h
Usage of ./whisper-to-graphite:
  -basedirectory string
    	Base directory where whisper files are located. Used to retrieve the metric name from the filename. (default "/var/lib/graphite/whisper")
  -directory string
    	Directory containing the whisper files you want to send to graphite again (default "/var/lib/graphite/whisper/collectd")
  -host string
    	Hostname/IP of the graphite server (default "127.0.0.1")
  -port int
    	graphite Port (default 2003)
  -pps int
    	Number of maximum points per second to send (0 means rate limiter is disabled)
  -protocol string
    	Protocol to use to transfer graphite data (tcp/udp/nop) (default "tcp")
  -workers int
    	Workers to run in parallel (default 5)

```

Assuming you don't want to use this as testcase for your IO subsystem,
you might want to ensure that you don't send the data to the host you are reading it from.

