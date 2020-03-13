# lib-srt-stats-analysis

A library designed to work with SRT core statistics and `tshark` datasets.

# Getting Started

## Requirements

* python 3.6+

Please follow the [instructions](https://github.com/mbakholdina/lib-tcpdump-processing#getting-started) to get started with library installation and usage.

# Executable scripts

```
venv/bin/python -m srt_stats_analysis.join_stats
```

# Documentation

The notes on aligning the datasets can be found [here](docs/notes.md).

# ToDo

* Rename the library to lib-srt-stats.
* Add remain features in align_srt_stats function.
* Script with plotting result dataframes.
* SRT statistics documentation:
    - Note regarding the first data point (the time of connection),
    - Note regarding aggregated and not aggregated statistics,
    - Correct mbpsBandwidth description
    - Note regarding relative time and new absolute time.
* srt-live-transmit:
    - Ticket: Add absolute time points plus first data point when connection has been established
* Test caller-rcv and listener-sender use case.
* Extract initial RTT from handshakes and adjust receiver and sender clocks.
* Experiment with shifting receiver statistics by RTT/2 (as a start, we can shift by initial RTT calculated from handshakes).
* Extract RTT estimation from acknoledgement packets and experiment with shifting receiver statistics by RTT/2.

Do not forget about issues found:
- Missing ACKACK - write an issue on github.
- mbpsBandwidth - some strange behaviour at the beginning.
- Initial RTT estimation is equal to 100ms and then slowly converges to the real value 60-65ms. Think about proper initial RTT value.