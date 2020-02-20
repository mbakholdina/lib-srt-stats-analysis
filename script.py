""" TODO """
import pathlib

# import matplotlib.pyplot as plt
import pandas as pd
# import plotly.graph_objects as go
# import seaborn as sns

from tcpdump_processing import convert, extract_packets


def main():
    # Set filepaths to the source files: sender and receiver SRT core .csv statistics, tshark .pcapng dumps collected on both sides
    STATS_SND = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    STATS_RCV = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/2-tshark-tracefile.pcapng'

    # Set the list of SRT statistics features to analyze
    SND_FEATURES = [
        # 'pktFlowWindow',
        # 'pktCongestionWindow',
        # 'pktFlightSize',
        # 'msRTT',
        # 'mbpsBandwidth',
        'pktSent',
        # 'pktSndLoss',
        # 'pktSndDrop',
        # 'pktRetrans',
        # 'byteSent',
        # 'byteSndDrop',
        # 'mbpsSendRate',
        # 'usPktSndPeriod',
    ]

    RCV_FEATURES = [
        # 'msRTT',
        # 'mbpsBandwidth',
        'pktRecv',
        # 'pktRcvLoss',
        # 'pktRcvDrop',
        # 'pktRcvRetrans',
        # 'pktRcvBelated',
        # 'byteRecv',
        # 'byteRcvLoss',
        # 'byteRcvDrop',
        # 'mbpsRecvRate',
    ]

    # Load SRT statistics from sender and receiver side to dataframes snd_stats and rcv_stats respectively, perform some basic data preparation
    snd_stats = pd.read_csv(STATS_SND, index_col='Timepoint', parse_dates=True)
    rcv_stats = pd.read_csv(STATS_RCV, index_col='Timepoint', parse_dates=True)

    snd_stats = snd_stats[SND_FEATURES]
    rcv_stats = rcv_stats[RCV_FEATURES]

    snd_stats.index = snd_stats.index.tz_convert(None)
    rcv_stats.index = rcv_stats.index.tz_convert(None)

    print('Sender statisitcs')
    print(snd_stats.head(10))
    print(snd_stats.tail(10))
    print('Receiver statistics')
    print(rcv_stats.head(10))
    print(rcv_stats.tail(10))

    # TODO: Adjust clocks

    # Combine sender and receiver data sets using linear interpolation.
    # TODO: Experiment with different interpolation methods for different features.
    # rcv_stats['isSender'] = False
    snd_stats = snd_stats.add_suffix('_snd')
    rcv_stats = rcv_stats.add_suffix('_rcv')
    snd_stats['isSender'] = True
    stats = snd_stats.join(rcv_stats, how='outer')
    # Cut dataframe by sender time
    # TODO: cut the time properly depending on different situations
    start_timestamp = snd_stats.index[0]
    end_timestamp = snd_stats.index[-1]
    stats = stats[(stats.index >= start_timestamp) & (stats.index <= end_timestamp)]
    # TODO: Fill NA values for the start point properly
    # stats.loc[start_timestamp, 'pktRecv_rcv'] = 0

    stats['isSender'] = stats['isSender'].fillna(False)
    stats['timeDiff'] = stats.index.to_series().diff().fillna(pd.to_timedelta(0))
    stats['timeDiffShifted'] = stats['timeDiff'].shift().fillna(pd.to_timedelta(0))
    
    print('Combined stats')
    print(stats.info())
    print(stats.head(20))
    print(stats.tail(20))

    df = stats[stats['isSender'] == False]
    df = df[['isSender', 'pktRecv_rcv', 'timeDiffShifted', 'timeDiff']]
    df = df.rename(columns={'timeDiffShifted': 'timeDiff_p1', 'timeDiff': 'timeDiff_p2'})

    df['timeDiff'] = df.index.to_series().diff()
    df['timeDiff'] = df['timeDiff'].fillna(df['timeDiff'].mean())
    
    df['timeDiff_p1'] = df['timeDiff_p1'] / df['timeDiff']
    df['timeDiff_p2'] = df['timeDiff_p2'] / df['timeDiff']

    df['pktRecv_rcv_p1'] = (df['timeDiff_p1'] * df['pktRecv_rcv']).round().astype('int32')
    df['pktRecv_rcv_p2'] = (df['timeDiff_p2'] * df['pktRecv_rcv']).round().astype('int32')

    df['pktRecv_rcv_new'] = df['pktRecv_rcv_p1'] + df['pktRecv_rcv_p2'].shift().fillna(0)

    print(df.head(20))
    print(df.tail(20))
    
    result = snd_stats.join(df['pktRecv_rcv_new'], how='outer')
    result['pktRecv_rcv_new'] = result['pktRecv_rcv_new'].shift(-1)

    print('Result')
    print(result.head(20))

    result['isSender'] = result['isSender'].fillna(False)
    result = result[result['isSender']]
    print(result.head(20))
    
    # SRT statistics vs Thark dump
    # Extract SRT packets from .pcapng tshark dump file:

    # RCV_TSHARK_CSV = convert.convert_to_csv(pathlib.Path(RCV_TSHARK_PCAPNG), True)
    # RCV_TSHARK_CSV = convert.convert_to_csv(pathlib.Path(RCV_TSHARK_PCAPNG))
    # rcv_srt_packets = extract_packets.extract_srt_packets(RCV_TSHARK_CSV)
    # rcv_srt_packets.head(10)


if __name__ == '__main__':
    main()