"""
Module designed to align SRT core statistics obtained from receiver
and sender as well as tshark datasets if necessary.
"""
import pathlib

import pandas as pd

from tcpdump_processing import convert, extract_packets


# Without Ethernet packet overhead, bytes
SRT_DATA_PACKET_HEADER_SIZE = 44
# TODO: Make proper separation: 1316 - live mode, 1456 - file mode
SRT_DATA_PACKET_PAYLOAD_SIZE = 1316
# SRT_DATA_PACKET_PAYLOAD_SIZE = 1456


def convert_pktsps_in_bytesps(value):
    return value * (SRT_DATA_PACKET_HEADER_SIZE + SRT_DATA_PACKET_PAYLOAD_SIZE)


def convert_bytesps_in_mbps(value):
    return value * 8 / 1000000


def align_srt_stats(snd_stats_path: str, rcv_stats_path: str):
    """
    Align SRT core statistics obtained from receiver and sender.

    Attributes:
        snd_stats_path:
            Filepath to .csv statistics collected at the sender side.
        rcv_stats_path:
            Filepath to .csv statistics collected at the receiver side.
    """
    # Set the list of SRT statistics features to analyze
    SND_FEATURES = [
        # 'pktFlowWindow',
        # 'pktCongestionWindow',
        # 'pktFlightSize',
        'msRTT',
        'mbpsBandwidth',
        'pktSent',              # aggregated
        'pktSndLoss',           # aggregated
        # 'pktSndDrop',
        # 'pktRetrans',
        # 'byteSent',
        # 'byteSndDrop',
        # 'mbpsSendRate',
        # 'usPktSndPeriod',
    ]

    RCV_FEATURES = [
        'msRTT',
        'mbpsBandwidth',
        'pktRecv',              # aggregated
        'pktRcvLoss',           # aggregated
        # 'pktRcvDrop',
        # 'pktRcvRetrans',
        # 'pktRcvBelated',
        # 'byteRecv',
        # 'byteRcvLoss',
        # 'byteRcvDrop',
        # 'mbpsRecvRate',
    ]

    # Load SRT statistics from sender and receiver side to dataframes 
    # snd_stats and rcv_stats respectively and extract features of interest
    snd_stats = pd.read_csv(snd_stats_path, index_col='Timepoint', parse_dates=True)
    rcv_stats = pd.read_csv(rcv_stats_path, index_col='Timepoint', parse_dates=True)
    snd_stats = snd_stats[SND_FEATURES]
    rcv_stats = rcv_stats[RCV_FEATURES]

    # Convert timezones to UTC+0
    snd_stats.index = snd_stats.index.tz_convert(None)
    rcv_stats.index = rcv_stats.index.tz_convert(None)

    print('\nSender stats')
    print(snd_stats.head(10))
    print(snd_stats.tail(10))
    print('\nReceiver stats')
    print(rcv_stats.head(10))
    print(rcv_stats.tail(10))

    # TODO: Adjust clocks

    # Combine sender and receiver datasets into stats dataframe
    snd_stats = snd_stats.add_suffix('_snd')
    rcv_stats = rcv_stats.add_suffix('_rcv')
    snd_stats['isSender'] = True
    stats = snd_stats.join(rcv_stats, how='outer')
    stats['isSender'] = stats['isSender'].fillna(False)

    # Further we will use sender timepoints to align the stats from 
    # receiver and sender
    # To do so, first we cut the time points on top and at the bottom 
    # of stats dataframe where statistics was collected only 
    # on receiver or sender side
    # TODO: I've tested caller-snd and listener-rcv setup.
    # Check additionally how this behaves in case of caller-rcv and 
    # listener-snd setup
    start_timestamp = max(snd_stats.index[0], rcv_stats.index[0])
    end_timestamp = min(snd_stats.index[-1], rcv_stats.index[-1])
    stats = stats[(stats.index >= start_timestamp) & (stats.index <= end_timestamp)]

    # Second, we check that the first and the last timepoints are both
    # sender timepoints. If not, drop them.
    if not stats['isSender'][0]:
        stats = stats[1:]

    if not stats['isSender'][-1]:
        stats = stats[:-1]
    
    print('\nJoined stats')
    print(stats.head(10))
    print(stats.tail(10))

    # Do linear interpolation for features where applicable
    cols_to_round = [
        'pktSent_snd',
        'pktSndLoss_snd',
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    stats.loc[:, stats.columns != 'isSender'] = stats.interpolate().fillna(method='bfill')
    stats.loc[:, cols_to_round] = stats.round()

    print('\nInterpolated stats')
    print(stats.head(10))
    print(stats.tail(10))

    # Extract only sender timepoints
    stats = stats[stats['isSender']]

    # Rearrange the columns
    cols_rearranged = [
        'pktSent_snd',
        'pktRecv_rcv',
        'pktSndLoss_snd',
        'pktRcvLoss_rcv',
        'msRTT_snd',
        'msRTT_rcv',
        'mbpsBandwidth_snd',
        'mbpsBandwidth_rcv'
    ]
    stats = stats[cols_rearranged]

    return stats


# TODO: Under development
def align_srt_tshark_stats(stats: pd.DataFrame, tshark_stats_path: str):
    """
    Align SRT stats and tshark 
    """
    # Extract SRT packets from .pcapng tshark dump file
    srt_packets = extract_packets.extract_srt_packets(tshark_stats_path)
    srt_packets.head(10)

    print(srt_packets.empty)

    # Extract UMSG_ACK packets from SRT packets rcv_srt_packets which 
    # contains receiving speed and bandwidth estimations reported by
    # receiver each 10 ms:
    umsg_ack_packets = extract_packets.extract_umsg_ack_packets(srt_packets)
    umsg_ack_packets.head(10)

    # From rcv_umsg_ack_packets dataframe, extract features valuable 
    # for further analysis, do some data cleaning and timezone correction
    TSHARK_FEATURES = ['ws.no', 'frame.time', 'srt.rate', 'srt.bw', 'srt.rcvrate']
    umsg_ack_packets = umsg_ack_packets[TSHARK_FEATURES]
    umsg_ack_packets = umsg_ack_packets.set_index('frame.time')
    umsg_ack_packets.index = umsg_ack_packets.index.tz_convert(None)
    umsg_ack_packets = umsg_ack_packets.rename(columns={"srt.rate": "srt.rate.pkts", "srt.bw": "srt.bw.pkts", "srt.rcvrate": "srt.rate.Bps"})
    umsg_ack_packets['srt.rate.Mbps'] = convert_bytesps_in_mbps(umsg_ack_packets['srt.rate.Bps'])
    umsg_ack_packets['srt.bw.Mbps'] = convert_bytesps_in_mbps(convert_pktsps_in_bytesps(umsg_ack_packets['srt.bw.pkts']))
    umsg_ack_packets = umsg_ack_packets[['ws.no', 'srt.rate.pkts', 'srt.rate.Mbps', 'srt.bw.pkts', 'srt.bw.Mbps']]
    umsg_ack_packets.head()


def main():
    # Set filepaths to the source files: sender and receiver SRT core .csv statistics, tshark .pcapng dumps collected on both sides
    # SND_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    # RCV_STATS_PATH = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    # RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_15Mbps/msharabayko@40.69.89.21/2-tshark-tracefile.pcapng'

    SND_STATS_PATH = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    RCV_STATS_PATH = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    SND_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@23.96.93.54/1-tshark-tracefile-snd.pcapng'
    # RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/2-tshark-tracefile-rcv.pcapng'

    # For the first time
    # SND_TSHARK_CSV = convert.convert_to_csv(pathlib.Path(SND_TSHARK_PCAPNG), True)
    # For the following time
    # SND_TSHARK_CSV = convert.convert_to_csv(pathlib.Path(SND_TSHARK_PCAPNG))

    # Align SRT statisitcs obtained from receiver and sender
    stats = align_srt_stats(SND_STATS_PATH, RCV_STATS_PATH)

    print('\nAligned sender and receiver statistics')
    print(stats.head(10))
    print(stats.tail(10))

    # Align SRT stats and tshark datasets
    # align_srt_tshark_stats(stats, SND_TSHARK_CSV)


if __name__ == '__main__':
    main()