"""
Module designed to align SRT core statistics obtained from receiver
and sender as well as tshark datasets if necessary.
"""
import pathlib

import pandas as pd

from tcpdump_processing.convert import convert_to_csv
from tcpdump_processing.extract_packets import extract_srt_packets, extract_umsg_handshake_packets, extract_umsg_ack_packets


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
    snd_stats = pd.read_csv(snd_stats_path, index_col='Timepoint')
    rcv_stats = pd.read_csv(rcv_stats_path, index_col='Timepoint')
    snd_stats = snd_stats[SND_FEATURES]
    rcv_stats = rcv_stats[RCV_FEATURES]

    # Convert index to datetime64 specifying the timepoint format
    form = '%d.%m.%Y %H:%M:%S.%f %z'
    snd_stats.index = pd.to_datetime(snd_stats.index, format=form)
    rcv_stats.index = pd.to_datetime(rcv_stats.index, format=form)

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
    cols_to_int = [
        'pktSent_snd',
        'pktSndLoss_snd',
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    cols_to_round = [
        'msRTT_snd',
        'msRTT_rcv',
        'mbpsBandwidth_snd',
        'mbpsBandwidth_rcv'
    ]
    stats.loc[:, stats.columns != 'isSender'] = stats.interpolate().fillna(method='bfill')
    stats.loc[:, cols_to_int] = stats.astype('int32')
    stats.loc[:, cols_to_round] = stats.round(2)

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


def align_srt_tshark_stats(stats: pd.DataFrame, rcv_tshark_csv: str):
    """
    Align SRT statistics and tshark data.

    Attributes:
        stats: 
            Aligned SRT statisitcs collected both at the receiver
            and sender sides, the output from align_srt_stats function.
        rcv_tshark_csv:
            Filepath to .csv thark data collected at the receiver side.
    """
    print('\nMerging tshark data with SRT statistics')

    # Extract SRT packets from .csv tshark dump file collected at the receiver side
    srt_packets = extract_srt_packets(rcv_tshark_csv)

    print('\nSRT packets extracted from receiver tshark dump')
    print(srt_packets.head(10))

    # Extract UMSG_ACK packets from SRT packets srt_packets that
    # contain receiving speed and bandwidth estimations reported by
    # receiver each 10 ms
    umsg_ack_packets = extract_umsg_ack_packets(srt_packets)

    print('\nUMSG_ACK packets extracted from SRT packets')
    print(umsg_ack_packets.head(10))

    # From umsg_ack_packets dataframe, extract features valuable 
    # for further analysis, do some data cleaning and timezone correction
    TSHARK_FEATURES = [
        'ws.no',
        'frame.time',
        'srt.rate',
        'srt.bw',
        'srt.rcvrate'
    ]
    umsg_ack_packets = umsg_ack_packets[TSHARK_FEATURES]
    umsg_ack_packets = umsg_ack_packets.set_index('frame.time')
    umsg_ack_packets.index = umsg_ack_packets.index.tz_convert(None)
    umsg_ack_packets = umsg_ack_packets.rename(
        columns={
            "srt.rate": "srt.rate.pkts",
            "srt.bw": "srt.bw.pkts",
            "srt.rcvrate": "srt.rate.Bps"
        }
    )
    umsg_ack_packets['srt.rate.Mbps'] = convert_bytesps_in_mbps(
        umsg_ack_packets['srt.rate.Bps']
    )
    umsg_ack_packets['srt.bw.Mbps'] = convert_bytesps_in_mbps(
        convert_pktsps_in_bytesps(umsg_ack_packets['srt.bw.pkts'])
    )
    umsg_ack_packets = umsg_ack_packets[
        [
            'ws.no',
            'srt.rate.pkts',
            'srt.rate.Mbps',
            'srt.bw.pkts',
            'srt.bw.Mbps'
        ]
    ]

    print('\nAdjusted UMSG_ACK packets')
    print(umsg_ack_packets.head(10))
    print(umsg_ack_packets.tail(10))

    # Combine stats dataframe (with SRT statistics) and adjusted 
    # umsg_ack_packets dataframe. stats dataframe timepoints will be
    # further used as the timepoints for result dataframe
    start_timestamp = stats.index[0]
    end_timestamp = stats.index[-1]
    
    stats['isStats'] = True
    cols = ['srt.rate.Mbps', 'srt.bw.Mbps']
    df = stats.join(umsg_ack_packets[cols].add_suffix('_tshark'), how='outer')
    df['isStats'] = df['isStats'].fillna(False)

    df = df[(df.index >= start_timestamp) & (df.index <= end_timestamp)]
    assert(df['isStats'][0] == True)
    assert(df['isStats'][-1] == True)

    print('\nJoined SRT stats and tshark statistics')
    print(df.head(10))
    print(df.tail(10))

    # Do interpolation
    cols_to_interpolate = [f'{col}_tshark' for col in cols]
    df.loc[:, cols_to_interpolate] = df.interpolate().fillna(method='bfill')
    df.loc[:, cols_to_interpolate] = df.round(2)

    print('\nInterpolated tshark statistics')
    print(df.head(10))
    print(df.tail(10))

    # Extract only stats dataframe timepoints (aligned SRT stats timepoints)
    df = df.loc[df['isStats'], df.columns != 'isStats']

    cols_to_int = [
        'pktSent_snd',
        'pktSndLoss_snd',
        'pktRecv_rcv',
        'pktRcvLoss_rcv',
    ]
    # TODO: Does not work
    # df.loc[:, cols_to_int] = df.astype('int32')
    for col in cols_to_int:
        df[col] = df[col].astype('int32')

    print('\nOnly SRT stats timepoints')
    print(df.head(10))
    print(df.tail(10))

    # Rearrange the columns
    cols_rearranged = [
        'pktSent_snd',
        'pktRecv_rcv',
        'pktSndLoss_snd',
        'pktRcvLoss_rcv',
        'msRTT_snd',
        'msRTT_rcv',
        'mbpsBandwidth_snd',
        'mbpsBandwidth_rcv',
        'srt.bw.Mbps_tshark',
        'srt.rate.Mbps_tshark'
    ]
    df = df[cols_rearranged]

    return df


# TODO: Under development
def check_time_difference(clr_tshark_csv: str, list_tshark_csv: str):
    """ TODO """
    # Extract SRT packets from .csv tshark dumps
    clr_srt_packets = extract_srt_packets(clr_tshark_csv)
    list_srt_packets = extract_srt_packets(list_tshark_csv)

    # Extract UMSG_HANDSHAKE packets from SRT packets
    clr_umsg_handshake = extract_umsg_handshake_packets(clr_srt_packets)
    list_umsg_handshake = extract_umsg_handshake_packets(list_srt_packets)

    print('\nUMSG_HANDSHAKE packets extracted from caller dump')
    print(clr_umsg_handshake)

    print('\nUMSG_HANDSHAKE packets extracted from listener dump')
    print(list_umsg_handshake)

    # Check whether there are 4 handshakes in tshark dumps
    if len(clr_umsg_handshake) != 4:
        raise Exception(
            'There are less than 4 UMSG_HANDSHAKE packets in tshark dump '
            'collected at the caller side'
        )

    if len(list_umsg_handshake) != 4:
        raise Exception(
            'There are less than 4 UMSG_HANDSHAKE packets in tshark dump '
            'collected at the listener side'
        )

    # Calculate initial RTT
    # Calculate initial RTT using caller data
    clr_rtt_1 = clr_umsg_handshake.loc[1, 'frame.time'] - clr_umsg_handshake.loc[0, 'frame.time']
    clr_rtt_2 = clr_umsg_handshake.loc[3, 'frame.time'] - clr_umsg_handshake.loc[2, 'frame.time']
    clr_rtt = (clr_rtt_1 + clr_rtt_2) / 2
    # Calculate initial RTT using listener data
    list_rtt = list_umsg_handshake.loc[2, 'frame.time'] - list_umsg_handshake.loc[1, 'frame.time']
    # Calculate result initial RTT
    rtt = (clr_rtt + list_rtt) / 2

    print(f'\nInitial RTT: {rtt}')

    # Calculate potential difference in time between caller and listener clocks
    # delta = clr_umsg_handshake.loc[0, 'frame.time'] - list_umsg_handshake.loc[0, 'frame.time'] + rtt / 2
    delta = list_umsg_handshake.loc[0, 'frame.time'] - clr_umsg_handshake.loc[0, 'frame.time']

    print(clr_umsg_handshake.loc[0, 'frame.time'])
    print(list_umsg_handshake.loc[0, 'frame.time'])
    print(type(list_umsg_handshake.loc[0, 'frame.time']))

    print(f'Time difference: {delta}')

    print(clr_umsg_handshake.info())



def main():
    # Set filepaths to the source files: sender and receiver SRT core
    # .csv statistics, tshark .pcapng dumps collected on both sides
    SND_STATS_CSV = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@23.96.93.54/4-srt-xtransmit-stats-snd.csv'
    RCV_STATS_CSV = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/3-srt-xtransmit-stats-rcv.csv'
    SND_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@23.96.93.54/1-tshark-tracefile-snd.pcapng'
    RCV_TSHARK_PCAPNG = '_data/_useast_eunorth_10.02.20_100Mbps/msharabayko@40.69.89.21/2-tshark-tracefile-rcv.pcapng'

    # TODO: Make it properly
    # For the first time
    # RCV_TSHARK_CSV = convert.convert_to_csv(pathlib.Path(RCV_TSHARK_PCAPNG), True)
    # For the following time
    SND_TSHARK_CSV = convert_to_csv(pathlib.Path(SND_TSHARK_PCAPNG))
    RCV_TSHARK_CSV = convert_to_csv(pathlib.Path(RCV_TSHARK_PCAPNG))

    CLR_TSHARK_CSV = SND_TSHARK_CSV
    LIST_TSHARK_CSV = RCV_TSHARK_CSV

    # Check the difference in time
    check_time_difference(CLR_TSHARK_CSV, LIST_TSHARK_CSV)

    return

    # Align SRT statisitcs obtained from the SRT receiver and sender
    stats = align_srt_stats(SND_STATS_CSV, RCV_STATS_CSV)

    print('\nAligned SRT sender and receiver statistics')
    print(stats.head(10))
    print(stats.tail(10))

    # Align SRT stats and tshark data
    df = align_srt_tshark_stats(stats, RCV_TSHARK_CSV)

    print('\nAligned SRT statisitics and tshark data')
    print(df.head(10))
    print(df.tail(10))


if __name__ == '__main__':
    main()