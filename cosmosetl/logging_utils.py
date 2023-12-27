import logging
import os
import time
import pathlib
from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
from config import MonitoringConfig


def logging_basic_config(filename=None):
    format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
    if filename is not None:
        logging.basicConfig(level=logging.INFO, format=format, filename=filename)
    else:
        logging.basicConfig(level=logging.INFO, format=format)


def logging_debug_config(filename=None):
    format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
    if filename is not None:
        logging.basicConfig(level=logging.DEBUG, format=format, filename=filename)
    else:
        logging.basicConfig(level=logging.DEBUG, format=format)


def config_log(level=logging.INFO):
    format = '%(asctime)s - %(name)s [%(levelname)s] - %(message)s'
    logging.basicConfig(
        format=format,
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S')


def write_first_row(log_file):
    if not os.path.exists(log_file):
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, 'w+') as f:
            f.write("start_block,end_block,start,end,tpb\n")


def write_log_performance(start_block, end_block, start, end, collector_id):
    root_path = MonitoringConfig.MONITOR_ROOT_PATH
    log_file = f"{root_path}/{collector_id}_monitor_{str(time.strftime('%Y-%m'))}.csv"
    if not os.path.exists(root_path):
        os.makedirs(os.path.dirname(root_path), exist_ok=True)
    write_first_row(log_file)
    if end_block > start_block:
        tpb = (end - start) / (end_block - start_block)
        tpb = round(tpb, 6)
        data_log = f"{start_block},{end_block},{int(start)},{int(end)}, {tpb}\n"
        with open(log_file, "a") as f:
            f.write(data_log)
        latest_log_file = f'{root_path}/{collector_id}_monitor_latest.csv'
        with open(latest_log_file, "w") as f:
            latest_data = f"start_block,end_block,start,end,tpb\n{data_log}"
            f.write(latest_data)


def write_monitor_logs(stream_name, synced_block, chain_id):
    monitor_path = MonitoringConfig.MONITOR_ROOT_PATH
    registry = CollectorRegistry()
    g = Gauge('last_block_synced', 'Block synced', ['process', 'chain_id'], registry=registry)
    g.labels(stream_name, chain_id).inc(synced_block)
    _file = monitor_path + stream_name + '.prom'
    write_to_textfile(_file, registry)
