# coding=utf-8
from datetime import datetime


def get_current_datetime(time_format):
    """
    time_format 형식의 현재 날짜 시간 데이터를 (int) 형으로 반환

    Parameters:
        time_format (str): 받아올 날짜 시간 데이터의 포맷 
            ("%Y"-년, "%m"-월, "%d"-일, "%H"-시, "%M"-분, "%S"-초) ex)"%Y%m%d%H%M%S" -YYYYmmDDHHMMSS

    Returns:
        (int): 현재 날짜 시간 데이터
    """
    now = datetime.now()
    return int(now.strftime(time_format))
