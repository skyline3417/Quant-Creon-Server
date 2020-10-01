# coding=utf-8
import enum
from dataclasses import dataclass

from creon_api import CreonStockCur, CreonStockJpBid
from database import MariaDB
from trade import BalanceData

# 주식 실시간 데이터 db 컬럼
_STOCK_RT_DATA_COLUMNS = (
    "id",
    "time",
    "single_price_flag",
    "price",
    "dchange",
    "quantity",
    "volume",
)
# 주식 실시간 데이터 db 컬럼의 데이터 타입
_STOCK_RT_DATA_TYPES = (
    "BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY",
    "INT",
    "ENUM('SINGLE_PRICE','NORMAL')",
    "INT",
    "INT",
    "INT",
    "INT",
)


class SINGLE_PRICE_FLAG(enum.Enum):
    """
    예상 체결가 구분 플래그
    """

    SINGLE_PRICE = ord("1")  # 동시호가
    NORMAL = ord("2")  # 장중


class StockTickRt:
    """
    실시간 주식 틱데이터 관련 클래스

    Attributes:
        stock_code (str): 종목 코드

        method (method): 실행할 호출한 인스턴스의 메소드

        db_kr_stock_data_realtime (database.MariaDB): db통신 관련 클래스 인스턴스

        creon_stock_cur (creon_api.CreonStockCur): 실행시킬 메소드가 있는 클래스(creon 실시간 주식 데이터 관련)의 인스턴스
    """

    def __init__(self, stock_code, method=None):
        """
        Parameters:
            stock_code (str): 종목 코드
            
            method (method): 실행할 호출한 인스턴스의 메소드
        """
        self.stock_code = stock_code
        self.method = method

        # 실시간 종목 데이터 테이블 생성
        self.db_kr_stock_data_realtime = MariaDB("KR_STOCK_DATA_REALTIME")
        self.db_kr_stock_data_realtime.create(self.stock_code, _STOCK_RT_DATA_COLUMNS, _STOCK_RT_DATA_TYPES)

        self.creon_stock_cur = CreonStockCur()

        # 이벤트 핸들러 세팅
        handler = self.creon_stock_cur.get_handler(StockRtEvent)
        handler.set_params("tick", self.creon_stock_cur, method)

        # stock_code에 대한 실시간 등록
        self.creon_stock_cur.set_input_value(0, self.stock_code)
        self.creon_stock_cur.subscribe()
        self.creon_stock_cur.check_rq_status()

    def unsubscribe(self):
        self.creon_stock_cur.unsubscribe()  # 실시간 등록 해지
        self.db_kr_stock_data_realtime.drop(self.stock_code)  # 실시간 종목 데이터 테이블 삭제


class StockAskBidRt:
    """
    실시간 주식 10차 호가 정보 관련 클래스

    Attributes:
        stock_code (str): 종목 코드

        method (method): 실행할 호출한 인스턴스의 메소드


        creon_stock_cur (creon_api.CreonStockCur): 실행시킬 메소드가 있는 클래스(creon 실시간 주식 데이터 관련)의 인스턴스
    """

    def __init__(self, stock_code, method=None):
        """
        Parameters:
            stock_code (str): 종목 코드
            
            method (method): 실행할 호출한 인스턴스의 메소드
        """
        self.stock_code = stock_code
        self.method = method

        self.creon_stock_jp_bid = CreonStockJpBid()

        # 이벤트 핸들러 세팅
        handler = self.creon_stock_jp_bid.get_handler(StockRtEvent)
        handler.set_params("ask_bid", self.creon_stock_jp_bid, method)

        # stock_code에 대한 실시간 등록
        self.creon_stock_jp_bid.set_input_value(0, self.stock_code)
        self.creon_stock_jp_bid.subscribe()
        self.creon_stock_jp_bid.check_rq_status()

    def unsubscribe(self):
        self.creon_stock_jp_bid.unsubscribe()  # 실시간 등록 해지


class StockRtEvent:
    """
    실시간 주식 데이터 들어올시 발생되는 이벤트 클래스

    Attributes:
        client (CreonStockCur): 실행시킬 메소드가 있는 클래스(creon 실시간 주식 데이터 관련)의 인스턴스

        method (method): 실행할 호출한 인스턴스의 메소드
    """

    def set_params(self, evt_type, client, method=None):
        """
        파라메터 설정

        Parameters:
            evt_type (str): 이벤트의 종류 (틱 or 호가)

            client (CreonStockCur): 실행시킬 메소드가 있는 클래스(creon 실시간 주식 데이터 관련)의 인스턴스

            method (method): 실행할 호출한 인스턴스의 메소드
        """
        self.evt_type = evt_type
        self.client = client
        self.method = method

    def OnReceived(self):
        """
        이벤트 발생시 실행됨
        """

        if self.evt_type == "tick":
            # 실시간 데이터 가져온후 리스트화
            rt_data = {
                "stock_code": self.client.get_header_value(0),  # 종목 코드
                "date_time": self.client.get_header_value(18),  # 시분초
                "e_single_price_flag": SINGLE_PRICE_FLAG(self.client.get_header_value(20)),  # 예상 체결가 구분 플래그 (동시호가 / 장중)
                "price": self.client.get_header_value(13),  # 현재가
                "day_changed": self.client.get_header_value(2),  # 대비
                "qty": self.client.get_header_value(17),  # 순간체결수량
                "vol": self.client.get_header_value(9),  # 거래량
            }

            data_db = [
                rt_data["date_time"],
                rt_data["single_price_flag"],
                rt_data["price"],
                rt_data["day_changed"],
                rt_data["qty"],
                rt_data["vol"],
            ]

            # db에 데이터 insert
            db_kr_stock_data_realtime = MariaDB("KR_STOCK_DATA_REALTIME")
            db_kr_stock_data_realtime.insert(rt_data["stock_code"], _STOCK_RT_DATA_COLUMNS[1:], data_db)

            BalanceData.update_current_price(rt_data["stock_code"], self.client.get_header_value(13))

        elif self.evt_type == "ask_bid":
            rt_data = {
                "stock_code": self.client.get_header_value(0),
                "ask": [0 for _ in range(10)],
                "bid": [0 for _ in range(10)],
                "ask_vol": [0 for _ in range(10)],
                "bid_vol": [0 for _ in range(10)],
            }
            data_index = [3, 7, 11, 15, 19, 27, 31, 35, 39, 43]
            for idx in range(10):
                rt_data["ask"][idx] = self.client.GetHeaderValue(data_index[idx])
                rt_data["bid"][idx] = self.client.GetHeaderValue(data_index[idx] + 1)
                rt_data["ask_vol"][idx] = self.client.GetHeaderValue(data_index[idx] + 2)
                rt_data["bid_vol"][idx] = self.client.GetHeaderValue(data_index[idx] + 3)

            rt_data["tot_ask"] = self.client.GetHeaderValue(23)
            rt_data["tot_bid"] = self.client.GetHeaderValue(24)

        if self.method:
            self.method(rt_data)


def main():
    pass


if __name__ == "__main__":
    main()
