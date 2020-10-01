# coding=utf-8
import utils
from creon_api import CreonCpConclusion
from trade import TradeInfo, TradeData, BalanceData
from database import MariaDB

from trade_info_enum import ORDER_TYPE, CONCLUSION_TYPE, MODIFY_CANCEL_TYPE, PRICE_TYPE, ORDER_CONDITION


class TradeStatusRt:
    """
    실시간 주문 및 체결 상태 관련 클래스

    Attributes:
        method (instance): 실행시킬 호출한 인스턴스의 메소드

        creon_conclusion (creon_api.CreonCpConclusion): 실행시킬 메소드가 있는 클래스(creon 실시간 주문 체결 데이터 관련)의 인스턴스
    """

    def __init__(self, method=None):
        self.creon_conclusion = CreonCpConclusion()

        # 이벤트 핸들러 세팅
        handler = self.creon_conclusion.get_handler(TradeStatusRtEvent)
        handler.set_params(self.creon_conclusion, method)

    def subscribe(self):
        self.creon_conclusion.subscribe()  # 실시간 등록

    def unsubscribe(self):
        self.creon_conclusion.unsubscribe()  # 실시간 등록 해지


class TradeStatusRtEvent:
    """
    실시간 주문 접수 및 체결 이벤트 관련 클래스

    Attributes:
        client (creon_api.CreonCpConclusion): 실행시킬 메소드가 있는 클래스(creon 실시간 주문 체결 데이터 관련)의 인스턴스

        method (instance): 실행시킬 호출한 인스턴스의 메소드  
    """

    def __init__(self):
        self.client = None
        self.method = None

    def set_params(self, client, method=None):
        """
        파라미터 세팅

        Parameters:
            client (creon_api.CreonCpConclusion): 실행시킬 메소드가 있는 클래스(creon 실시간 주문 체결 데이터 관련)의 인스턴스

            method (instance): 실행시킬 호출한 인스턴스의 메소드
        """
        self.client = client
        self.method = method

    def OnReceived(self):
        """
        이벤트 발생시 실행
        """
        # 이벤트 발생된 주문 정보
        trade_info = {
            "date_time": utils.get_current_datetime("%Y%m%d%H%M%S"),  # 날짜 시간 YYYYMMDDHHmmSS
            "stock_name": self.client.get_header_value(2),  # 주식이름
            "qty": self.client.get_header_value(3),  # 수량
            "price": self.client.get_header_value(4),  # (접수된 or 체결된) 가격
            "order_num": self.client.get_header_value(5),  # 주문 번호
            "origin_order_num": self.client.get_header_value(6),  # 원 주문번호
            "stock_code": self.client.get_header_value(9),  # 종목 코드
            "e_order_type": ORDER_TYPE(self.client.get_header_value(12)),  # 주문 타입 (매수, 매도)
            "e_conclusion_type": CONCLUSION_TYPE(self.client.get_header_value(14)),  # 체결 타입 (체결, 확인, 거부, 접수)
            "e_modify_cancel_type": MODIFY_CANCEL_TYPE(self.client.get_header_value(16)),  # 정정 취소 타입
            "e_price_type": PRICE_TYPE(self.client.get_header_value(18)),  # 호가 타입 (일반, 시장가)
            "e_order_condition": ORDER_CONDITION(self.client.get_header_value(19)),  # 주문 조건 (IOC, FOK)
            "avg_price": self.client.get_header_value(21),  # 평균단가
            "able_sell_qty": self.client.get_header_value(22),  # 매도 가능 수량
            "balance_qty": self.client.get_header_value(23),  # 체결 기준 수량
            "total_price": None,  # 거래 금액 # TODO : 거래 수수료 및 세금 계산 필요
        }

        # 정정 주문의 경우 수량이 0이면 원주문의 전체 수량을 선택한다는것임 따라서 원 주문의 주문 수량을 가져옴 (그외 주문은 이상이 없음)
        if trade_info["e_modify_cancel_type"] == MODIFY_CANCEL_TYPE.MODIFY and trade_info["qty"] == 0:
            db_kr_operation_data = MariaDB("KR_OPERATION_DATA")
            trade_info["qty"] = db_kr_operation_data.select(
                "KR_Unconcluded_Order", "quantity", "order_number = " + str(trade_info["origin_order_num"])
            )

        trade_info.total_price = trade_info["price"] * trade_info["qty"]  # 거래 금액 # TODO : 거래 수수료 및 세금 계산 필요

        TradeData.add_trade_history(trade_info)  # 주문 / 체결 정보 업데이트

        # db 매도 가능 수량 변경 (정정/취소 주문이 접수된 경우는 creon api 에서 값이 0으로 오기 때문에 제외)
        if not (trade_info["e_conclusion_type"] == CONCLUSION_TYPE.RECEIVED and not trade_info["e_modify_cancel_type"] == MODIFY_CANCEL_TYPE.NONE):
            BalanceData.change_able_sell_quantity(trade_info["stock_code"], trade_info["able_sell_qty"])

        # 접수된 주문이 아닌 경우 db 미체결 잔량 차감
        if not trade_info["e_conclusion_type"] == CONCLUSION_TYPE.RECEIVED:

            # 정정/취소 주문인경우 원미체결 주문 잔량 차감
            if trade_info["e_conclusion_type"] == CONCLUSION_TYPE.CONFIRMED:
                TradeData.remove_unconcluded_order(trade_info["origin_order_num"], trade_info["qty"])
            else:
                TradeData.remove_unconcluded_order(trade_info["order_num"], trade_info["qty"])

        # (접수된 매수/매도 주문) or (확인된 정정 주문) 인 경우 미체결 주문 db에 추가
        if (trade_info["e_conclusion_type"] == CONCLUSION_TYPE.RECEIVED and trade_info["e_modify_cancel_type"] == MODIFY_CANCEL_TYPE.NONE) or (
            trade_info["e_conclusion_type"] == CONCLUSION_TYPE.CONFIRMED and trade_info["e_modify_cancel_type"] == MODIFY_CANCEL_TYPE.MODIFY
        ):
            TradeData.add_unconcluded_order(trade_info)

        # 체결된 주문인 경우 잔고 데이터 업데이트
        if trade_info["e_conclusion_type"] == CONCLUSION_TYPE.CONCLUDED:
            BalanceData.change_stock_balance(
                trade_info["stock_code"], trade_info["balance_qty"], trade_info["able_sell_qty"], trade_info["avg_price"]
            )

        if self.method:
            self.method(trade_info)


def main():
    pass


if __name__ == "__main__":
    main()
