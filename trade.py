# coding=utf-8
from dataclasses import dataclass

from creon_api import CreonStockOrder, CreonCpTdUtil, CreonBalance, CreonUnconcluded
from database import MariaDB
from trade_info_enum import ORDER_TYPE, CONCLUSION_TYPE, MODIFY_CANCEL_TYPE, PRICE_TYPE, ORDER_CONDITION

# 주문 오브젝트 초기화
_g_creon_td_util = CreonCpTdUtil()
_g_creon_td_util.trade_init()

TRADE_FEE_PERCENT = 0.011360
SELL_TAX_PERCENT = 0.25


@dataclass
class TradeInfo:
    """
    거래 정보 저장용 데이터 클래스
    """

    date_time: int  # 날짜 시간 YYYYMMDDHHmmSS
    stock_name: str  # 주식이름
    qty: int  # 수량
    price: int  # (접수된 or 체결된) 가격
    order_num: int  # 주문 번호
    origin_order_num: int  # 원 주문번호
    stock_code: str  # 종목 코드
    e_order_type: ORDER_TYPE  # 주문 타입 (매수, 매도)
    e_conclusion_type: CONCLUSION_TYPE  # 체결 타입 (체결, 확인, 거부, 접수)
    e_modify_cancel_type: MODIFY_CANCEL_TYPE  # 정정 취소 타입
    e_price_type: PRICE_TYPE  # 호가 타입 (일반, 시장가)
    e_order_condition: ORDER_CONDITION  # 주문 조건 (IOC, FOK)
    avg_price: int  # 평균단가
    able_sell_qty: int  # 매도 가능 수량
    balance_qty: int  # 체결 기준 수량
    total_price: int  # 총 거래 금액


class Order:
    """
    주식 주문 관련 클래스

    Attributes:
        creon_stock_order (creon_api.CreonStockOrder): creon 주문 관련 클래스 인스턴스
    """

    creon_stock_order = CreonStockOrder()

    @classmethod
    def buy(cls, stock_code, qty, e_order_condition, e_price_type, price):
        """
        매수 주문

        Parameters:
            stock_code (str): 종목 코드

            qty (int): 주문 수량

            e_order_condition (order_info_enum.ORDER_CONDITION): 주문 조건

            e_price_type (order_info_enum.PRICE_TYPE): 호가 종류

            price (int): 주문가

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        return cls.buy_sell(ORDER_TYPE.BUY, stock_code, qty, e_order_condition, e_price_type, price)

    @classmethod
    def sell(cls, stock_code, qty, e_order_condition, e_price_type, price):
        """
        매도 주문

        Parameters:
            stock_code (str): 종목 코드

            qty (int): 주문 수량

            e_order_condition (order_info_enum.ORDER_CONDITION): 주문 조건

            e_price_type (order_info_enum.PRICE_TYPE): 호가 종류

            price (int): 주문가

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        return cls.buy_sell(ORDER_TYPE.SELL, stock_code, qty, e_order_condition, e_price_type, price)

    @classmethod
    def buy_sell(cls, e_order_type, stock_code, qty, e_order_condition, e_price_type, price):
        """
        매수/매도 주문

        Parameters:
            e_order_type (order_info_enum.ORDER_TYPE): 주문 종류 (매수/매도)

            stock_code (str): 종목 코드

            qty (int): 주문 수량

            e_order_condition (order_info_enum.ORDER_CONDITION): 주문 조건

            e_price_type (order_info_enum.PRICE_TYPE): 호가 종류

            price (int): 주문가

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        cls.creon_stock_order.buy_sell.set_input_value(0, e_order_type.value)
        cls.creon_stock_order.buy_sell.set_input_value(1, _g_creon_td_util.account)
        cls.creon_stock_order.buy_sell.set_input_value(2, _g_creon_td_util.product_code)
        cls.creon_stock_order.buy_sell.set_input_value(3, stock_code)
        cls.creon_stock_order.buy_sell.set_input_value(4, qty)
        cls.creon_stock_order.buy_sell.set_input_value(5, price)
        cls.creon_stock_order.buy_sell.set_input_value(7, e_order_condition.value)
        cls.creon_stock_order.buy_sell.set_input_value(8, e_price_type.value)

        cls.creon_stock_order.buy_sell.block_request()
        if not cls.creon_stock_order.buy_sell.check_rq_status():
            return False

        order_number = cls.creon_stock_order.buy_sell.get_header_value(8)
        return order_number

    @classmethod
    def modify_price(cls, origin_order_num, stock_code, qty, price):
        """
        가격 정정 주문

        Parameters:
            origin_order_num (int): 정정할 원 주문번호

            stock_code (str): 종목 코드

            qty (int): 정정 수량

            price (int): 정정 주문가

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        cls.creon_stock_order.modify_price.set_input_value(1, origin_order_num)
        cls.creon_stock_order.modify_price.set_input_value(2, _g_creon_td_util.account)
        cls.creon_stock_order.modify_price.set_input_value(3, _g_creon_td_util.product_code)
        cls.creon_stock_order.modify_price.set_input_value(4, stock_code)
        cls.creon_stock_order.modify_price.set_input_value(5, qty)
        cls.creon_stock_order.modify_price.set_input_value(6, price)

        cls.creon_stock_order.modify_price.block_request()

        if not cls.creon_stock_order.modify_price.check_rq_status():
            return False

        order_number = cls.creon_stock_order.modify_price.get_header_value(7)

        return order_number

    @classmethod
    def modify_type(cls, origin_order_num, stock_code, qty, e_order_condition, e_price_type, price):
        """
        유형 정정 주문

        Parameters:
            origin_order_num (int): 정정할 원 주문번호

            stock_code (str): 종목 코드

            qty (int): 정정 수량

            e_order_condition (order_info_enum.ORDER_CONDITION): 정정할 주문 조건

            e_price_type (order_info_enum.PRICE_TYPE): 정정할 호가 종류

            price (int): 정정 주문가

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        cls.creon_stock_order.modify_type.set_input_value(1, origin_order_num)
        cls.creon_stock_order.modify_type.set_input_value(2, _g_creon_td_util.account)
        cls.creon_stock_order.modify_type.set_input_value(3, _g_creon_td_util.product_code)
        cls.creon_stock_order.modify_type.set_input_value(4, stock_code)
        cls.creon_stock_order.modify_type.set_input_value(5, qty)
        cls.creon_stock_order.modify_type.set_input_value(6, price)
        cls.creon_stock_order.modify_type.set_input_value(8, e_order_condition.value)
        cls.creon_stock_order.modify_type.set_input_value(9, e_price_type.value)

        cls.creon_stock_order.modify_type.block_request()

        if not cls.creon_stock_order.modify_type.check_rq_status():
            return False

        order_number = cls.creon_stock_order.modify_type.get_header_value(8)
        return order_number

    @classmethod
    def cancel(cls, origin_order_num, stock_code, qty=0):
        """
        취소주문

        Parameters:
            origin_order_num (int): 취소할 원 주문번호

            stock_code (str): 종목 코드

            qty (int): 취소 수량

        Returns:
            (int): 주문번호

            (bool): False - 주문 실패한 경우 (오류)
        """
        cls.creon_stock_order.cancel.set_input_value(1, origin_order_num)
        cls.creon_stock_order.cancel.set_input_value(2, _g_creon_td_util.account)
        cls.creon_stock_order.cancel.set_input_value(3, _g_creon_td_util.product_code)
        cls.creon_stock_order.cancel.set_input_value(4, stock_code)
        cls.creon_stock_order.cancel.set_input_value(5, qty)

        cls.creon_stock_order.cancel.block_request()

        if not cls.creon_stock_order.cancel.check_rq_status():
            return False

        order_number = cls.creon_stock_order.cancel.get_header_value(6)
        return order_number


class TradeData:
    """
    거래 데이터 관련 클래스

    Attributes:
        db_kr_operation_data (database.MariaDB): db 통신 관련 클래스 인스턴스
    """

    db_kr_operation_data = MariaDB("KR_OPERATION_DATA")

    @classmethod
    def add_trade_history(cls, trade_info):
        """
        거래 기록 (주문/체결) db에 저장

        Parameters:
            trade_info (TradeInfo): 주식 주문 체결 데이터 클래스 인스턴스 (구조체)
        """
        trade_info_db = {
            "date_time": trade_info.date_time,
            "order_number": trade_info.order_num,
            "conclusion_type": trade_info.e_conclusion_type.name,
            "order_type": trade_info.e_order_type.name,
            "modify_cancel_type": trade_info.e_modify_cancel_type.name,
            "origin_order_number": trade_info.origin_order_num,
            "stock_code": trade_info.stock_code,
            "stock_name": trade_info.stock_name,
            "quantity": trade_info.qty,
            "price_type": trade_info.e_price_type.name,
            "order_condition": trade_info.e_order_condition.name,
            "price": trade_info.price,
            "total_price": trade_info.total_price,
            "average_price": trade_info.avg_price,
            "able_sell_quantity": trade_info.able_sell_qty,
            "balance_quantity": trade_info.balance_qty,
        }

        columns_db = []
        data_db = []

        for key, value in trade_info_db.items():
            columns_db.append(key)
            data_db.append(value)

        # 거래 정보가 체결 데이터일 경우 체결 기록 db에 추가
        if trade_info.e_conclusion_type == CONCLUSION_TYPE.CONCLUDED:
            cls.db_kr_operation_data.insert("KR_Conclusion_History", columns_db, data_db)
        # 거래 정보가 주문 데이터일 경우 주문 기록 db에 추가
        else:
            cls.db_kr_operation_data.insert("KR_Order_History", columns_db, data_db)

    @classmethod
    def update_unconcluded_order(cls):
        """
        미체결 잔량 정보를 서버로부터 가져와 갱신
        """
        creon_unconcluded = CreonUnconcluded()

        creon_unconcluded.set_input_value(0, _g_creon_td_util.account)
        creon_unconcluded.set_input_value(1, _g_creon_td_util.product_code)
        creon_unconcluded.set_input_value(7, 500)

        all_rcv_data_db = creon_unconcluded.get_data_list([1, 13, 3, 4, 11, 21, 7], 500, 5)

        if not all_rcv_data_db:
            return

        # 가져온 데이터 수정 (주문 타입(매수/매도) 열거형 이름으로)
        for rcv_row in all_rcv_data_db:
            rcv_row[1] = ORDER_TYPE(rcv_row[1]).name

        columns_db = ["order_number", "order_type", "stock_code", "stock_name", "quantity", "price_type", "price"]

        cls.db_kr_operation_data.delete("KR_Unconcluded_Order")
        cls.db_kr_operation_data.insert("KR_Unconcluded_Order", columns_db, all_rcv_data_db)

    @classmethod
    def add_unconcluded_order(cls, trade_info):
        """
        미체결 주문 목록 db에 미체결 주문 추가

        Parameters:
            trade_info (TradeInfo): 주식 주문 체결 데이터 클래스 인스턴스 (구조체)
        """
        # 미체결 데이터 및 컬럼
        unconcluded_order_db = {
            "date_time": trade_info.date_time,
            "order_number": trade_info.order_num,
            "order_type": trade_info.e_order_type.name,
            "stock_code": trade_info.stock_code,
            "stock_name": trade_info.stock_name,
            "quantity": trade_info.qty,
            "price_type": trade_info.e_price_type.name,
            "order_condition": trade_info.e_order_condition.name,
            "price": trade_info.price,
        }
        columns_db = []
        data_db = []
        for key, value in unconcluded_order_db.items():
            columns_db.append(key)
            data_db.append(value)

        cls.db_kr_operation_data.insert("KR_Unconcluded_Order", columns_db, data_db)  # db에 데이터 추가

    @classmethod
    def remove_unconcluded_order(cls, order_num, qty):
        """
        미체결 주문 목록 db의 order_number에 해당하는 미체결 주문의 수량을
        qty만큼 차감

        Parameters:
            order_num (int): 미체결 주문 번호

            qty (int): 체결/수정/취소 된 수량 (빼줄 값)
        """

        # 업데이트할 미체결 주문량 계산 (갱신될_값 = 미체결_수량 - 체결된or갱신된_수량)
        unconcluded_qty = cls.db_kr_operation_data.select("KR_Unconcluded_Order", "quantity", "order_number = " + str(order_num))
        if not unconcluded_qty:
            return
        new_unconcluded_qty = unconcluded_qty - qty

        # 미체결 수량이 0으로 바뀌었을 경우 미체결 주문 데이터 삭제 후 리턴
        if new_unconcluded_qty == 0:
            cls.db_kr_operation_data.delete("KR_Unconcluded_Order", "order_number = " + str(order_num))
            return

        # 미체결 수량 업데이트
        cls.db_kr_operation_data.update(
            "KR_Unconcluded_Order", "quantity", new_unconcluded_qty, "order_number = " + str(order_num),
        )


class BalanceData:
    """
    잔고 데이터 클래스

    Attributes:
        db_kr_operation_data (database.MariaDB): db 통신 관련 클래스 인스턴스
    """

    db_kr_operation_data = MariaDB("KR_OPERATION_DATA")

    @classmethod
    def update_stock_balance(cls):
        """
        주식 잔고 데이터를 서버로부터 가져와 갱신
        """
        creon_balance = CreonBalance()

        # 받아올 값 세팅
        creon_balance.set_input_value(0, _g_creon_td_util.account)
        creon_balance.set_input_value(1, _g_creon_td_util.product_code)
        creon_balance.set_input_value(2, 500)
        creon_balance.set_input_value(3, "2")

        all_rcv_data_db = creon_balance.get_data_list([12, 0, 17, 18, 7, 15, 10, 11, 9], 500, 7)

        if not all_rcv_data_db:
            return

        for rcv_row in all_rcv_data_db:
            stock_code = rcv_row[0]
            stock_info = cls.db_kr_operation_data.select(
                "KR_Stock_List", ["market_kind", "section_kind", "wics_code"], "stock_code ='" + stock_code + "'"
            )
            rcv_row[2:1] = stock_info

            # 손익금, 평가금액 단위 (원)으로 맞춤
            rcv_row[9] /= 1000
            rcv_row[11] /= 1000

        columns_db = [
            "stock_code",
            "stock_name",
            "market_kind",
            "section_kind",
            "wics_code",
            "average_unit_price",
            "profit_unit_price",
            "quantity",
            "able_sell_quantity",
            "profit",
            "profit_ratio",
            "evaluation",
        ]
        cls.db_kr_operation_data.delete("KR_Stock_Balance")
        cls.db_kr_operation_data.insert("KR_Stock_Balance", columns_db, all_rcv_data_db)

    @classmethod
    def change_stock_balance(cls, stock_code, balance_qty, able_sell_qty, avg_price):
        """
        db에 주식 잔고 데이터를 변경

        Parameters:
            stock_code (str): 종목 코드

            balance_qty (int): 잔고 수량

            able_sell_qty (int): 매도 가능 수량

            avg_price (int): 평균 단가f
        """
        # 해당 종목의 잔고가 0일 경우 잔고 테이블에서 종목 삭제 후 리턴
        if balance_qty == 0:
            cls.db_kr_operation_data.delete("KR_Stock_Balance", "stock_code = '" + stock_code + "'")
            return

        # 잔고 테이블에 종목이 없을경우 종목 추가
        if not cls.db_kr_operation_data.is_exist("KR_Stock_Balance", "stock_code = '" + stock_code + "'"):
            db_data = [stock_code, cls.db_kr_operation_data.select("KR_Stock_List", "stock_name", "stock_code = '" + stock_code + "'")]
            db_data += cls.db_kr_operation_data.select(
                "KR_Stock_List", ["market_kind", "section_kind", "wics_code"], "stock_code = '" + stock_code + "'",
            )
            cls.db_kr_operation_data.insert(
                "KR_Stock_Balance", ["stock_code", "stock_name", "market_kind", "section_kind", "wics_code"], db_data,
            )

        # 종목 잔고 데이터 갱신
        profit_unit_price = avg_price / (1 - (TRADE_FEE_PERCENT / 100 + SELL_TAX_PERCENT / 100))  # 손익단가 계산
        data_db = [avg_price, profit_unit_price, balance_qty, able_sell_qty]
        columns_db = ["average_unit_price", "profit_unit_price", "quantity", "able_sell_quantity"]
        cls.db_kr_operation_data.update(
            "KR_Stock_Balance", columns_db, data_db, "stock_code = '" + stock_code + "'",
        )

    @classmethod
    def change_able_sell_quantity(cls, stock_code, able_sell_qty):
        """
        주식 잔고 db에 stock_code에 해당하는 종목의 매도 가능 수량을 변경

        Parameters:
            stock_code (str): 종목 코드

            able_sell_qty (int): 업데이트 할 매도 가능 수량
        """
        cls.db_kr_operation_data.update(
            "KR_Stock_Balance", "able_sell_quantity", able_sell_qty, "stock_code = '" + stock_code + "'",
        )

    @classmethod
    def update_current_price(cls, stock_code, cur_price):
        """
        """
        data_db = cls.db_kr_operation_data.select("KR_Stock_Balance", ["profit_unit_price", "quantity"], "stock_code = '" + stock_code + "'")
        if not data_db:
            return
        profit_unit_price = data_db[0]
        qty = data_db[1]

        profit = (cur_price - profit_unit_price) * qty
        profit_ratio = (cur_price / profit_unit_price) * 100 - 100
        evaluation = cur_price * qty * (1 - (TRADE_FEE_PERCENT / 100 + SELL_TAX_PERCENT / 100))

        cls.db_kr_operation_data.update(
            "KR_Stock_Balance",
            ["current_price", "profit", "profit_ratio", "evaluation"],
            [cur_price, int(profit), profit_ratio, int(evaluation)],
            "stock_code ='" + stock_code + "'",
        )


def main():
    order = Order.buy("A000660", 15, ORDER_CONDITION.NONE, PRICE_TYPE.NORMAL, 84100)
    input()
    Order.modify_price(order, "A000660", 0, 84900)


if __name__ == "__main__":
    main()
