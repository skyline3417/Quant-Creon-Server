# coding=utf-8
import os
import time
import enum

import win32com.client
from pywinauto import application

from stock_info_enum import MARKET_KIND, CONTROL_KIND, SUPERVISION_KIND, STOCK_STATUS_KIND, SECTION_KIND


class LIMIT_TYPE(enum.Enum):
    """
    요청 제한 타입
    """

    TRADE_REQUEST = 0  # 주문 / 계좌 관련 RQ 요청
    NONTRADE_REQUEST = 1  # 시세관련 RQ 요청
    SUBSCRIBE = 2  # 시세관련 SB (실시간 등록)


class CpCybos:
    """
    크레온 api 관련 클래스

    Attributes:
        obj_com (obj_win32com): win32com 오브젝트
    """

    obj_com = win32com.client.Dispatch("CpUtil.CpCybos")

    @classmethod
    def get_connect_status(cls):
        """
        크레온api 연결상태를 확인

        Returns:
            (bool): 연결상태 (True - 연결 정상, False - 연결 끊김)
        """
        if cls.obj_com.IsConnect == 0:
            return False
        return True

    @classmethod
    def disconnect(cls):
        """
        크레온api와 연결 해제
        """
        if cls.get_connect_status():
            cls.obj_com.PlusDisconnect()

    @classmethod
    def get_limit_request_remain_time(cls):
        """
        요청개수를 재계산하기까지 남은 시간을 반환 (반환된 시간이 지나기 전에 남은 요청 개수를 초과하면 제한됨)

        Returns:
            (int): 남은 시간 (단위: ms)
        """
        return cls.obj_com.LimitRequestRemainTime

    @classmethod
    def get_limit_remain_count(cls, e_limit_type):
        """
        요청타입을 받아 해당 요청타입에 대한 제한을 하기까지 남은 요청 횟수 반환

        Parameters:
            e_limit_type (LIMIT_TYPE): 요청 타입

        Returns:
            (int): 남은 요청 횟수
        """
        return cls.obj_com.GetLimitRemainCount(e_limit_type.value)

    @classmethod
    def wait_to_do_request(cls, e_limit_type):
        """
        요청타입을 받아 요청타입에 대한 제한 횟수가 초기화 될때까지 대기

        Parameters:
            e_limit_type (LIMIT_TYPE): 요청 타입
        """
        while cls.get_limit_remain_count(e_limit_type) <= 1:
            pass


# original_func 콜하기 전에 PLUS 연결 상태 체크하는 데코레이터


class CreonCpCodeMgr:
    """
    주식 종목 정보 관련 클래스

    Attributes:
        obj_com (COM_obj): win32com 오브젝트(creon api 종목 정보 관련)
    """

    obj_com = win32com.client.Dispatch("CpUtil.CpCodeMgr")

    @classmethod
    def get_stock_name(cls, stock_code):
        """
        종목 코드를 받아 종목명을 반환

        Parameters:
            stock_code (str): 종목 코드

        Returns:
            (str): 종목 이름
        """
        return cls.obj_com.CodeToName(stock_code)

    @classmethod
    def get_stock_industry_code(cls, stock_code):
        """
        종목 코드를 받아 증권 전산 업종 코드를 반환

        Parameters:
            stock_code (str): 종목 코드

        Returns:
            (str?): 증권 전산 업종 코드
        """
        return cls.obj_com.GetStockIndustryCode(stock_code)

    @classmethod
    def get_stock_market_kind(cls, stock_code):
        """
        종목 코드를 받아 코드에 해당하는 소속부를 반환

        Parameters:
            stock_code (str): 종목 코드

        Returns :
            (MARKET_KIND): 소속부
        """
        return MARKET_KIND(cls.obj_com.GetStockMarketKind(stock_code))

    @classmethod
    def get_stock_control_kind(cls, stock_code):
        """
        종목 코드를 받아 코드에 해당하는 감리구분 반환

        Parameters:
            stock_code (str): 종목코드

        Returns:
            (CONTROL_KIND): 감리구분
        """
        return CONTROL_KIND(cls.obj_com.GetStockControlKind(stock_code))

    @classmethod
    def get_stock_supervision_kind(cls, stock_code):
        """
        종목 코드를 받아 코드에 해당하는 관리구분 반환

        Parameters:
            stock_code (str): 종목코드

        Returns:
            (SUPERVISION_KIND): 관리구분
        """
        return SUPERVISION_KIND(cls.obj_com.GetStockSupervisionKind(stock_code))

    @classmethod
    def get_stock_status_kind(cls, stock_code):
        """
        종목 코드를 받아 코드에 해당하는 주식 상태 반환

        Parameters:
            stock_code (str): 종목코드

        Returns:
            (STOCK_STATUS_KIND): 주식 상태
        """
        return STOCK_STATUS_KIND(cls.obj_com.GetStockStatusKind(stock_code))

    @classmethod
    def get_stock_section_kind(cls, stock_code):
        """
        종목코드를 받아 부구분코드를 반환

        Parameters:
            stock_code (str): 종목코드

        Returns:
            (SECTION_KIND): 부구분코드
        """
        return SECTION_KIND(cls.obj_com.GetStockSectionKind(stock_code))

    @classmethod
    def get_stock_code_list(cls, e_market_kind):
        """
        마켓에 해당하는 종목코드 리스트 반환

        Parameters:
            e_market_kind (MARKET_KIND): 시장 구분

        Returns:
            (list): 시장에 해당하는 종목코드 전체
        """
        return cls.obj_com.GetStockListByMarket(e_market_kind.value)


class CreonDataComm:
    """
    크레온 api와의 기본 통신 구조 클래스

    Attributes:
        obj_com (COM_obj): win32com 오브젝트

        e_limit_type (LIMIT_TYPE): 요청 타입
    """

    def __init__(self, com_obj_name, e_limit_type):
        self.obj_com = win32com.client.Dispatch(com_obj_name)
        self.e_limit_type = e_limit_type

    def check_rq_status(self):
        """
        통신상태를 체크

        통신상태가 정상이 아닐시 오류코드와 오류메시지를 출력함

        Returns:
            (bool): 통신상태 (True - 정상, False - 오류)
        """
        rq_status = self.obj_com.GetDibStatus()
        rq_msg = self.obj_com.GetDibMsg1()

        if rq_status == 0:
            return True
            # print("통신상태 정상[{}]{}".format(rqStatus, rqRet), end=' ')

        print("RQ ERROR : {}\n\nMSG : {}".format(rq_status, rq_msg))
        return False

    def set_input_value(self, value_type, value):
        """
        요청할 데이터의 종류를 세팅

        Parameters:
            value_type (int): 값의 타입

            value (): 값
        """
        self.obj_com.SetInputValue(value_type, value)

    def get_header_value(self, value_type):
        """
        헤더 데이터를 반환

        Parameters:
            value_type (int): 받아올 값의 타입

        Returns:
            (): value_type에 대한 값
        """
        return self.obj_com.GetHeaderValue(value_type)

    def get_data_value(self, value_type, index):
        """
        데이터를 받아옴

        Parameters:
            value_type (int): 받아올 값의 타입

            index (int): 받아올 값의 인덱스(순번)

        Returns:
            (): index 순번의 value_type 타입 데이터
        """
        return self.obj_com.GetDataValue(value_type, index)

    def block_request(self):
        """
        데이터를 요청
        """
        CpCybos.wait_to_do_request(self.e_limit_type)
        self.obj_com.BlockRequest()

    def is_continue(self):
        """
        연속 데이터 유무를 확인

        Returns:
            (bool): 연속데이터 유무 (True - 있음, False - 없음)
        """
        if self.obj_com.Continue == 1:
            return True
        return False

    def get_data_list(self, creon_idx_list, rq_data_count, rq_count_code):
        """
        여러개의 데이터를 가져와 리스트로 반환

        Parameters:
            creon_idx_list (list): 가져올 데이터 컬럼 인덱스 리스트 (creon api 기준)

            rq_data_count (int): 요청한 데이터 갯수

            rq_count_code (int): 받아온 데이터 갯수를 반환하는 creon_api type코드 (get_header_value의 인자) 
        
        Returns:
            (list): 리스트 데이터
        """
        all_rcv_data = []
        while True:
            self.block_request()  # 데이터 요청

            # 요청 상태 체킹 (오류가있다면 메시지 출력후 종료함)
            if not self.check_rq_status():
                return None

            # 수신된 데이터 수 만큼 반복
            for row_idx in range(self.get_header_value(rq_count_code)):
                rcv_row_data = []  # 차트 한줄 데이터

                # 차트 한줄 데이터 받아오기
                for creon_idx in creon_idx_list:
                    rcv_row_data.append(self.get_data_value(creon_idx, row_idx))

                # 차트 한줄 데이터가 비었을 경우 리턴
                if not rcv_row_data:
                    return all_rcv_data

                all_rcv_data.append(rcv_row_data)  # 데이터 한줄 추가

                # 데이터를 요청한 갯수 만큼 받았을 경우 리턴
                if rq_data_count == len(all_rcv_data):
                    return all_rcv_data

            # 서버가 가진 모든 데이터를 다 받았을 경우 리턴
            if not self.is_continue():
                return all_rcv_data

    def subscribe(self):
        """
        실시간 데이터 받아오기 등록
        """
        if CpCybos.get_limit_remain_count(self.e_limit_type):
            self.obj_com.Subscribe()
        else:
            print("NO REMAIN SUBSCRIBE RQ COUNT")

    def unsubscribe(self):
        """
        실시간 데이터 받아오기 등록 해제
        """
        self.obj_com.Unsubscribe()

    def get_handler(self, event_class):
        """
        실시간 데이터 받아오기의 이벤트 핸들러 세팅

        Parameters:
            event_class (class): 이벤트 클래스

        Returns:
            (event_class): event_class의 인스턴스
        """
        return win32com.client.WithEvents(self.obj_com, event_class)


class CreonStockChart(CreonDataComm):
    """
    주식 차트 데이터 관련 클래스
    """

    def __init__(self):
        CreonDataComm.__init__(self, "CpSysDib.StockChart", LIMIT_TYPE.NONTRADE_REQUEST)


class CreonStockCur(CreonDataComm):
    """
    실시간 주가 데이터 관련 클래스
    """

    def __init__(self):
        CreonDataComm.__init__(self, "Dscbo1.StockCur", LIMIT_TYPE.NONTRADE_REQUEST)


class CreonStockOrder:
    """
    주식 주문 관련 클래스
    """

    def __init__(self):
        self.buy_sell = CreonDataComm("CpTrade.CpTd0311", LIMIT_TYPE.TRADE_REQUEST)  # 매수 매도 주문
        self.modify_price = CreonDataComm("CpTrade.CpTd0313", LIMIT_TYPE.TRADE_REQUEST)  # 가격 정정 주문
        self.modify_type = CreonDataComm("CpTrade.CpTd0303", LIMIT_TYPE.TRADE_REQUEST)  # 유형 정정 주문
        self.cancel = CreonDataComm("CpTrade.CpTd0314", LIMIT_TYPE.TRADE_REQUEST)  # 취소 주문


class CreonStockAble:
    """
    주식 거래 가능 수량 관련 클래스
    """

    def __init__(self):
        self.buy = CreonDataComm("CpTrade.CpTdNew5331A", LIMIT_TYPE.TRADE_REQUEST)  # 매수
        self.sell = CreonDataComm("CpTrade.CpTdNew5331B", LIMIT_TYPE.TRADE_REQUEST)  # 매도


class CreonUnconcluded(CreonDataComm):
    """
    미체결 잔량 데이터 관련 클래스
    """

    def __init__(self):
        CreonDataComm.__init__(self, "CpTrade.CpTd5339", LIMIT_TYPE.TRADE_REQUEST)


class CreonCpConclusion(CreonDataComm):
    """
    실시간 체결 데이터 관련 클래스
    """

    def __init__(self):
        CreonDataComm.__init__(self, "Dscbo1.CpConclusion", LIMIT_TYPE.SUBSCRIBE)


class CreonBalance(CreonDataComm):
    """
    잔고 평가 관련 클래스
    """

    def __init__(self):
        CreonDataComm.__init__(self, "CpTrade.CpTd6033", LIMIT_TYPE.TRADE_REQUEST)


class CreonCpTdUtil:
    """
    주문 관련 오브젝트 초기화 클래스

    Attributes:
        obj_com (win32com): win32com 오브젝트

        account (str): 계좌

        product_code (str): 계좌 상품 코드
    """

    def __init__(self):
        self.obj_com = win32com.client.Dispatch("CpTrade.CpTdUtil")
        self.account = None  # 계좌
        self.product_code = None  # 계좌 상품 코드

    def trade_init(self):
        """
        주문 오브젝트 초기화

        Returns:
            (bool): 주문 오브젝트 초기화 성공 여부 (True - 성공, False - 실패)
        """
        # 주문 초기화 오류시 오류 메시지 출력후 False 리턴
        if self.obj_com.TradeInit(0) != 0:
            print("주문 오브젝트 초기화 오류")
            return False

        # 계좌와 계좌 상품코드 받아오기
        self.account = self.obj_com.AccountNumber[0]
        self.product_code = self.obj_com.GoodsList(self.account, 1)[0]

        return True


CREON_ID = "a"
CREON_PWD = "a"
CERT_PWD = "a"


class CreonLogin:
    """
    creon 자동 실행, 로그인 관련 클래스
    """

    @classmethod
    def kill_client(cls):
        """
        windows 상에서 실행중인 creon 관련 task를 모두 중지
        """
        os.system("taskkill /IM coStarter* /F /T")
        os.system("taskkill /IM CpStart* /F /T")
        os.system("taskkill /IM DibServer* /F /T")

        os.system("wmic process where \"name like '%coStarter%'\" call terminate")
        os.system("wmic process where \"name like '%CpStart%'\" call terminate")
        os.system("wmic process where \"name like '%DibServer%'\" call terminate")

    @classmethod
    def connect(cls, id_=CREON_ID, pwd=CREON_PWD, pwdcert=CERT_PWD):
        """
        creon PLUS 실행 후 자동로그인하는 메서드

        Parameters:
            id_ (str): creon

            pwd (str): creon 암호

            pwdcert (str): creon 공인인증서 암호
        """
        if not CpCybos.get_connect_status():
            CpCybos.disconnect()
            cls.kill_client()

            app = application.Application()
            app.start(
                "C:\\CREON\\STARTER\\coStarter.exe /prj:cp /id:{id} /pwd:{pwd} /pwdcert:{pwdcert} /autostart".format(id=id_, pwd=pwd, pwdcert=pwdcert)
            )

        while not CpCybos.get_connect_status():
            time.sleep(1)


def main():
    """
    크레온 자동 실행, 로그인
    """
    CreonLogin.connect()


if __name__ == "__main__":
    main()
