# coding=utf-8
from database import MariaDB
from creon_api import CreonLogin, CreonCpCodeMgr, CreonStockChart
from stock_info_enum import MARKET_KIND


class StockData:
    """
    주식 데이터 관련 클래스

    Attributes:
        db_kr_operation_data (database.MariaDB): db 통신 관련 클래스 인스턴스
    """

    db_kr_operation_data = MariaDB("KR_OPERATION_DATA")

    @classmethod
    def update_stock_list(cls):
        """
        creon 서버에서 종목정보 가져와 db에  업데이트
        """

        # 종목 데이터 db 컬럼
        columns_db = [
            "stock_code",
            "stock_name",
            "market_kind",
            "section_kind",
            "supervision_kind",
            "control_kind",
            "stock_status_kind",
        ]

        # 서버에서 모든 종목코드 가져옴
        stock_code_list = CreonCpCodeMgr.get_stock_code_list(MARKET_KIND.KOSPI) + CreonCpCodeMgr.get_stock_code_list(MARKET_KIND.KOSDAQ)

        for stock_code in stock_code_list:
            # 종목 정보 가져와서 리스트화
            data_db = [
                stock_code,  # 종목 코드
                CreonCpCodeMgr.get_stock_name(stock_code),  # 종목 이름
                CreonCpCodeMgr.get_stock_market_kind(stock_code).name,  # 소속부 구분 (코스피 / 코스닥)
                CreonCpCodeMgr.get_stock_section_kind(stock_code).name,  # 구분 코드 (주권 / ETF / ...)
                CreonCpCodeMgr.get_stock_supervision_kind(stock_code).name,  # 관리 구분
                CreonCpCodeMgr.get_stock_control_kind(stock_code).name,  # 감리 구분
                CreonCpCodeMgr.get_stock_status_kind(stock_code).name,  # 주식 상태
            ]

            # 종목데이터가 db 테이블에 이미 있을경우 update 없을경우 insert
            if cls.db_kr_operation_data.is_exist("KR_Stock_List", "stock_code = '" + stock_code + "'"):
                cls.db_kr_operation_data.update("KR_Stock_List", columns_db[1:], data_db[1:], "stock_code = " + "'" + stock_code + "'")
            else:
                cls.db_kr_operation_data.insert("KR_Stock_List", columns_db, data_db)

    @classmethod
    def update_all_chart_data(cls, chart_type):
        """
        모든 종목의 chart_type에 해당하는 차트데이터를 업데이트함

        Parameters:
            chart_type (str): 업데이트할 차트데이터의 종류 ("D" - 1일봉 차트, "m" - 1분봉차트)
        """
        stock_code_list = cls.db_kr_operation_data.select("KR_Stock_List", "stock_code")  # db에 저장된 모든 종목의 코드와 이름 가져옴

        # stock_code_list에 있는 종목 모두 chart_type에 해당하는 차트데이터 업데이트
        for idx, stock_code in enumerate(stock_code_list):
            print("UPDATE ALL 1" + chart_type + " DATA [" + str(idx) + " / " + str(len(stock_code_list)) + "]\n\n")
            cls.update_chart_data(stock_code, chart_type)

    @classmethod
    def update_chart_data(cls, stock_code, chart_type):
        """
        stock_code 종목의 chart_type에 해당하는 차트 데이터를 업데이트

        Parameters:
            stock_code (str): 종목 코드

            chart_type (str): 업데이트할 차트데이터의 종류 ("D" - 1일봉 차트, "m" - 1분봉차트)
        """
        # 현재 업데이트 상태 출력
        stock_name = cls.db_kr_operation_data.select("KR_Stock_List", "stock_name", "stock_code = '" + stock_code + "'")
        print("UPDATE 1" + chart_type + " DATA " + stock_code + " " + stock_name + "\n\n")

        # 일봉 데이터일 경우의 데이터셋
        if chart_type == "D":
            # 일봉 차트 데이터 db 컬럼/타입
            columns_and_types = {
                "date": "INT PRIMARY KEY",
                "open": "INT",
                "high": "INT",
                "low": "INT",
                "close": "INT",
                "volume": "INT",
                "shares_listed": "BIGINT",
                "foreign_limit": "BIGINT",
                "foreign_hold": "BIGINT",
                "foreign_ratio": "DOUBLE",
                "agency_net_buy": "BIGINT",
                "agency_acc_net_buy": "BIGINT",
            }
            db_KR_STOCK_DATA = MariaDB("KR_STOCK_DATA_1DAY")  # 일봉 차트 데이터 db 컬럼
            recent_date_time_column = "recent_1day_data_date"  # 일봉 차트 데이터 db 컬럼
            creon_idxs = (0, 2, 3, 4, 5, 8, 12, 14, 16, 17, 20, 21)  # 일봉차트 데이터에 필요한 차트데이터 인덱스 (creon api 기준)
        # 분봉 데이터일 경우의 데이터셋
        elif chart_type == "m":
            # 분봉 차트 데이터 db 컬럼/타입
            columns_and_types = {
                "date_time": "BIGINT PRIMARY KEY",
                "open": "INT",
                "high": "INT",
                "low": "INT",
                "close": "INT",
                "volume": "INT",
            }
            db_KR_STOCK_DATA = MariaDB("KR_STOCK_DATA_1MIN")  # 일봉 차트 데이터 db 컬럼
            recent_date_time_column = "recent_1min_data_date_time"  # 분봉 차트 데이터 db 컬럼
            creon_idxs = (0, 1, 2, 3, 4, 5, 8)  # 분봉차트 데이터에 필요한 차트데이터 인덱스 (creon api 기준)

        columns_db = []  # db컬럼
        data_types_db = []  # db 컬럼의 데이터 타입

        # columns_and_types 딕셔너리를 두개의 리스트로 분리
        for key, value in columns_and_types.items():
            columns_db.append(key)
            data_types_db.append(value)

        # 최근 데이터의 날짜/시간을 가져옴
        recent_data_date_time = cls.db_kr_operation_data.select("KR_Stock_List", recent_date_time_column, "stock_code = '" + stock_code + "'")

        all_rcv_chart_data_db = cls.get_chart_data(stock_code, creon_idxs, chart_type, 1, recent_data_date_time)  # 서버에서 차트 데이터 가져옴

        if all_rcv_chart_data_db:
            db_KR_STOCK_DATA.create(stock_code, columns_db, data_types_db)  # db에 종목 테이블 생성
            db_KR_STOCK_DATA.insert(stock_code, columns_db, all_rcv_chart_data_db)  # 서버에서 가져온 데이터 db에 insert

            # 최근 데이터 날짜/시간 업데이트
            cls.db_kr_operation_data.update(
                "KR_Stock_List", recent_date_time_column, all_rcv_chart_data_db[0][0], "stock_code = '" + stock_code + "'",
            )
        else:
            print("NO DATA")  # 서버에서 가져온 데이터가 비어있을 경우

    @classmethod
    def get_chart_data(self, stock_code, creon_idxs, chart_type, chart_period, recent_data_date_time, rq_data_count=200000):
        """
        creon 서버에서 차트 데이터를 가져옴

        Parameters:
            stock_code (str): 종목 코드

            creon_idxs (list): 가져올 데이터의 타입 인덱스들 (creon api에 정의된)

            chart_type (str): 가져올 차트데이터의 종류 ("D" - 일봉 차트, "m" - 분봉차트)

            chart_period (int): 데이터의 간격 (1, 5 (분/일))

            recent_data_date_time (int): 보유하고있는 가장 최근 데이터의 시점

            rq_data_count (int): 가져올 데이터의 최대 갯수

        Returns:
            (list[][]): 받아온 데이터

            (None): 데이터 요청 오류발생 혹은 받아온 데이터가 없을 경우
        """
        creon_stock_chart = CreonStockChart()

        # 받아올 데이터 정보 세팅
        creon_stock_chart.set_input_value(0, stock_code)  # 받아올 종목 코드
        creon_stock_chart.set_input_value(1, ord("2"))  # 개수로 받아오기
        creon_stock_chart.set_input_value(4, rq_data_count)  # 데이터 갯수
        creon_stock_chart.set_input_value(5, creon_idxs)  # 데이터 종류
        creon_stock_chart.set_input_value(6, ord(chart_type))  # 데이터 타입 (월 / 일 / 분)
        creon_stock_chart.set_input_value(7, chart_period)  # 데이터 간격
        creon_stock_chart.set_input_value(9, ord("1"))  # 수정주가 사용

        all_rcv_data = []  # 데이터 받을 리스트

        while True:
            creon_stock_chart.block_request()  # 데이터 요청

            # 요청 상태 체킹 (오류가있다면 메시지 출력후 종료함)
            if not creon_stock_chart.check_rq_status():
                return None

            # 수신된 데이터 수 만큼 반복
            for row_idx in range(creon_stock_chart.get_header_value(3)):
                rcv_row_data = []  # 차트 한줄 데이터

                # 차트 한줄 데이터 받아오기
                for col_idx in range(len(creon_idxs)):
                    rcv_row_data.append(creon_stock_chart.get_data_value(col_idx, row_idx))

                # 차트 한줄 데이터가 비었을 경우 리턴
                if not rcv_row_data:
                    return all_rcv_data

                # 분봉차트일 경우 날짜와 시간을 합친후 rcv_row_data[1]에 저장, rcv_row_data[0]은 삭제
                if chart_type == "m":
                    if rcv_row_data[1] < 999:
                        rcv_row_data[0] *= 10
                    rcv_row_data[1] = int(str(rcv_row_data[0]) + str(rcv_row_data[1]))
                    del rcv_row_data[0]

                # 받아온 데이터의 날짜/시간이 최근 날짜/시간 보다 더 전이거나 같은경우 리턴
                if rcv_row_data[0] <= recent_data_date_time:
                    return all_rcv_data

                all_rcv_data.append(rcv_row_data)  # 데이터 한줄 추가

                # 데이터를 요청한 갯수 만큼 받았을 경우 리턴
                if rq_data_count == len(all_rcv_data):
                    return all_rcv_data

            # 서버가 가진 모든 데이터를 다 받았을 경우 리턴
            if not creon_stock_chart.is_continue():
                return all_rcv_data


def main():
    CreonLogin.connect()

    # StockData.update_stock_list()
    StockData.update_all_chart_data("D")
    # StockData.update_all_chart_data("m")


if __name__ == "__main__":
    main()
