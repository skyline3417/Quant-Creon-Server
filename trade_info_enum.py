import enum


# 주문 유형
class ORDER_TYPE(enum.Enum):
    SELL = "1"  # 매도
    BUY = "2"  # 매수


# 주문 조건
class ORDER_CONDITION(enum.Enum):
    NONE = "0"  # 해당 없음
    IOC = "1"  # IOC
    FOK = "2"  # FOK


# 호가 종류
class PRICE_TYPE(enum.Enum):
    NONE = "00"  # 해당 없음
    NORMAL = "01"  # 일반
    MARKET = "03"  # 시장가


# 체결 유형
class CONCLUSION_TYPE(enum.Enum):
    CONCLUDED = "1"  # 체결
    CONFIRMED = "2"  # 확인
    REJECTED = "3"  # 거부
    RECEIVED = "4"  # 접수


# 정정 취소 구분 코드
class MODIFY_CANCEL_TYPE(enum.Enum):
    NONE = "1"  # 일반
    MODIFY = "2"  # 정정 주문
    CANCEL = "3"  # 취소 주문
