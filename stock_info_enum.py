import enum


# 소속 부
class MARKET_KIND (enum.Enum) :
    NULL      = 0   # 구분없음
    KOSPI     = 1   # 거래소
    KOSDAQ    = 2   # 코스닥
    FREEBOARD = 3   # K-OTC
    KRX       = 4   # KRX
    KONEX     = 5   # KONEX

# 감리 구분
class CONTROL_KIND (enum.Enum) :
    NONE          = 0   # 정상
    ATTENTION     = 1   # 주의
    WARNING       = 2   # 경고
    DANGER_NOTICE = 3   # 위험예고
    DANGER        = 4   # 위험

# 관리 구분
class SUPERVISION_KIND (enum.Enum) :
    NONE   = 0  # 일반종목
    NORMAL = 1  # 관리종목

# 주식 상태
class STOCK_STATUS_KIND (enum.Enum) :
    NORMAL = 0  # 정상
    STOP   = 1  # 거래정지
    BREAK  = 2  # 거래중단

# 구분 코드
class SECTION_KIND (enum.Enum) :
    NULL    = 0     # 구분없음
    ST      = 1     # 주권
    MF      = 2     # 투자회사
    RT      = 3     # 부동산투자회사
    SC      = 4     # 선박투자회사
    IF      = 5     # 사회간접자본투융자회사
    DR      = 6     # 주식예탁증서
    SW      = 7     # 신수인수권증권
    SR      = 8     # 신주인수권증서
    ELW     = 9     # 주식워런트증권
    ETF     = 10    # 상장지수펀드(ETF)
    BC      = 11    # 수익증권
    FETF    = 12    # 해외ETF
    FOREIGN = 13    # 외국주권
    FU      = 14    # 선물
    OP      = 15    # 옵션   
    KN      = 16    # KONEX
    ETN     = 17    # ETN 