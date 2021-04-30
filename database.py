# coding=utf-8
import pymysql

MARIA_DB_HOST = ""
MARIA_DB_PORT = 
MARIA_DB_USER = ""
MARIA_DB_PASSWORD = ""
MARIA_DB_CHARSET = ""


class MariaDB:
    """
    maria db 관련 클래스

    Attributes:
        db_name (str): 접속할 db 이름

        db_conn (pymysql.connect): pymysql 연결 인스턴스

        db_cursor (pymysql.cursor): pymysql 커서 인스턴스
    """

    def __init__(self, db_name):
        """
        Parameters:
            db_name (str): 접속할 db 이름
        """
        self.db_name = db_name
        self.db_conn = pymysql.connect(
            host=MARIA_DB_HOST, port=MARIA_DB_PORT, user=MARIA_DB_USER, password=MARIA_DB_PASSWORD, db=self.db_name, charset=MARIA_DB_CHARSET,
        )

        self.db_cursor = self.db_conn.cursor()

    def __del__(self):
        self.db_conn.close()  # 인스턴스 삭제시 db 연결 종료

    def execute(self, query, data=None):
        """
        sql query 문 실행
        data의 갯수는 쿼리문의 포맷코드(%s) 갯수와 일치해야함

        Parameters:
            query (str): 실행시킬 쿼리문

            data
                (list[]): 쿼리문에 포맷코드(%s)있는경우

                (list[][]): 여러개의 쿼리 실행해야 할때 (excutemany)

                (None): 포맷코드가 없는 경우
        """
        # print("EXECUTE QUERY ON DB : " + self.db_name)

        # data가 없을경우
        if not data:
            # print(query)
            pass
        else:
            # data가 이차원 리스트인 경우
            if isinstance(data[0], list):
                # print(query % tuple(data[0]))
                # print("AND " + str(len(data) - 1) + " OTHERS\n")

                self.db_cursor.executemany(query, data)

                return
            else:
                # print(query % tuple(data))
                pass

        # print("\n")

        self.db_cursor.execute(query, data)

    def is_exist(self, table, where):
        """
        테이블 table의 column 컬럼에 data가 있는지 확인

        Parameters:
            table (str): 테이블 이름

            where (str): WHERE 조건

        Returns:
            (bool): 존재여부 (True - 있음, False - 없음)
        """
        self.execute("SELECT * FROM " + table + " WHERE " + where)

        if self.db_cursor.fetchall():
            return True
        else:
            return False

    def select(self, table, columns=None, where=None):
        """
        mysql SELECT 문

        Parameters:
            table (str): 테이블 이름

            columns
                (str): 컬럼 (한개일때)

                (list[str]): 컬럼 리스트
            
            where 
                (str): mysql WHERE 문

                (None): 테이블의 컬럼에 해당하는 데이터 전부 가져오기
        
        Returns:
            (): 단일 데이터

            (list[]): 1행 혹은 1열 데이터

            (list[][]): 다행 다열 데이터
        """
        self.db_conn.commit()

        query = "SELECT "

        if columns:
            if not (isinstance(columns, list) or isinstance(columns, tuple)):
                columns = [columns]  # 컬럼이 리스트나 튜플이 아닌경우

            for column in columns:
                query += column + ", "

            query = query[:-2]
        else:
            query += "*"

        query += " FROM " + table

        if where:
            query += " WHERE " + where

        self.execute(query)
        db_data = self.db_cursor.fetchall()

        if not db_data:
            return None

        if len(db_data) == 1:
            if len(db_data[0]) == 1:
                return db_data[0][0]
            else:
                return list(db_data[0])
        else:
            return_data = []
            if len(db_data[0]) == 1:
                for data in db_data:
                    return_data.append(data[0])

                return return_data
            else:
                return db_data

    def insert(self, table, columns, data):
        """
        mysql INSERT 문

        Parameters:
            table (str): 테이블 이름

            columns
                (str): 컬럼 한개 일 경우

                (list[str]): 컬럼 여러개일 경우
            
            data
                (): 포맷코드 데이터 (컬럼 한개일 경우)

                (list[]): 포맷코드 데이터 (컬럼이 여러개일 경우)

                (list[][]): 포맷코드 데이터 (executemany로 대량 insert 시켜야 할 경우)
        """
        if not isinstance(columns, list) and not isinstance(columns, tuple):
            columns = [columns]

        if not isinstance(data, list) and not isinstance(data, tuple):
            data = [data]

        query = "INSERT INTO " + table + " ("

        for column in columns:
            query += column + ", "

        query = query[:-2]

        query += ") VALUES ("

        for _ in range(len(columns)):
            query += "%s, "

        query = query[:-2]
        query += ")"

        self.execute(query, data)
        self.db_conn.commit()

    def update(self, table, columns, data, where):
        """
        mysql UPDATE 문

        Parameters:
            table (str): 테이블 이름

            columns
                (str): 컬럼이름 (한개일경우)

                (list[str]): 컬럼이름 (여러개일 경우)

            data
                (): 포맷코드 데이터 (컬럼 한개일 경우)

                (list[]): 포맷코드 데이터 (컬럼이 여러개일 경우)

                (list[][]): 포맷코드 데이터 (executemany로 대량 insert 시켜야 할 경우)
            
            where (str): mysql WHERE 문
        """
        if not isinstance(columns, list) and not isinstance(columns, tuple):
            columns = [columns]

        if not isinstance(data, list) and not isinstance(data, tuple):
            data = [data]

        query = "UPDATE " + table + " SET "

        for column in columns:
            query += column + " = %s, "

        query = query[:-2]
        query += " WHERE " + where

        self.execute(query, data)
        self.db_conn.commit()

    def create(self, table, columns, data_types):
        """
        mysql CREATE 문

        Parameters:
            table (str): 테이블 이름

            columns
                (str): 컬럼이름 (한개일 경우)

                (list[str]): 컬럼이름 (여러개의 경우)
            
            data_types
                (str): db 데이터 형식 (컬럼 한개의 경우)

                (list[str]): db 데이터 형식 (컬럼 여러개의 경우)
        """
        if not isinstance(columns, list) and not isinstance(columns, tuple):
            columns = [columns]

        if not isinstance(data_types, list) and not isinstance(data_types, tuple):
            data_types = [data_types]

        query = "CREATE TABLE IF NOT EXISTS " + table + " ("

        for column, type in zip(columns, data_types):
            query += column + " " + type + ", "

        query = query[:-2] + ")"

        self.execute(query)

    def delete(self, table, where=None):
        """
        mysql DELETE 문

        Parameters:
            table (str): 테이블 이름

            where 
                (str): mysql WHERE 문

                (None): 테이블의 레코드 전체 삭제
        """
        query = "DELETE FROM " + table

        if where:
            query += " WHERE " + where

        self.execute(query)
        self.db_conn.commit()

    def drop(self, table):
        """
        mysql DROP 문

        Parameters:
            table (str): DROP 시킬 테이블 이름
        """
        query = "DROP TABLE " + table

        self.execute(query)
        self.db_conn.commit()
