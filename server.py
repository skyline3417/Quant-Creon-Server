import time
import threading
from queue import Queue
import socket
import json

import stock_data
import trade
import database
from stock_data_realtime import StockTickRt, StockAskBidRt
from trade_status_realtime import TradeStatusRt
from trade_info_enum import *

HOST = ""
PORT = 30565


class QuantServer:
    def __init__(self):
        self.client_conn_dict = {}
        self.task_list = {
            "trade_status_rt_sub": TaskTradeStatusRt(self),
            "order": TaskOrder(self),
            "stock_tick_rt_sub": TaskStockTickRt(self),
            "stock_askbid_rt_sub": TaskStockAskBidRt(self),
        }

    def start_server(self):
        trade.BalanceData.update_stock_balance()
        trade.TradeData.update_unconcluded_order()

        db_kr_operation_data = database.MariaDB("KR_OPERATION_DATA")
        balance_stock_code_list = db_kr_operation_data.select("KR_Stock_Balance", "stock_code")
        if balance_stock_code_list:
            if not isinstance(balance_stock_code_list, list):
                balance_stock_code_list = [balance_stock_code_list]
            req = {"username": "system", "req_type": "stock_data_rt", "req_data": {"set_status": True, "stock_code_list": balance_stock_code_list}}
            self.task_list["stock_tick_rt_sub"].insert_q(req)

        task_socket_thread = threading.Thread(target=self.task_socket, daemon=True)
        task_socket_thread.start()

    def task_socket(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)

        while True:
            print("Waiting on port : %d" % PORT)

            socket_conn, addr = server_socket.accept()

            login_thread = threading.Thread(target=self.login, args=(socket_conn,))
            login_thread.start()

    def login(self, socket_conn):

        try:
            recv_data = socket_conn.recv(1024)
        except:
            return

        username = json.loads(recv_data.decode("utf-8"))["username"]
        password = json.loads(recv_data.decode("utf-8"))["password"]

        db_mysql = database.MariaDB("mysql")

        if username in self.client_conn_dict:
            self.client_conn_dict[username].close_client()

        if password == db_mysql.select("user", "Password", "User = '" + username + "'"):
            socket_conn.send("SUCCESS".encode("utf-8"))
            print("login success : ", username)
            self.client_conn_dict[username] = ClientConn(username, socket_conn, self)

        else:
            print("login failed")
            socket_conn.send("FAIL".encode("utf-8"))
            socket_conn.close()

    def insert_send_q(self, username, data):
        if username in self.client_conn_dict:
            self.client_conn_dict[username].insert_send_q(data)

    def delete_client(self, username):
        self.task_list["stock_tick_rt_sub"].delete_user(username)
        # self.task_list["stock_askbid_rt_sub"].delete_user(username)
        self.task_list["trade_status_rt_sub"].delete_user(username)
        del self.client_conn_dict[username]


class ClientConn:
    def __init__(self, username, socket_conn, caller):
        self.username = username
        self.socket_conn = socket_conn
        self.send_q = Queue()
        self.recv_q = Queue()

        self.caller = caller

        socket_send_thread = threading.Thread(target=self.socket_send, daemon=True)
        socket_recv_thread = threading.Thread(target=self.socket_recv, daemon=True)

        socket_send_thread.start()
        socket_recv_thread.start()

    def socket_send(self):
        while True:
            try:
                send_data = self.send_q.get()

                if send_data == "CLOSE":
                    break

                self.socket_conn.sendall(send_data.encode("utf-8"))
            except:
                break

            time.sleep(0.001)
        print("send end")

    def insert_send_q(self, data):
        self.send_q.put(data)

    def socket_recv(self):
        execute_recv_req_thread = threading.Thread(target=self.execute_recv_req, daemon=True)
        execute_recv_req_thread.start()

        while True:
            try:
                recv_data = self.socket_conn.recv(1024).decode("utf-8")
            except:
                self.close_client()
                break

            if recv_data == "CLOSE" or recv_data == "":
                self.close_client()
                break

            self.recv_q.put(recv_data)

    def execute_recv_req(self):
        while True:
            recv_data = self.recv_q.get()

            if recv_data == "CLOSE":
                break

            req = json.loads(recv_data)
            req["username"] = self.username
            print(req)
            self.caller.task_list[req["req_type"]].insert_q(req)
        print("execute end")

    def close_client(self):
        self.socket_conn.close()
        self.insert_send_q("CLOSE")
        self.recv_q.put("CLOSE")

        self.caller.delete_client(self.username)


class TaskTradeStatusRt(threading.Thread):
    def __init__(self, caller):
        threading.Thread.__init__(self)

        self.caller = caller

        self.trade_status_rt = TradeStatusRt(self.event)

        self.sub_req_q = Queue()
        self.sub_username_list = []

        self.setDaemon(True)
        self.start()

    def run(self):
        self.trade_status_rt.subscribe()

        while True:
            req = self.sub_req_q.get()

            username = req["username"]
            sub_req = req["req_data"]

            if sub_req["set_status"]:
                if not username in self.sub_username_list:
                    self.sub_username_list.append(username)
            else:
                if username in self.sub_username_list:
                    self.sub_username_list.remove(username)

    def event(self, trade_info):
        print("event!!!!")
        if trade_info["e_conclusion_type"] == CONCLUSION_TYPE.CONCLUDED:
            req = {
                "username": "system",
                "req_type": "stock_data_rt",
                "req_data": {"set_status": bool(trade_info["balance_qty"]), "stock_code_list": [trade_info["stock_code"]]},
            }
            self.caller.task_list["stock_tick_rt_sub"].insert_q(req)

        trade_info["e_order_type"] = trade_info["e_order_type"].name
        trade_info["e_conclusion_type"] = trade_info["e_conclusion_type"].name
        trade_info["e_modify_cancel_type"] = trade_info["e_modify_cancel_type"].name
        trade_info["e_price_type"] = trade_info["e_price_type"].name
        trade_info["e_order_condition"] = trade_info["e_order_condition"].name

        data_json = {"res_type": "trade_status", "res_data": trade_info}

        for username in self.sub_username_list:
            self.caller.insert_send_q(username, json.dumps(data_json))

    def delete_user(self, username):
        if username in self.sub_username_list:
            self.sub_username_list.remove(username)

    def insert_q(self, data):
        self.sub_req_q.put(data)


class TaskStockDataRt(threading.Thread):
    def __init__(self, res_type, rt_class, caller):
        threading.Thread.__init__(self)
        self.res_type = res_type
        self.rt_class = rt_class
        self.caller = caller

        self.sub_req_q = Queue()
        self.sub_status_dict = {}

        self.setDaemon(True)
        self.start()

    def run(self):
        while True:
            req = self.sub_req_q.get()

            username = req["username"]
            sub_req = req["req_data"]

            if sub_req["set_status"]:
                for stock_code in sub_req["stock_code_list"]:
                    if not stock_code in self.sub_status_dict:
                        self.sub_status_dict[stock_code] = {}
                        self.sub_status_dict[stock_code]["ins"] = self.rt_class(stock_code, self.event)
                        self.sub_status_dict[stock_code]["user_list"] = []

                    if not username in self.sub_status_dict[stock_code]["user_list"]:
                        self.sub_status_dict[stock_code]["user_list"].append(username)
            else:
                for stock_code in sub_req["stock_code_list"]:
                    if stock_code in self.sub_status_dict:
                        if username in self.sub_status_dict[stock_code]["user_list"]:
                            self.sub_status_dict[stock_code]["user_list"].remove(username)

                        if not self.sub_status_dict[stock_code]["user_list"]:
                            self.sub_status_dict[stock_code]["ins"].unsubscribe()
                            del self.sub_status_dict[stock_code]
            print(self.sub_status_dict)

    def insert_q(self, data):
        self.sub_req_q.put(data)

    def event(self, stock_rt_data):
        if self.res_type == "stock_tick_rt_data":
            stock_rt_data["e_market_hours_kind"] = stock_rt_data["e_market_hours_kind"].name

        data_json = {"res_type": self.res_type, "res_data": stock_rt_data}
        if stock_rt_data["stock_code"] in self.sub_status_dict:
            for username in self.sub_status_dict[stock_rt_data["stock_code"]]["user_list"]:
                self.caller.insert_send_q(username, json.dumps(data_json))

    def delete_user(self, username):
        for stock_code in self.sub_status_dict.keys():
            if username in self.sub_status_dict[stock_code]["user_list"]:
                self.sub_status_dict[stock_code]["user_list"].remove(username)

            if not self.sub_status_dict[stock_code]["user_list"]:
                self.sub_status_dict[stock_code]["ins"].unsubscribe()
                del self.sub_status_dict[stock_code]


class TaskStockTickRt(TaskStockDataRt):
    def __init__(self, caller):
        TaskStockDataRt.__init__(self, "stock_tick_rt_data", StockTickRt, caller)


class TaskStockAskBidRt(TaskStockDataRt):
    def __init__(self, caller):
        TaskStockDataRt.__init__(self, "stock_askbid_rt_data", StockAskBidRt, caller)


class TaskOrder(threading.Thread):
    def __init__(self, caller):
        threading.Thread.__init__(self)
        self.order_q = Queue()
        self.caller = caller

        self.setDaemon(True)
        self.start()

    def run(self):
        while True:
            req = self.order_q.get()

            username = req["username"]
            order_info = req["req_data"]

            if order_info["order_type"] == "buy":
                order_num = trade.Order.buy(
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_type"] == "sell":
                order_num = trade.Order.sell(
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_type"] == "modify_type":
                order_num = trade.Order.modify_type(
                    order_info["origin_order_num"],
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_type"] == "modify_price":
                order_num = trade.Order.modify_price(order_info["origin_order_num"], order_info["stock_code"], order_info["qty"], order_info["price"])
            elif order_info["order_type"] == "cancel":
                order_num = trade.Order.cancel(order_info["origin_order_num"], order_info["stock_code"], order_info["qty"])

            json_dict = {"res_type": "order", "res_data": {"order_num": order_num}}
            self.caller.insert_send_q(username, json.dumps(json_dict))

    def insert_q(self, data):
        self.order_q.put(data)


def main():
    quant_server = QuantServer()
    quant_server.start_server()
    while True:
        time.sleep(10)
        pass


if __name__ == "__main__":
    main()

