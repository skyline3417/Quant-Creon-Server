import time
import threading
from queue import Queue
import socket
import json

import stock_data
import trade
import database
from stock_data_realtime import StockTickData, StockDataRt
from trade_status_realtime import TradeStatusRt
from trade_info_enum import *

HOST = ""
PORT = 30565


class QuantServer:
    def __init__(self):
        self.stock_data_rt = {}
        self.trade_status_rt = None

        self.stock_data_rt_sub_q = Queue()
        self.order_q = Queue()

        self.conn_client_dict = {}
        self.conn_client_lock = threading.Lock()
        self.recv_q = None

    def start_server(self):
        trade.BalanceData.update_stock_balance()
        trade.TradeData.update_unconcluded_order()
        self.trade_status_rt = TradeStatusRt(self.trade_status_rt_event)
        self.trade_status_rt.subscribe()

        execute_order_thread = threading.Thread(target=self.execute_order, args=(self.order_q,))
        stock_data_rt_sub_thread = threading.Thread(target=self.excute_stock_data_rt_sub, args=(self.stock_data_rt_sub_q,))
        execute_recv_req_thread = threading.Thread(target=self.execute_recv_req)

        execute_order_thread.start()
        stock_data_rt_sub_thread.start()
        execute_recv_req_thread.start()

        db_kr_operation_data = database.MariaDB("KR_OPERATION_DATA")
        balance_stock_code_list = db_kr_operation_data.select("KR_Stock_Balance", "stock_code")

        if balance_stock_code_list:
            if not isinstance(balance_stock_code_list, list):
                balance_stock_code_list = [balance_stock_code_list]

            req = {"username": "system", "req_type": "stock_data_rt", "req_data": {"set_status": True, "stock_code_list": balance_stock_code_list}}
            self.stock_data_rt_sub_q.put(req)

        self.start_socket()

    def start_socket(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)

        while True:
            print("Waiting on port : %d" % PORT)

            conn_socket, addr = server_socket.accept()

            login_thread = threading.Thread(target=self.login, args=(conn_socket,))
            login_thread.start()

    def login(self, conn_socket):
        recv_data = conn_socket.recv()

        username = json.loads(recv_data.decode("utf-8"))["username"]
        password = json.loads(recv_data.decode("utf-8"))["password"]

        db_mysql = database.MariaDB("mysql")

        if username in self.conn_client_dict:
            close_socket(username)

        if password == db_mysql.select("user", "Password", "User = '" + username + "'"):
            conn_socket.send("SUCCESS".encode("utf-8"))

            self.conn_client_dict[login_data[username]]["conn_socket"] = conn_socket
            self.conn_client_dict[login_data[username]]["send_q"] = Queue()

            socket_send_thread = threading.Thread(target=self.socket_send, args=(username,))
            socket_recv_thread = threading.Thread(target=self.socket_recv, args=(username,))

            socket_send_thread.start()
            socket_recv_thread.start()

        else:
            conn_socket.send("FAIL".encode("utf-8"))
            conn_socket.close()

    def socket_send(self, username):
        while True:
            try:
                send_data = self.conn_client_dict[username]["send_q"].get()

                if send_data == "CLOSE":
                    break

                self.conn_client_dict[username]["conn_socket"].sendall(send_data.encode("utf-8"))
            except:
                break

            time.sleep(0.001)

    def socket_recv(self, username):
        while True:
            try:
                recv_data = self.conn_client_dict[username]["conn_socket"].recv(1024).decode("utf-8")
            except:
                self.close_socket(username)
                break

            if recv_data == "CLOSE":
                self.close_socket(username)
                break

            self.recv_q.put((username, recv_data,))

    def close_socket(self, username):
        self.conn_client_lock.acquire()

        if not username in self.conn_client_dict:
            self.conn_client_lock.release()
            return

        self.conn_client_dict[username]["conn_socket"].close()
        self.conn_client_dict[username]["send_q"].put("CLOSE")

        req = {
            "username": username,
            "req_type": "stock_data_rt",
            "req_data": {"set_status": False, "stock_code_list": "ALL"},
        }
        self.stock_data_rt_req_q.put(req)

        del self.conn_client_dict[username]

        self.conn_client_lock.release()

    def insert_send_q(self, username, data):
        self.conn_client_lock.acquire()

        if username in self.conn_client_dict:
            self.conn_client_dict[username]["send_q"].put(data)

        self.conn_client_lock.release()

    def stock_data_rt_event(self, stock_tick_data):
        data_json = {
            "res_type": "trade_status_rt",
            "res_data": {
                "stock_code": stock_tick_data.stock_code,
                "date_time": stock_tick_data.date_time,
                "single_price_flag": stock_tick_data.single_price_flag.name,
                "price": stock_tick_data.price,
                "day_changed": stock_tick_data.day_changed,
                "qty": stock_tick_data.qty,
                "vol": stock_tick_data.vol,
            },
        }
        if stock_tick_data.stock_code in self.stock_data_rt:
            for username in self.stock_data_rt[stock_tick_data.stock_code]["requester"]:
                self.insert_send_q(username, json.dumps(data_json))

    def trade_status_rt_event(self, trade_info):
        if trade_info.e_conclusion_type == CONCLUSION_TYPE.CONCLUDED:
            req = {
                "username": "system",
                "req_type": "stock_data_rt",
                "req_data": {"set_status": bool(trade_info.balance_qty), "stock_code_list": [trade_info.stock_code]},
            }
            self.stock_data_rt_sub_q.put(req)

        data_json = {
            "res_type": "trade_status",
            "res_data": {
                "date_time": trade_info.date_time,
                "stock_name": trade_info.stock_name,
                "qty": trade_info.qty,
                "price": trade_info.price,
                "order_num": trade_info.order_num,
                "origin_order_num": trade_info.origin_order_num,
                "stock_code": trade_info.stock_code,
                "e_order_type": trade_info.e_order_type.name,
                "e_conclusion_type": trade_info.e_conclusion_type.name,
                "e_modify_cancel_type": trade_info.e_modify_cancel_type.name,
                "e_price_type": trade_info.e_price_type.name,
                "e_order_condition": trade_info.e_order_condition.name,
                "avg_price": trade_info.avg_price,
                "able_sell_qty": trade_info.able_sell_qty,
                "balance_qty": trade_info.balance_qty,
                "total_price": trade_info.total_price,
            },
        }

        for username in self.conn_client_dict.keys():
            self.insert_send_q(username, json.dumps(data_json))

    def execute_recv_req(self):
        while True:
            username, recv_data = self.recv_q.get()

            req = json.loads(recv_data)
            req["username"] = username

            if req["req_type"] == "order":
                order_q.put(req)
            elif req["req_type"] == "stock_data_rt":
                stock_data_rt_sub_q.put(req)

    def excute_stock_data_rt_sub(self, stock_data_rt_req_q):
        while True:
            req = stock_data_rt_req_q.get()

            username = req["username"]

            if req["req_data"]["stock_code_list"] == "ALL":
                for stock_code in self.stock_data_rt.keys():
                    if username in self.stock_data_rt[stock_code]["requester"]:
                        self.stock_data_rt[stock_code]["requester"].remove(username)

                    if not self.stock_data_rt[stock_code]["requester"]:
                        self.stock_data_rt[stock_code]["ins"].unsubscribe()
                        del stock_data_rt[stock_code]
            else:
                for stock_code in req["req_data"]["stock_code_list"]:
                    if req["req_data"]["set_status"]:
                        if not stock_code in self.stock_data_rt:
                            self.stock_data_rt[stock_code] = {"ins": StockDataRt(stock_code, self.stock_data_rt_event)}
                            self.stock_data_rt[stock_code]["requester"] = []

                        if not username in self.stock_data_rt[stock_code]["requester"]:
                            self.stock_data_rt[stock_code]["requester"].append(username)
                    else:
                        if username in self.stock_data_rt[stock_code]["requester"]:
                            self.stock_data_rt[stock_code]["requester"].remove(username)

                        if not self.stock_data_rt[stock_code]["requester"]:
                            self.stock_data_rt[stock_code]["ins"].unsubscribe()
                            del stock_data_rt[stock_code]

            json_dict = {"res_type": "stock_data_rt", "res_data": req["req_data"]}
            self.insert_send_q(username, json.dumps(json_dict))

    def execute_order(self, order_q):
        while True:
            req = order_q.get()

            username = req["username"]
            order_info = req["req_data"]

            if order_info["order_data"] == "buy":
                order_num = trade.Order.buy(
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_data"] == "sell":
                order_num = trade.Order.sell(
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_data"] == "modify_type":
                order_num = trade.Order.modify_type(
                    order_info["origin_order_num"],
                    order_info["stock_code"],
                    order_info["qty"],
                    ORDER_CONDITION[order_info["e_order_condition"]],
                    PRICE_TYPE[order_info["e_price_type"]],
                    order_info["price"],
                )
            elif order_info["order_data"] == "modify_price":
                order_num = trade.Order.modify_price(order_info["origin_order_num"], order_info["stock_code"], order_info["qty"], order_info["price"])
            elif order_info["order_data"] == "cancel":
                order_num = trade.Order.cancel(order_info["origin_order_num"], order_info["stock_code"], order_info["qty"])

            json_dict = {"res_type": "order", "res_data": {"order_num": order_num}}
            self.insert_send_q(username, json.dumps(json_dict))


def main():
    quant_server = QuantServer()
    quant_server.start_server()
    while True:
        time.sleep(10)
        pass


if __name__ == "__main__":
    main()

