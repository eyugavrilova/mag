#!/usr/bin/python3

import socket
import configparser
import datetime
import random
import hashlib
import time

# Читаем конфигурационный файл.
config = configparser.ConfigParser()
config.read("./node.ini")
n_log, n_port, n_name = config["node"]["log"], config["node"]["port"], config["node"]["name"]

# Функция записи в лог с выводом на экран.
def Filelog(s):
    print(s)
    with open(n_log, 'a') as fd:
         fd.write("{} {}\n".format(datetime.datetime.now(), s))
         
Filelog("Starting {} on port {}... oh, shi...".format(n_name, n_port))

# Создаем сокет, привязываем его к порту и начинаем слушать подключения.
srv_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, proto=0)
srv_socket.bind(('', int(n_port)))
srv_socket.listen(100)

while True:
    # Принимаем соединение от клиента.
    clnt_sock, clnt_addr = srv_socket.accept()
    Filelog('Connection from {}'.format(clnt_addr))
    try:
        # Начинаем получать данные от клиента.
        while True:
            data = clnt_sock.recv(1024)
            if not data: break
            dataget = data.decode()
            Filelog('Get {}'.format(dataget))
            # По запросу init - возвращаем имя сервера.
            # В противном случае возвращаем имя и md5-хэш полученного сообщения.
            # Здесь должен быть обработчик входящих данных.
            datasend = n_name
            if (dataget != 'init'):
                # Разделяем входящие данные на номер сообщения, номер клиента и обрабатываемые данные
                darr = dataget.split(';', 2);
                # Вычисляем md5-хэш данных и изображаем занятость
                datasend = '{} {};{};{}'.format(n_name, darr[0], darr[1],
                                                hashlib.md5(darr[2].encode("utf-8")).hexdigest())
                time.sleep(random.randint(10, 50)/ 1000)
            # Возвращаем ответ клиенту.
            clnt_sock.sendall(datasend.encode())
    finally:
        clnt_sock.close()
    break
    
srv_socket.close()

