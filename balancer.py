#!/usr/bin/python3

import socket
import configparser
import datetime
import random
import threading
import time
from kazoo.client import KazooClient

# Читаем конфигурационный файл.
config = configparser.ConfigParser()
config.read("./balancer.ini")
b_log, n_count = config["balancer"]["log"], int(config["balancer"]["nodescount"])
n_zoo_host = config["zoo"]["host"]
l_ip, l_port = config["leo"]["ip"], int(config["leo"]["port"])

# Функция записи в лог с выводом на экран.
def Filelog(s):
    print(s)
    with open(b_log, 'a') as fd: fd.write("{} {}\n".format(datetime.datetime.now(), s))
    
def FilelogStat(f, s):
    with open("{}_stat.csv".format(f), 'a') as fd:
         fd.write("{};{}\n".format(datetime.datetime.now(), s))    
 
Filelog("Starting balancer ({} nodes)...".format(n_count))


# Список узлов. Каждый элемент -словарь, где 'N' - номер узла (ключ), 'ip' - ip-адрес, 'port' - порт на котором слушает узел
# 'conn' - объект-сокет (сначала None, потом устанавливается), 'name' - имя узла (получаем при инициализации соединния).
# 'queue' - очередь сообщений на отправку в многопоточном режиме. 'thread' - поток, обрабатывающий данный узел
nodes = []

# Список привязок клиент-узел для алгоритма sticky sessions.
# Каждый элемент - словарь, где 'client' - номер клиента, 'idx' - номер узла, назначенного клиенту.
sticky_list = []

# Глобальные переменные - номер сообщения и последний присвоенный номер обрабатывающего узла
g_mess_num = 0
g_node_idx = 0

# Соединение с Leonhard
l_conn = None

# Считываем из инишника информацию о заданных узлах, добавляем их в список узлов.
for i in range(n_count):
    n_idx = "node{}".format(i)
    nodes.append({'N' : i, 'ip' : config[n_idx ]["ip"], 'port' : int(config[n_idx ]["port"]),
                  'conn' : None, 'name' : None, 'queue' : [], 'thread' : None})


# Функция треда чтения статистики из zookeeper
def ZkProcess():
    Filelog('Zookeeper Thread started')
    while (zk_connected !=0 ):
        for zk_n in zk.get_children("/"):
            zk_cpu = '/{}/cpuusage'.format(zk_n)
            zk_mem = '/{}/freemem'.format(zk_n)
            data_cpu = None
            data_mem = None
            if zk.exists(zk_cpu):
                data_cpu, stat_cpu = zk.get(zk_cpu)
            if zk.exists(zk_mem):
                data_mem, stat_mem = zk.get(zk_mem)
            if data_cpu and data_mem:
                FilelogStat(zk_n, "{};{}".format(data_cpu.decode(), data_mem.decode()))
        time.sleep(1)
    Filelog('Zookeeper Thread finished')

# Соединяемся с ZooKeeper
zk_connected = 0
if n_zoo_host:
    try:
        zk = KazooClient(hosts=n_zoo_host)
        zk.start()
        Filelog('Connected to ZooKeeper {}'.format(n_zoo_host))
        Filelog('ZooKeeper nodes {}'.format(zk.get_children("/")))
        zk_connected = 1
        zk_thread = threading.Thread(target=ZkProcess)
        zk_thread.start()
    except:
        Filelog('ZooKeeper connection error on {}'.format(n_zoo_host))
    

# Функция - обработчик треда для каждого узла с номером nodeid
def ThreadProcess(nodeid):
    # Счетчик сообщений, переданных тредом данному узлу
    t_counter = 0
    Filelog('Thread {} started'.format(nodeid)) 
    while nodes[nodeid]['conn'] != None:
        # Проверяем, есть ли сообщения в очереди для узла
        if len(nodes[nodeid]['queue']) > 0:
            t_counter += 1;
            # Забираем первый элемент из очереди
            item = nodes[nodeid]['queue'].pop(0);
            Filelog('Node {}. Num = {}({}). SEND Client {} : {} Queue {}'.format(nodeid, item['num'], t_counter,
                                                                      item['client'], item['msg'], len(nodes[nodeid]['queue'])))
            # Подготавливаем сообщение (глобальный номер сообщенияб номер клиента, сами данные)
            datasend = '{};{};{}'.format(item['num'], item['client'], item['msg'])
            # Отправляем сообщение узлу и получаем ответ
            answer = Sendrecv(nodes[nodeid]['conn'], datasend)
            Filelog('Node {}. Num = {}({}). RECV {}'.format(nodeid, item['num'], t_counter, answer))
        else:
            time.sleep(0.001)
    Filelog('Thread {} finished. {} messages processed'.format(nodeid, t_counter)) 


# Функция передает данные в соединение (если оно существует) и возвращает ответ сервера
def Sendrecv(conn, msg):
    try:
        if (conn):
            conn.sendall(msg.encode())
            data = conn.recv(1024)
            return data.decode()
        else:
            return None
    except Exception:
        return None

# Функция возвращает номер очередного узла, к которому установлено активное соединение,
# без привязки к клиенту. Если использовать только ее - получится round robin
def getNextNode():
    global g_node_idx
    curridx = g_node_idx
    ln = len(nodes)
    while True:
        g_node_idx += 1
        if (g_node_idx >= ln): g_node_idx = 0
        if nodes[g_node_idx]['conn'] != None: break
        if g_node_idx == curridx: break
    Filelog('getNextNode {} {} {}'.format(ln, curridx, g_node_idx)) 
    return g_node_idx

# Функция реализует алгоритм sticky sessions. Проверяет, назначен ли данному клиенту какой-нибудь узел.
# Если назначен - возвращает его, если не назначен - назначает по round robin,
# делает запись в список привязок, возвращает назначенный номер узла.
def getStickyNode(client_id):
    for s in sticky_list:
        if s['client'] == client_id: return s['idx']
    newidx = getNextNode()
    Filelog('getStickyNode {} {}'.format(client_id, newidx)) 
    sticky_list.append({'client' : client_id, 'idx' : newidx})
    return newidx


# Функция отправки данных одному из узлов и получения ответа от него.
# Номер клиента генерируется случайным образом из определенного диапазона (т.к. реальных "клиентов" пока нет).
# Данному клиенту выбирается или назначается номер обрабатывающего узла по алгоритму sticky sessions.
# Данные добавляются в очередь асинхронного обработчика выбранного узла
# Функция возвращает номер обрабатывающего узла
def SendRecvMsg(msg):
    global g_mess_num
    g_mess_num += 1
    cl_id = random.randint(1, 1500)   
    if l_conn:
        newidx = int(Sendrecv(l_conn, "{}".format(cl_id)));
        Filelog("Leonhard: node {} for client {}".format(newidx, cl_id))
    else:
        newidx = getStickyNode(cl_id)
    # newidx = getStickyNode(cl_id)  - для round robin
    nodes[newidx]['queue'].append({'num' : g_mess_num, 'client' : cl_id, 'msg' : msg})
    return newidx 


# Далее непосредственно тело программы

# Сначала - первичная инициализация соединений
# Для каждого узла создаем соединение и делаем первичный запрос - получаем имя узла
for n in nodes:
    Filelog("Node {} ({}:{})".format(n['N'], n['ip'], n['port']))
    try:
        # Создаем сокет и привязываемся к нужному ip и порту
        n['conn'] = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        n['conn'].connect((n['ip'], n['port']))
        
        # Делаем поток-обработчик сообщений для данного узла
        n['thread'] = threading.Thread(target=ThreadProcess, args=(n['N'],))
        n['thread'].start()
        
        # Первое сообщение - запрашиваем имя узла. Запросы пока синхронные, в основном потоке приложения
        n['name'] = Sendrecv(n['conn'], 'init');
        Filelog('Connected to {}:{}  Get node name: {}'.format(n['ip'], n['port'], n['name']));
    except Exception:
        n['conn'] = None;
        Filelog('Connection error to {}:{}'.format(n['ip'], n['port']));
        
        
# Соединяемся с leonhard, если он прописан, говорим ему количество узлов
if l_ip: 
    try:
        l_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        l_conn.connect((l_ip, l_port))
        l_answer = Sendrecv(l_conn, "{}".format(n_count));
        Filelog('Connected to leonhard {}:{} Answer {}'.format(l_ip, l_port, l_answer));  
    except Exception:
        l_conn = None
        Filelog('Connection error to leonhard {}:{}'.format(l_ip, l_port));        
        

# Основной цикл программы. Считываем сообщения из консоли.
# Произвольное сообщение отправляется непосредственно в функцию SendRecvMsg
# По сообщению file происходит построчная отправка всех данных из файла data.txt
# Сообщение quit прерывает выполнение программы, производится закрытие всех соединений и выход.
try:
    while True:
        mess = input('Message?')
        if mess == 'quit':  break
        elif mess == 'file':
            # Отправляем файл построчно
            f = open('./data.txt')
            for line in f:
                idx = SendRecvMsg(line.rstrip())
                time.sleep(random.randint(10, 20)/ 1000)
        else: idx = SendRecvMsg(mess)
    time.sleep(1)
finally:
# Закрываем все активные соединения
    print(sticky_list)
    if zk_connected:
        zk_connected = 0
        zk.stop()
    if l_conn:
        l_conn.close()
    for n in nodes:
        if (n['conn'] != None):
            n['conn'].close()
            n['conn'] = None





