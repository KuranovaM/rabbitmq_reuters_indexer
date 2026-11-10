import glob
import json
import subprocess
import sys
import time

import pika

from etl import send_to


def wr(num, *mess):
    with open(f'man_log{num}.txt', 'a') as f:
        print(*mess, file=f)

# def find():
#     pass
class Manager:
    def __init__(self, num, counts, k):
        self.k = k
        self.connection = None
        self.channel = None
        self.find_res = []
        self.data = {}
        self.load_k = 0
        self.find_k = -1
        self.info_res = []
        self.info_k = 0
        self.keeper_count = 0
        self.live_k = set()
        self.last_seen = {}
        self.hb_timeout = 3  # пинг keeper
        self.is_running = True
        self.last_count = k
        self.find_word = None
        self.is_work = None


        self.im_lider = False
        self.is_election = False
        self.manage_count = counts
        self.self_num = num
        self.last_leader_hb = 0
        self.got_ok = False
        self.election_started = 0
        self.leader_id = counts - 1
        self.client_tag = None
        self.keeper_tag = None

        self.leader_timeout = 5.0  # 4.0
        self.hb_interval = 1.0
        self.last_hb_sent = 0.0

        self.is_end = False

    def start_proc(self, num, tp, param = None):
        try:
            wr(self.self_num, f'open {tp}.py')
            process = subprocess.Popen(
                [sys.executable, f"{tp}.py", str(num), str(param)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except Exception as e:
            wr(self.self_num, "============= ERROR!!!", e)

    def start_election(self):
        self.is_election = True
        self.got_ok = False
        self.election_started = time.time()

        sent = False
        for i in range(self.self_num + 1, self.manage_count):
            self.send_to(f'to_manage{i}_queue',
                         {'mess': 'election', 'data': self.self_num})
            sent = True

        if not sent:
            self.announce_coordinator()

    def announce_coordinator(self):
        """
        Разослать конец выборов
        :return:
        """
        wr(self.self_num, 'IM LIDER')
        self.is_election = False
        self.got_ok = False
        self.broadcast({'mess': 'coordinator', 'data': self.self_num})
        self.become_leader()

    def broadcast(self, msg):
        for i in range(self.manage_count):
            if i != self.self_num:
                self.send_to(f'to_manage{i}_queue', msg)

    def become_leader(self):
        self.im_lider = True
        self.is_election = False
        self.got_ok = False
        self.leader_id = self.self_num
        self.last_leader_hb = time.time()

        wr(self.self_num, 'tyt')
        try:
            if self.client_tag is None:
                self.client_tag = self.channel.basic_consume(
                    queue='to_manager_queue',
                    on_message_callback=self.processing,
                    auto_ack=True, # False
                )

                wr(self.self_num, 'tyt1')
        except Exception as e:
            wr(self.self_num, e)
        if self.keeper_tag is None:
            self.keeper_tag = self.channel.basic_consume(
                queue='to_manager_from_k',
                on_message_callback=self.find,
                auto_ack=True,
            )
        wr(self.self_num, 'tyt2')
        try:
            self.live_k = set(range(self.k))
        except Exception as e:
            wr(self.self_num, 'tyt3', e)
        self.last_seen = {i: time.time() for i in range(self.k)}
        # wr(self.self_num, 'tyt4')
        wr(self.self_num, f'I AM LEADER ({self.self_num}), k={self.k}')
        self.check_mess_lid()

    def become_follower(self, leader_id):
        was_leader = self.im_lider
        self.im_lider = False
        self.is_election = False
        self.got_ok = False
        self.leader_id = leader_id

        if was_leader:
            if self.client_tag:
                try:
                    self.channel.basic_cancel(self.client_tag)
                except Exception as e:
                    wr(self.self_num, 'NOT LEADER ERR', e)
                self.client_tag = None
            if self.keeper_tag:
                try:
                    self.channel.basic_cancel(self.keeper_tag)
                except Exception as e:
                    wr(self.self_num, 'NOT LEADER ERR @@@', e)
                self.keeper_tag = None

        self.last_leader_hb = time.time()
        wr(self.self_num, f'follower, leader={leader_id}')

    def handle_control(self, ch, method, properties, body):
        msg = json.loads(body.decode())
        mess = msg['mess']
        sender = msg['data']

        if mess == 'heartbeat':
            self.last_leader_hb = time.time()
            # if sender != self.leader_id:
            #     self.become_follower(sender)

        elif mess == 'election':
            if sender < self.self_num:
                self.send_to(f'to_manage{sender}_queue',
                             {'mess': 'ok', 'data': self.self_num})
                if not self.is_election:
                    self.start_election()

        elif mess == 'ok':
            self.got_ok = True

        elif mess == 'coordinator':
            self.become_follower(sender)
            self.last_leader_hb = time.time()

        elif mess == 'set_k':
            self.k = msg['k']

        elif mess == 'words':
            data = msg['data']
            for w in data['words']:
                self.data.setdefault(w.lower(), set()).add(data['num'])

        elif mess == 'purge_sync':
            self.data.clear()

        elif mess == 'exit':
            self.is_running = False
            self.is_end = True
            ch.stop_consuming()
            wr(self.self_num, 'EXIT send')


        elif mess == 'kill_m':
            self.send_to('to_client_queue', f'Manager {self.self_num} killed')
            self.is_running = False
            self.is_end = True
            ch.stop_consuming()
            wr(self.self_num, 'EXIT')


    def start(self):
        """
        Создаем основные очереди для связи с менеджером
        to_manager_queue - от клиента
        to_manager_from_k - от остальных
        to_manage{i}_queue - очередь к другим менеджерам
        :return:
        """
        wr(self.self_num, 'func start')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        wr(self.self_num, 'connection')

        for i in range(self.manage_count):
            self.channel.queue_declare(queue=f'to_manage{i}_queue')
            self.channel.queue_purge(queue=f'to_manage{i}_queue')

        self.channel.basic_consume(
            queue=f'to_manage{self.self_num}_queue',
            on_message_callback=self.handle_control,
            auto_ack=True,
        )
        self.channel.queue_declare(queue='to_manager_from_k')
        self.channel.queue_purge(queue=f'to_manager_from_k')
        self.channel.queue_declare(queue='to_manager_queue')
        # self.channel.queue_purge(queue=f'to_manager_queue')

        if self.self_num == self.manage_count - 1:
            self.announce_coordinator()
        else:
            self.last_leader_hb = time.time()

        try:
            while self.is_running:
                self.connection.process_data_events(time_limit=1)
                if not self.is_running:
                    break
                if self.im_lider:
                    self.check_heartbeats()  # для keeper'ов
                self.check_mess_lid()
                if self.is_end:
                    break

        except Exception as e:
            wr(self.self_num, "Завершение... ", e)
        finally:
            self.connection.close()

    def check_mess_lid(self):
        """
        Проверяем либо сигналы от лидера, либо проверяем выборы
        :return:
        """
        now = time.time()
        if self.im_lider:
            if now - self.last_hb_sent >= self.hb_interval:
                self.broadcast({'mess': 'heartbeat', 'data': self.self_num})
                self.last_hb_sent = now
            return

        # wr(self.self_num, 'check_mess_lid')
        if not self.is_election and now - self.last_leader_hb > self.leader_timeout:
            wr(self.self_num, 'leader timeout -> start election')
            self.start_election()

        if self.is_election:
            if not self.got_ok:
                if now - self.election_started > self.leader_timeout:
                    wr(self.self_num, 'no OK -> I am leader')
                    self.announce_coordinator()
            else:
                if now - self.election_started > self.leader_timeout * 2:
                    wr(self.self_num, 'no COORDINATOR -> restart election')
                    self.is_election = False
                    self.start_election()

    def send_to(self, queue, body, reply_to = None):
        """
        Отправляем данные в нужную очередь + добавляем куда слать ответ если надо
        :param queue: куда
        :param body: что
        :param reply_to: куда ответить или none
        :return:
        """
        # wr(self.self_num, 'send to ' + queue + ' body ' + str(body))
        properties = None
        if reply_to is not None:
            properties = pika.BasicProperties(reply_to=reply_to.encode())
        if type(body) == dict:
            body = json.dumps(body)
        self.channel.basic_publish(exchange='', routing_key=queue.encode(),
                          properties=properties,
                          body=body.encode())

    def get_keeper_q(self, num):
        """
        Выдаем нужную очередь, в зависимости от того, умер процесс или нет
        :param num: номер по которому узнаеи
        :return:
        """
        if num in self.live_k:
            return f'to_keeper{num}_queue'
        return f'to_r{num}_from_man'

    def check_heartbeats(self):
        """
        Проверяем разницу во времени
        (если больше, то он умер)
        :return:
        """
        now = time.time()
        for num, t in list(self.last_seen.items()):
            if num in self.live_k and now - t > self.hb_timeout:
                self.live_k.discard(num)
                wr(self.self_num, f'keeper {num} DEAD (no heartbeat {now - t:.0f}s)')
                # self.send_to('to_client_queue', f'Keeper {num} seems dead')
        if self.last_count != len(self.live_k):
            if self.is_work == 'info':
                self.is_work = None
                self.info_res.clear()
                self.info_k = 0
                self.send_k_info()
            elif self.is_work == 'find':
                self.is_work = None
                self.find_res.clear()
                self.send_k_find()



    def processing(self, ch, method, properties, body):
        """
        Обработчик от клиента
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        self.send_to('to_client_queue', 'alive')
        mess_j = json.loads(body.decode())
        mess = mess_j['mess']
        wr(self.self_num, 'proc with mess ' + str(mess_j))
        if mess == 'start':
            """
            Запуск хранителей и etl + очистка их очередей
            """
            self.k = mess_j['data']
            wr(self.self_num, 'start new proc')
            for i in range(self.k):
                self.channel.queue_declare(queue=f'to_etl{i}_queue')
                self.channel.queue_purge(queue=f'to_etl{i}_queue')
                self.channel.queue_declare(queue=f'to_keeper{i}_queue')
                self.channel.queue_purge(queue=f'to_keeper{i}_queue')
                self.channel.queue_declare(queue=f'to_r{i}_from_man')
                wr(self.self_num, f'start etl num {i}')
                self.start_proc(i, 'etl', self.k)
                wr(self.self_num, f'start keeper num {i}')
                self.start_proc(i, 'keeper', self.k)
                wr(self.self_num, f'start rep num {i}')
                self.start_proc(i, 'rep', self.k)
                self.live_k.add(i)
                self.last_seen[i] = time.time()
            # self.send_to(properties.reply_to, 'System ready')
            self.broadcast({'mess': 'set_k', 'data': self.self_num, 'k': self.k})
            self.send_to(properties.reply_to, 'System ready')

        elif mess == 'load':
            """
            Распределение файлов по процессам для загрузки данных
            """
            self.check_heartbeats()
            self.last_count = len(self.live_k)
            data = mess_j['data']
            files =  glob.glob(f"{data['path']}/*.sgm")
            wr(self.self_num, '((((((((((((((((((', files)
            if not files:
                self.send_to(properties.reply_to, 'files not found')
            else:
                lst = [i in self.live_k for i in range(self.k)]
                for i in range(len(files)):
                    self.send_to(f'to_etl{i % self.k}_queue',
                                 {'mess': 'load', 'data': {'path': files[i],
                                                           'mode': data['mode'], 'k': lst}})
                # self.send_to(properties.reply_to, 'Ok')
                self.keeper_count = 0
                for i in range(self.k):
                    self.send_to(f'to_etl{i}_queue', {'mess': 'end', 'k': lst})
        elif mess == 'info':
            self.check_heartbeats()
            self.send_k_info()

        elif mess == 'kill':
            """
            Удаляем хранителя
            """
            self.send_to(f'to_keeper{mess_j['data']}_queue', {'mess': 'exit'})
            self.send_to('to_client_queue', 'ok')
            wr(self.self_num, 'ok i killed keeper! $)')


        elif mess == 'kill_m':
            """
            Удаляем менеджера
            """
            target = mess_j['data']
            if target == self.self_num:
                self.send_to('to_client_queue', f'Manager {self.self_num} killed')
                self.is_running = False
                ch.stop_consuming()
                self.is_end = True
                wr(self.self_num, 'EXIT!')
            else:
                self.send_to(f'to_manage{target}_queue',
                             {'mess': 'kill_m', 'data': target})

        elif mess == 'exit':
            """
            Остановка всех процессов
            """
            wr(self.self_num, 'Exit start')
            for i in range(self.k):
                self.send_to(f'to_keeper{i}_queue', {'mess': 'exit'})
                self.send_to(f'to_etl{i}_queue', {'mess': 'exit'})
                self.send_to(f'to_r{i}_from_man', {'mess': 'exit'})
            self.send_to('to_client_queue', 'Good bye')
            wr(self.self_num, 'Exit send')
            self.broadcast({'mess': 'exit', 'data': self.self_num})
            self.is_running = False
            ch.stop_consuming()
            self.is_end = True
            wr(self.self_num, 'EXIT')

        elif mess == 'find':
            """
            Поиск только если есть слово в словаре менеджера
            """
            # wr(self.self_num, 'FIND', mess_j['data'], self.data)
            if mess_j['data'] not in self.data:
                self.send_to(properties.reply_to, 'This word is missing, please try again')
            else:
                self.find_word = mess_j['data']
                self.check_heartbeats()
                self.send_k_find()

        elif mess == 'purge':
            """
            Очистка хранителей и менеджера
            """

            self.check_heartbeats()
            wr(self.self_num, 'purge')
            for i in range(self.k):
                self.send_to(self.get_keeper_q(i), {'mess': 'purge'})
            self.data.clear()
            wr(self.self_num, 'send end purge')
            self.send_to(properties.reply_to, 'Ok')
            wr(self.self_num, 'OK end purge')
            self.broadcast({'mess': 'purge_sync', 'data': self.self_num})
        # with open('man_log.txt', 'a') as f:
        #     f.write('client mess: ' + str(mess_j) + '\n')

        wr(self.self_num, 'client mess: ', mess_j)

    def send_k_find(self):
        """
        Поиск по слову
        Вынесла отдельно чтобы потвторно можно было послать
        :return:
        """
        self.last_count = len(self.live_k)
        self.find_k = len(self.data[self.find_word])
        self.is_work = 'find'

        wr(self.self_num, 'count', self.find_k)
        for i in self.data[self.find_word]:
            self.send_to(self.get_keeper_q(i), {'mess': 'find', 'data': self.find_word})


    def send_k_info(self):
        """
        Запрос о количестве заголовков на хранителях
        :return:
        """
        # self.check_heartbeats()
        self.last_count = len(self.live_k)
        self.is_work = 'info'
        for i in range(self.k):
            self.send_to(self.get_keeper_q(i), {'mess': 'get_info'})

    def find(self, ch, method, properties, body):
        """
        Обработка сообзений от хранителей и etl
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        # wr(self.self_num, 'keep', body.decode())
        message = json.loads(body.decode())
        mess = message['mess']
        if mess == 'find':
            self.find_res.append(message['data'])
        elif mess == 'end_load':
            wr(self.self_num, f'END LOAD! keeper {self.keeper_count}')
            self.keeper_count += 1
        elif mess == 'load':
            self.load_k += 1
        elif mess == 'words':
            data = message['data']
            for i in data['words']:
                if i.lower() not in self.data:
                    self.data[i.lower()] = set()
                self.data[i.lower()].add(data['num'])
            if self.im_lider:
                self.broadcast({'mess': 'words', 'from': self.self_num, 'data': data})
            # wr(self.self_num, message['data'], '!!!!!!!!!!!!!')
            # wr(self.self_num, 'current data:', self.data)
            # wr(self.self_num, )
        elif mess == 'end':
            self.find_k -= 1
        elif mess == 'info':
            wr(self.self_num, message['data'])
            data = message['data']
            self.info_res.append(f"{data['num']}: {data['count']} headers")
            self.info_k += 1
            if self.info_k == self.k:
                self.send_to('to_client_queue', '\n'.join(self.info_res))
                self.info_res.clear()
                self.info_k = 0
                self.is_work = None
        elif mess == 'alive':
            num = message['data']
            self.last_seen[num] = time.time()
            if num not in self.live_k:
                wr(self.self_num, f'keeper {num} alive again')
                self.live_k.add(num)

        if self.find_k == 0:
            self.find_res.sort()
            self.send_to('to_client_queue', 'Resalt:\n' + '\n'.join(self.find_res)
                         + f'\n Count: {len(self.find_res)}')
            self.find_res.clear()
            self.find_k = -1
            wr(self.self_num, "send finding titles to client")
            self.is_work = None
        if self.load_k == self.k:
            self.send_to('to_client_queue', 'Ok')
            wr(self.self_num, 'ok i load data')
            self.load_k = 0
            self.is_work = None
        if self.keeper_count == self.k:
            self.send_to('to_client_queue', 'Ok')
            wr(self.self_num, 'ok all keepers asked')
            self.keeper_count = 0
            self.is_work = None




if __name__ == '__main__':
    m = Manager(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
    with open('man_log.txt', 'a') as f:
        f.write('start \n')
    print('@@@@@')
    m.start()
    wr(int(sys.argv[2]), 'EXIT Man')
