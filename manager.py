import glob
import json
import subprocess
import sys

import pika



def wr(*mess):
    with open('man_log.txt', 'a') as f:
        print(*mess, file=f)

# def find():
#     pass
class Manager:
    def __init__(self):
        self.k = None
        self.connection = None
        self.channel = None
        self.find_res = []
        self.data = {}
        self.load_k = 0
        self.find_k = -1
        self.info_res = []
        self.info_k = 0

    def start_proc(self, num, tp, param = None):
        try:
            wr(f'open {tp}.py')
            process = subprocess.Popen(
                [sys.executable, f"{tp}.py", str(num), str(param)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        except Exception as e:
            wr("============= ERROR!!!", e)

    def start(self):
        """
        Создаем основные очереди для связи с менеджером
        to_manager_queue - от клиента
        to_manager_from_k - от остальных
        :return:
        """
        # files = glob.glob(f"{path}/*.sgm")
        # print(files)
        # radius = -1
        #
        # count = (len(files) // k) + ((len(files) % k) != 0)  # сколько файлов для каждого etl
        #
        # if mode:  # диапазонное
        #     radius = 26 // k + ((26 % k) != 0)
        wr('func start')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        wr('connection')

        self.channel.queue_declare(queue='to_manager_queue')
        # self.channel.queue_purge(queue=f'to_manager_queue')
        self.channel.basic_consume(queue='to_manager_queue', on_message_callback=self.processing, auto_ack=True) # auto_ack=False)

        self.channel.queue_declare(queue='to_manager_from_k')
        self.channel.queue_purge(queue=f'to_manager_from_k')
        self.channel.basic_consume(queue='to_manager_from_k', on_message_callback=self.find,auto_ack=True) # auto_ack=False)

        wr('start consuming')
        try:
            self.channel.start_consuming()
        except Exception as e:
            wr("Завершение... ", e)
        finally:
            self.connection.close()

    def send_to(self, queue, body, reply_to = None):
        """
        Отправляем данные в нужную очередь + добавляем куда слать ответ если надо
        :param queue: куда
        :param body: что
        :param reply_to: куда ответить или none
        :return:
        """
        wr('send to ' + queue + ' body ' + str(body))
        properties = None
        if reply_to is not None:
            properties = pika.BasicProperties(reply_to=reply_to.encode())
        if type(body) == dict:
            body = json.dumps(body)
        self.channel.basic_publish(exchange='', routing_key=queue.encode(),
                          properties=properties,
                          body=body.encode())

    def processing(self, ch, method, properties, body):
        """
        Обработчик от клиента
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        mess_j = json.loads(body.decode())
        mess = mess_j['mess']
        wr('proc with mess ' + str(mess_j))
        if mess == 'start':
            """
            Запуск хранителей и etl + очистка их очередей
            """
            self.k = mess_j['data']
            wr('start new proc')
            for i in range(self.k):
                self.channel.queue_declare(queue=f'to_etl{i}_queue')
                self.channel.queue_purge(queue=f'to_etl{i}_queue')
                self.channel.queue_declare(queue=f'to_keeper{i}_queue')
                self.channel.queue_purge(queue=f'to_keeper{i}_queue')
                wr(f'start etl num {i}')
                self.start_proc(i, 'etl', self.k)
                wr(f'start keeper num {i}')
                self.start_proc(i, 'keeper', self.k)
            self.send_to(properties.reply_to, 'System ready')

        elif mess == 'load':
            """
            Распределение файлов по процессам для загрузки данных
            """
            data = mess_j['data']
            files =  glob.glob(f"{data['path']}/*.sgm")
            wr('((((((((((((((((((', files)
            if not files:
                self.send_to(properties.reply_to, 'files not found')
            else:
                for i in range(len(files)):
                    self.send_to(f'to_etl{i % self.k}_queue',
                                 {'mess': 'load', 'data': {'path': files[i],
                                                           'mode': data['mode'], 'k': self.k}})
                self.send_to(properties.reply_to, 'Ok')

        elif mess == 'info':
            for i in range(self.k):
                self.send_to(f'to_keeper{i}_queue', {'mess': 'get_info'})

        elif mess == 'exit':
            """
            Остановка всех процессов
            """
            wr('Exit start')
            for i in range(self.k):
                self.send_to(f'to_keeper{i}_queue', {'mess': 'exit'})
                self.send_to(f'to_etl{i}_queue', {'mess': 'exit'})
            self.send_to('to_client_queue', 'Good bye')
            wr('Exit send')
            ch.stop_consuming()
            wr('EXIT')

        elif mess == 'find':
            """
            Поиск только если есть слово в словаре менеджера
            """
            wr('FIND', mess_j['data'], self.data)
            if mess_j['data'] not in self.data:
                self.send_to(properties.reply_to, 'This word is missing, please try again')
            else:
                self.find_k = len(self.data[mess_j['data']])

                wr('count', self.find_k)
                for i in self.data[mess_j['data']]:
                    self.send_to(f'to_keeper{i}_queue', {'mess': 'find', 'data': mess_j['data']})

        elif mess == 'purge':
            """
            Очистка хранителей и менеджера
            """
            wr('purge')
            for i in range(self.k):
                self.send_to(f'to_keeper{i}_queue', {'mess': 'purge'})
            self.data.clear()
            wr('send end purge')
            self.send_to(properties.reply_to, 'Ok')
            wr('end purge')
        # with open('man_log.txt', 'a') as f:
        #     f.write('client mess: ' + str(mess_j) + '\n')

        wr('client mess: ', mess_j)

    def find(self, ch, method, properties, body):
        """
        Обработка сообзений от хранителей и etl
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return:
        """
        wr('keep', body.decode())
        message = json.loads(body.decode())
        mess = message['mess']
        if mess == 'find':
            self.find_res.append(message['data'])
        elif mess == 'load':
            self.load_k += 1
        elif mess == 'words':
            data = message['data']
            for i in data['words']:
                if i.lower() not in self.data:
                    self.data[i.lower()] = set()
                self.data[i.lower()].add(data['num'])
            wr(message['data'], '!!!!!!!!!!!!!')
            wr('current data:', self.data)
            wr()
        elif mess == 'end':
            self.find_k -= 1
        elif mess == 'info':
            data = message['data']
            self.info_res.append(f"Keeper {data['num']}: {data['count']} headers")
            self.info_k += 1
            if self.info_k == self.k:
                self.send_to('to_client_queue', '\n'.join(self.info_res))
                self.info_res.clear()
                self.info_k = 0
        if self.find_k == 0:
            self.find_res.sort()
            self.send_to('to_client_queue', 'Resalt:\n' + '\n'.join(self.find_res)
                         + f'\n Count: {len(self.find_res)}')
            self.find_res.clear()
            self.find_k = -1
            wr("send finding titles to client")
        if self.load_k == self.k:
            self.send_to('to_client_queue', 'Ok')
            self.load_k = 0
        # with open('man_log.txt', 'a') as f:
        #     f.write('keeper mess: ' + str(message) + '\n')
        wr('keeper mess: ', message)

    # def load(self, path):
    #     pass


if __name__ == '__main__':
    m = Manager()
    with open('man_log.txt', 'a') as f:
        f.write('start \n')
    print('@@@@@')
    m.start()
    wr('EXIT Man')
