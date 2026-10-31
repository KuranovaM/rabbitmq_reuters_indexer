import json
import sys

import pika

def wr(num, *mess):
    with open(f'keeper{num}_log.txt', 'a') as f:
        print(*mess, file=f)

class Keeper:
    def __init__(self, num):
        self.data = []
        self.num = num
        self.connection = None
        self.channel = None
        self.is_rep_ready = False
        self.is_end = 0
        self.is_running = True

    def start(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=f'to_keeper{self.num}_queue')  # , durable=True)
        self.channel.queue_declare(queue=f'to_manager_from_k') # для поиска заголовков по слову
        self.channel.queue_declare(queue=f'to_k{self.num}_from_etl') # для загрузки
        self.channel.queue_declare(queue=f'to_r{self.num}_from_keeper') # реплика
        wr(self.num, 'CONNECT')
        self.channel.basic_consume(queue=f'to_keeper{self.num}_queue', on_message_callback=self.resp, auto_ack=True)
        self.channel.basic_consume(queue=f'to_k{self.num}_from_etl', on_message_callback=self.load, auto_ack=True)
        try:
            while self.is_running:
                self.connection.process_data_events(time_limit=1)  # 1 сек — период heartbeat

                if self.is_running:
                    try:
                        self.send_to('to_manager_from_k', {'mess': 'alive', 'data': self.num})
                        # wr(self.num, 'HEARTBEAT!!!')
                    except Exception as e:
                        wr(self.num, 'heartbeat failed', e)
                        break
        except Exception as e:
            wr(self.num, "Завершение... ", e)
        finally:
            self.connection.close()

    def send_to(self, queue, body, reply_to = None):
        properties = None
        if reply_to is not None:
            properties = pika.BasicProperties(reply_to=reply_to.encode())
        self.channel.basic_publish(exchange='', routing_key=queue.encode(),
                          properties=properties,
                          body=json.dumps(body).encode())

    def find(self, word):
        count = 0
        word = word.lower()
        wr(self.num, '---------------find word', word)
        for title, file in self.data:
            if word in title.lower().split():
                self.send_to('to_manager_from_k', {
                    'mess': 'find',
                    'data': f"{title},\tfile {file},\t keeper {self.num}"
                })
                count += 1
        wr(self.num,'find', count, 'titles, send end mess')
        self.send_to('to_manager_from_k', {'mess': "end"})

    def load(self, ch, method, properties, body):
        # wr(self.num,'LOAD!')
        message = json.loads(body.decode())
        # wr(self.num,'get title num ', len(self.data) + 1, 'title', message)
        mess = message['mess']
        if mess != 'load':
            wr(self.num, 'method load', mess)
        if mess == 'load':
            self.is_rep_ready = False
            # self.data[message['data']['title']] = message['data']['file']
            self.data.append((message['data']['title'], message['data']['file']))
            self.send_to(f'to_r{self.num}_from_keeper',
                         {'mess': 'load', 'data': {'title': message['data']['title'],
                                                         'file': message['data']['file']}})
            # self.count += 1
        if mess == 'end':
            self.is_end += 1
            wr(self.num, message['k'])
            if self.is_end == message['k']:
                # for i in self.data:
                #     self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'load', 'data': i})
                self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'end'})
                wr(self.num, 'END LOAD num 2!')
        if mess == 'end_load':
            wr(self.num, 'END REPL LOAD!')
            self.is_rep_ready = True
            self.send_to('to_manager_from_k', {'mess': 'end_load'})



    def resp(self, ch, method, properties, body):
        wr(self.num,"RESP", body.decode())
        message = json.loads(body.decode())
        mess = message['mess']
        if mess == 'purge':
            self.data.clear()
            wr(self.num, 'all purge')
            self.send_to(f'to_r{self.num}_from_man', message)
        elif mess == 'exit':
            # self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'exit'})
            ch.stop_consuming()
            self.is_running = False
            wr(self.num, 'exit command')

        elif mess == 'find':
            self.find(message['data'])
            wr(self.num, 'end find')
        elif mess == 'get_info':
            self.send_to('to_manager_from_k', {'mess': 'info',
                                              'data': {'count': len(self.data), 'num': f"Keeper {self.num}"}})
        else:
            wr(self.num,'Хз что тут', message)

if __name__ == '__main__':
    # wr('', int(sys.argv[1]))
    wr(int(sys.argv[1]),'Start!! keeper', int(sys.argv[1]))
    m = Keeper(int(sys.argv[1]))
    wr(int(sys.argv[1]), 'INIT')
    m.start()
    wr(int(sys.argv[1]), f'EXIT, end of work keeper num {m.num}')
