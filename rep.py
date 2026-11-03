import json
import sys

import pika

def wr(num, *mess):
    with open(f'rep{num}_log.txt', 'a') as f:
        print(*mess, file=f)

class Replica:
    def __init__(self, num):
        self.data = []
        self.num = num
        self.connection = None
        self.channel = None
        self.load_flag = False
        self.is_main = False
        self.is_end = 0

    def start(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=f'to_k{self.num}_from_etl')  # для ответа хранителю
        self.channel.queue_declare(queue=f'to_manager_from_k') # для поиска заголовков по слову (отправка менеджеру)
        self.channel.queue_declare(queue=f'to_r{self.num}_from_keeper') # для загрузки
        self.channel.queue_declare(queue=f'to_r{self.num}_from_man') # для поиска (запрос от менеджера)
        # self.channel.queue_purge(queue=f'to_r{self.num}_from_keeper')
        wr(self.num, 'CONNECT')
        self.channel.basic_consume(queue=f'to_r{self.num}_from_man', on_message_callback=self.resp, auto_ack=True)
        self.channel.basic_consume(queue=f'to_r{self.num}_from_keeper', on_message_callback=self.load, auto_ack=True)
        try:
            self.channel.start_consuming()
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
                    'data': f"{title},\tfile {file},\t replica {self.num}"
                })
                count += 1
        wr(self.num,'find', count, 'titles, send end mess')
        self.send_to('to_manager_from_k', {'mess': "end"})

    def load(self, ch, method, properties, body):
        message = json.loads(body.decode())
        # wr(self.num,'LOAD!', message)
        # wr(self.num,'get title num ', len(self.data) + 1, 'title', message)
        mess = message['mess']
        if mess == 'load':
            self.load_flag = True
            # self.data[message['data']['title']] = message['data']['file']
            self.data.append((message['data']['title'], message['data']['file']))
            # wr(self.num, 'load data')
            # self.count += 1

        # if mess == 'end':
        #     self.is_end += 1
        #     wr(self.num, message['k'])
        #     if self.is_end == message['k']:
        #         # for i in self.data:
        #         #     self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'load', 'data': i})
        #         self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'end'})
        #         wr(self.num, 'END LOAD num 2!')
        if mess == 'end':
            wr(self.num, 'I hane end etl')
            self.is_end += 1
            wr(self.num, message['k'])
            if self.is_end == message['k']:
                self.is_end = 0
                wr(self.num, 'Здесь!!!')
                # for i in self.data:
                #     self.send_to(f'to_r{self.num}_from_keeper', {'mess': 'load', 'data': i})
                self.send_to('to_manager_from_k', {'mess': 'end_load'})
        if mess == 'end_k':
            self.load_flag = True
            wr(self.num, 'i write end_load')
            self.send_to(f'to_k{self.num}_from_r', {'mess': 'end_load'})



    def resp(self, ch, method, properties, body):
        wr(self.num,"RESP", body.decode())
        message = json.loads(body.decode())
        mess = message['mess']
        if mess == 'purge':
            self.data.clear()
            wr(self.num, 'all purge')
        elif mess == 'exit':
            self.is_main = True
            ch.stop_consuming()
            wr(self.num, 'exit command')
        elif mess == 'find':
            self.is_main = True
            self.find(message['data'])
            wr(self.num, 'end find')
        elif mess == 'get_info':
            self.is_main = True
            wr(self.num, 'INFO: ', len(self.data))
            self.send_to('to_manager_from_k', {'mess': 'info',
                                              'data': {'count': len(self.data), 'num': f"Replica {self.num}"}})
        elif mess == 'load' or mess == 'end':
            self.is_main = True
            wr(self.num, 'from etl', message)
            self.load(ch, method, properties, body)
        else:
            wr(self.num,'Хз что тут', message)

if __name__ == '__main__':
    # wr('', int(sys.argv[1]))
    wr(int(sys.argv[1]),'Start!! rep', int(sys.argv[1]))
    m = Replica(int(sys.argv[1]))
    wr(int(sys.argv[1]), 'INIT')
    m.start()
    wr(int(sys.argv[1]), f'EXIT, end of work repl num {m.num}')
