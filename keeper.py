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

    def start(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=f'to_keeper{self.num}_queue')  # , durable=True)
        self.channel.queue_declare(queue=f'to_manager_from_k') # для поиска заголовков по слову
        self.channel.queue_declare(queue=f'to_k{self.num}_from_etl') # для загрузки
        wr(self.num, 'CONNECT')
        self.channel.basic_consume(queue=f'to_keeper{self.num}_queue', on_message_callback=self.resp, auto_ack=True)
        self.channel.basic_consume(queue=f'to_k{self.num}_from_etl', on_message_callback=self.load, auto_ack=True)
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
            if word in title.lower():
                self.send_to('to_manager_from_k', {
                    'mess': 'find',
                    'data': f"{title},\tfile {file},\t keeper {self.num}"
                })
                count += 1
        wr(self.num,'find', count, 'titles, send end mess')
        self.send_to('to_manager_from_k', {'mess': "end"})

    def load(self, ch, method, properties, body):
        wr(self.num,'LOAD!')
        message = json.loads(body.decode())
        wr(self.num,'get title num ', len(self.data) + 1, 'title', message)
        mess = message['mess']
        if mess == 'load':
            # self.data[message['data']['title']] = message['data']['file']
            self.data.append((message['data']['title'], message['data']['file']))
            # self.count += 1



    def resp(self, ch, method, properties, body):
        wr(self.num,"RESP", body.decode())
        message = json.loads(body.decode())
        mess = message['mess']
        if mess == 'purge':
            self.data.clear()
            wr(self.num, 'all purge')
        elif mess == 'exit':
            ch.stop_consuming()
            wr(self.num, 'exit command')
        elif mess == 'find':
            self.find(message['data'])
            wr(self.num, 'end find')
        elif mess == 'get_info':
            self.send_to('to_manager_from_k', {'mess': 'info',
                                              'data': {'count': len(self.data), 'num': self.num}})
        else:
            wr(self.num,'Хз что тут', message)

if __name__ == '__main__':
    # wr('', int(sys.argv[1]))
    wr(int(sys.argv[1]),'Start!! keeper', int(sys.argv[1]))
    m = Keeper(int(sys.argv[1]))
    wr(int(sys.argv[1]), 'INIT')
    m.start()
    wr(int(sys.argv[1]), f'EXIT, end of work keeper num {m.num}')
