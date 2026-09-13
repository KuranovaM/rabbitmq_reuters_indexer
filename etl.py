import json
import re
import sys
from curses.ascii import isalpha

import pika


def wr(*mess):
    with open('etl_log.txt', 'a') as f:
        print(*mess, file=f)
        # f.write(' '.join(map(str, mess)) + '\n')
        # for i in mess:
        #     f.write(i + ' ')

def send_to(channel, queue, body, reply_to = None):
    properties = None
    if reply_to is not None:
        properties = pika.BasicProperties(reply_to=reply_to.encode())
    channel.basic_publish(exchange='', routing_key=queue.encode(),
                      properties=properties,
                      body=json.dumps(body).encode())

def split_words(ch, string, num):
    data = {'mess': 'words', 'data': {'words': string.split(), 'num': num}}
    wr('____________- Words data', data)
    send_to(ch, 'to_manager_from_k', data)

def load(ch, method, properties, body):
    message = json.loads(body.decode())
    if message['mess'] == 'exit':
        ch.stop_consuming()
        return
    if message['mess'] != 'load':
        wr(f'ETL Error, unknown message from {method.routing_key}:', message)
    data = message['data']
    flag = False
    # res = []
    # local_res = []
    title = ''
    mode = data['mode'] == 'h'
    count = 26 // data['k'] + ((26 % data['k']) != 0)
    send_flag = False
    wr('open file ', data['path'])
    try:
        with open(data['path'], 'r') as f:
            for i in f:
                if '<TITLE>' in i:
                    flag = True
                    title = i[i.index('<TITLE>') + 7:]
                    if '</TITLE>' in title:
                        title = title[:title.index('</TITLE>')]
                        # res.append(title) # тут наверное можно сразу отправлять
                        wr((ord(title[0].lower()) - ord('a')), (ord(title[0].lower()) - ord('a')) // count, count, title)
                        send_flag = True
                        flag = False
                elif '</TITLE>' in i and flag:
                    title += i[:i.index('</TITLE>')]

                    # res.append(title[:title.index('</TITLE>')]) # и тут
                    wr((ord(title[0].lower()) - ord('a')) // count, count, title, 2)
                    send_flag = True
                    flag = False

                    # if mode:
                    #     send_to(ch, f'to_keeper{hash(title) % data["k"]}_queue',
                    #             {'mess': 'load', 'data': title})
                    # else:
                    #     send_to(ch, f'to_keeper{(ord(title[0].lower()) - ord("a")) * count}_queue',
                    #             {'mess': 'load', 'data': title})
                    # title = ''
                    # flag = False
                elif flag:
                    title += i
                if send_flag and title != '':
                    title = re.sub(r'<[^>]+>', '', title)
                    title = title.replace('&lt;', '<').replace('&gt;', '>')
                    title = title.replace('&amp;', '&').replace('&quot;', '"')
                    title = title.replace('<', '').replace('>', '')
                    title = ' '.join(title.split()).strip()
                    wr('title:', title)
                    if title != '':
                        num = 0
                        if mode:
                            num = hash(title) % data['k']
                        else:
                            first_char = title[0].lower()
                            if 'a' <= first_char <= 'z':
                                num = (ord(first_char) - ord('a')) // count
                                if num >= data['k']:
                                    num = data['k'] - 1
                            else:
                                num = 0
                        send_to(ch, f'to_k{num}_from_etl',
                                {'mess': 'load', 'data': {'title': title, 'file': data['path']}})
                        split_words(ch, title, num)
                        title = ''
        wr('END of file', data['path'])
        # for i in range(data['k'])
        # send_to(ch, 'to_manager_from_k', {'mess': 'load'})
        # wr('send mess end to manager')

    except FileNotFoundError:
        wr('File not found', data['path'])

def start(num, k):

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    for i in range(k):
        channel.queue_declare(queue=f'to_k{i}_from_etl')
        # channel.queue_purge(queue=f'to_k{i}_from_etl')
    channel.queue_purge(queue=f'to_etl{num}_queue')
    channel.queue_declare(queue=f'to_manager_from_k')
    # channel.queue_purge(queue=f'to_manager_from_k')
    channel.queue_declare(queue=f'to_etl{num}_queue')
    channel.basic_consume(queue=f'to_etl{num}_queue', on_message_callback=load, auto_ack=True)

    try:
        channel.start_consuming()
    except Exception as e:
        wr(num,"Завершение... ", e)
    finally:
        connection.close()
    wr('END!!!!')
    connection.close()

if __name__ == '__main__':
    wr('Start!!')
    wr(' '.join(sys.argv))
    start(int(sys.argv[1]), int(sys.argv[2]))

    wr(f'End of work etl num {int(sys.argv[1])}')
