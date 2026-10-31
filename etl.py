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
    # wr('____________- Words data', data)
    send_to(ch, 'to_manager_from_k', data)

def get_keeper_q(lst, num):
    if lst[num]:
        return f'to_k{num}_from_etl'
    return f'to_r{num}_from_keeper'

def load(ch, method, properties, body):
    message = json.loads(body.decode())
    if message['mess'] == 'end':
        for i in range(len(message['k'])):
            send_to(ch, get_keeper_q(message['k'], i),
                    {'mess': 'end', 'k': len(message['k'])})
        wr('END LOAD send to ', [get_keeper_q(message['k'], i) for i in range(len(message['k']))])
        return
    if message['mess'] == 'exit':
        ch.stop_consuming()
        return
    if message['mess'] != 'load':
        wr(f'ETL Error, unknown message from {method.routing_key}:', message)
        return
    data = message['data']
    flag = False
    # res = []
    # local_res = []
    title = ''
    mode = data['mode'] == 'h'
    k = len(data['k'])
    count = 26 // k + ((26 % k) != 0)
    send_flag = False
    wr('open file ', data['path'])
    try:
        with open(data['path'], 'r', errors='replace') as f:
            for i in f:
                if '<TITLE>' in i:
                    flag = True
                    title = i[i.index('<TITLE>') + 7:]
                    # wr('Im here 1')
                    if '</TITLE>' in title:
                        title = title[:title.index('</TITLE>')]
                        send_flag = True
                        flag = False
                elif '</TITLE>' in i and flag:
                    title += i[:i.index('</TITLE>')]
                    send_flag = True
                    flag = False
                elif flag:
                    title += i
                if send_flag and title != '':
                    title = re.sub(r'<[^>]+>', '', title)
                    title = title.replace('&lt;', '<').replace('&gt;', '>')
                    title = title.replace('&amp;', '&').replace('&quot;', '"')
                    title = title.replace('<', '').replace('>', '')
                    title = ' '.join(title.split()).strip()
                    # wr('title:', title)
                    if title != '':
                        if mode:
                            num = hash(title) % k
                        else:
                            first_char = title[0].lower()
                            if 'a' <= first_char <= 'z':
                                try:
                                    num = (ord(first_char) - ord('a')) // count
                                    if num >= k:
                                        num = k - 1
                                except Exception as e:
                                    num = 0
                                    wr('ERROR~~~', e)
                            else:
                                num = 0
                        send_to(ch, get_keeper_q(data['k'], num),
                                {'mess': 'load', 'data': {'title': title, 'file': data['path']}})
                        split_words(ch, title, num)
                        title = ''
        wr('END of file', data['path'])
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
