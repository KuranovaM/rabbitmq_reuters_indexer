import subprocess
import sys

import pika
import json


def send_to(channel, queue, body, reply_to = None):
    properties = None
    if reply_to is not None:
        properties = pika.BasicProperties(reply_to=reply_to.encode())
    channel.basic_publish(exchange='', routing_key=queue.encode(),
                      properties=properties,
                      body=json.dumps(body).encode())

def resp_one(ch, method, properties, body):
    # print('print')
    ch.stop_consuming()
    body = body.decode()
    # reply_queue = properties.reply_to
    print(body)


def wait_answer(channel):
    for method, properties, body in channel.consume('to_client_queue'):
        print(body.decode())
        channel.cancel()
        # return body.decode()

def main():
    end_flag = True
    # start_flag = False
    k = 0
    connection = None
    channel = None
    # callback_queue = None
    # result = None

    while end_flag:
        try:
            string = input().strip().lower()
        except EOFError:
            break
        if string in ["end", "exit"]:
            end_flag = False
            break

        if 'start' in string:
            string = string.split()
            if len(string) < 2:
                print("Parameter required: the number of ETL processes")
            elif len(string) > 2:
                print("too many parameters")
            else:
                if not string[1].isdigit():
                    print('Unknown command has been entered, try again')
                else:
                    k = int(string[1])
                    # start_flag = True
                    break
        elif 'load' in string or 'purge' in string or 'find' in string:
            print("The manager hasn't been started yet. Please, enter 'start'")
        else:
            print('Unknown command has been entered, try again')

    # запуск менеджера, очереди, бла-бла
    if end_flag:
        # надо запустить файл manager как-то...
        # print('+++++')
        process = subprocess.Popen(
            [sys.executable, "manager.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        # print('!!!!!!!!!!')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='to_manager_queue') #, durable=True)
        channel.queue_purge(queue='to_manager_queue')

        channel.queue_declare(queue='to_client_queue')
        channel.queue_purge(queue='to_client_queue')
        # print('======')

        send_to(channel, 'to_manager_queue', {"mess": 'start', "data": k}, 'to_client_queue')
        # print('-----')
        # channel.basic_publish(exchange='', routing_key='to_manager_queue',
        #                       properties=pika.BasicProperties(reply_to='to_client_queue'),
        #                       body='start')
        channel.basic_consume(queue='to_client_queue', on_message_callback=resp_one, auto_ack=True)
        # print('((((')
        # channel.start_consuming()
        # print('System ready')
        wait_answer(channel)

    while end_flag:
        try:
            string = input().strip()
        except EOFError:
            break
        if string.lower() in ["end", "exit"]:
            end_flag = False
            break
        if string == '':
            # print('tyt')
            continue
        spl_string = string.split()
        if spl_string[0].lower() == 'start':
            print('System has been started yet')
        elif spl_string[0].lower() == 'purge' and len(spl_string) == 1:
            send_to(channel, 'to_manager_queue', {'mess': 'purge'}, reply_to='to_client_queue')
            # channel.start_consuming()
            wait_answer(channel)
            # pass # тут что-то про очистку узлов
        elif spl_string[0].lower() == 'load' and len(spl_string) == 3 \
                and spl_string[-1] in ['d', 'h']: # and all([not i.isdigit() for i in spl_string]) \
            send_to(channel, 'to_manager_queue', {'mess': 'load',
                                                  'data': {'path': spl_string[1],
                                                           'mode': spl_string[-1]}}, 'to_client_queue')
            # channel.start_consuming()
            wait_answer(channel)
            # pass # тут что-то про загрузку узлов
        elif spl_string[0].lower() == 'find' and len(spl_string) == 2:
            send_to(channel, 'to_manager_queue', {'mess': 'find', 'data': spl_string[1]},
                    'to_client_queue')
            # channel.start_consuming()
            # pass # тут
            wait_answer(channel)
        elif spl_string[0].lower() == 'info' and len(spl_string) == 1:
            send_to(channel, 'to_manager_queue', {'mess': 'info'}, 'to_client_queue')
            wait_answer(channel)
        else:
            print('Unknown command has been entered, try again')

    send_to(channel, 'to_manager_queue', {"mess": 'exit'}, 'to_client_queue')
    # print("Good bye")
    wait_answer(channel)
    # конец
    connection.close()

main()