#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pika
import random
import json
from conf import setting


class RpcClient(object):
    def __init__(self):
        self.task = []#执行命令后保存任务id,查询任务后删除任务id
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=setting.RABBITMQ_ADDR))
        self.channel = self.connection.channel()
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue
        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        #如果要查询的任务id和收到的id相同则把收到的内容赋值给self.response
        if self.corr_id == props.correlation_id:
            self.response = body

    def run(self, n):
        #将要执行的命令发送给队列,并生成任务id
        self.response = None
        while True:
            #生成随机的不重复的任务id
            self.corr_id = str(random.randint(1,99999))
            if self.corr_id not in self.task:
                break
        self.task.append(self.corr_id)
        print('task id: %s'%self.corr_id)
        self.channel.basic_publish(exchange='',
                                   routing_key='rpc_queue',
                                   properties=pika.BasicProperties(
                                       reply_to=self.callback_queue,
                                       correlation_id=self.corr_id,
                                   ),
                                   body=json.dumps(n))

    def check_task(self,cmd):
        #查询任务执行结果
        if cmd[1] not in self.task:
            print('任务不存在')
            return
        self.corr_id = cmd[1]
        while self.response is None:
            self.connection.process_data_events()
        for i in json.loads(self.response.decode()):
            print(i)
        self.task.remove(cmd[1])


def main():
    rpcclient = RpcClient()
    while True:
        cmd = input('-->').strip().split()
        if len(cmd) < 2:
            print('输入错误(run "df -h" --hosts HOST_ID or check_task TASK_ID)')
            continue
        if hasattr(rpcclient,cmd[0]):
            getattr(rpcclient,cmd[0])(cmd)