#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pika
import paramiko
import json
import threading
from conf import setting


class RpcServer(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=setting.RABBITMQ_ADDR))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='rpc_queue')

    def on_request(self,ch, method, props, body):
        #将命令执行结果发送给队列
        self.handle(body)
        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=pika.BasicProperties(correlation_id=props.correlation_id),
                         body=json.dumps(self.result))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def cmd_handle(self, ip, port, username, password, cmd):
        #登录远程主机执行操作并将结果返回
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=ip, port=int(port), username=username, password=password)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        out = stdout.read().decode()
        err = stderr.read().decode()
        if out:
            self.result.append('------------------------------'+ip+'------------------------------\n'+out)
        else:
            self.result.append('------------------------------'+ip+'------------------------------\n'+err)
        ssh.close()

    def cmd(self,ip,command):
        #实现多线程执行命令
        self.result = []
        list_t = []
        for i in ip:
            t = threading.Thread(target=self.cmd_handle, args=(i,*setting.HOST_INFO[i].split(), command))
            t.start()
            list_t.append(t)
        for i in list_t:
            i.join()

    def handle(self,cmd):
        #将命令转换成更容易操作的格式并调用执行命令的函数
        index = []
        cmd = json.loads(cmd.decode())
        if len(cmd) < 4:
            self.result = ['输入错误(run "df -h" --hosts HOST_ID)']
            return
        del cmd[0]
        for i in cmd:
            if '"' in i:
                index.append(cmd.index(i))
        if len(index) == 1:
            cmd_run = cmd[index[0]]
            cmd.remove(cmd_run)
            cmd_run = cmd_run.strip('"')
        elif len(index) == 2:
            cmd_run = cmd[index[0]:index[1]+1]
            for i in cmd_run:
                cmd.remove(i)
            cmd_run = ' '.join(cmd_run).strip('"')
        else:
            self.result = ['输入错误(run "df -h" --hosts HOST_ID)']
            return
        if cmd[0] == '--hosts':
            cmd.remove('--hosts')
        else:
            self.result = ['输入错误(run "df -h" --hosts HOST_ID)']
            return
        self.cmd(cmd,cmd_run)


def main():
    rpcserver = RpcServer()
    rpcserver.channel.basic_qos(prefetch_count=1)
    rpcserver.channel.basic_consume(rpcserver.on_request, queue='rpc_queue')
    rpcserver.channel.start_consuming()
