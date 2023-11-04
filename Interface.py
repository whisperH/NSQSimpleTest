import sys
import os
import nsq
import json
import shutil
import asyncio
from functools import partial
import tornado
from PyQt5.QtCore import QThread, pyqtSignal, QMutex
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QVBoxLayout, QHBoxLayout, QPushButton, QFileDialog
from multiprocessing import Process
import requests




class Track2D(QWidget):
    def __init__(self):
        super().__init__()
        self.video_name_without_extension = None
        self.initUI()
        self.writer = nsq.Writer(['127.0.0.1:4150'])




    def initUI(self):
        self.setWindowTitle('2DTrack')

        # 创建鱼类运动视频播放窗口
        video_window = QLabel('鱼类运动视频播放窗口')
        video_window.setStyleSheet('background-color: lightyellow;')
        video_window.setMinimumSize(800, 600)

        # 创建鱼类运动指标展示窗口
        metrics_window = QLabel('鱼类运动指标展示窗口')
        metrics_window.setStyleSheet('background-color: lightblue;')
        metrics_window.setMinimumSize(500, 400)

        # 创建上传视频并分析按钮
        upload_button = QPushButton('上传视频并分析')
        upload_button.clicked.connect(self.upload_video)

        # 创建数据指标文件导出按钮
        export_button = QPushButton('数据指标文件导出')
        export_button.clicked.connect(self.export_metrics)

        # 创建视频保存按钮
        save_button = QPushButton('视频保存')
        save_button.clicked.connect(self.save_video)

        # 创建按钮布局
        button_layout = QHBoxLayout()
        button_layout.addWidget(export_button)
        button_layout.addWidget(save_button)

        # 创建主布局
        main_layout = QHBoxLayout()
        main_layout.addWidget(video_window, 1)
        main_layout.addWidget(metrics_window, 2)

        # 创建整体布局
        layout = QVBoxLayout()
        layout.addLayout(main_layout)
        layout.addWidget(upload_button)
        layout.addLayout(button_layout)

        self.setLayout(layout)

    def send_message(self, topic, transfer_data):
        # 当writer连接nsqd成功后, 再推送数据
        if not self.writer.conns:
            return self.io_loop.add_callback(
                partial(self.send_message, *(topic, transfer_data))
            )

        # 推送数据
        self.writer.pub(topic, transfer_data, self.finish_pub)

    def recv_message(self, message):
        # message.enable_async()
        recv_data = json.loads(message.body)
        return recv_data

    def upload_video(self):
        # 上传视频并进行分析的功能实现
        button_name = self.sender().text()
        print("点击的按钮名称：", button_name)
        # file_dialog = QFileDialog()
        # video_path, _ = file_dialog.getOpenFileName(self, '上传视频', '', 'Video Files (*.mp4 *.avi)')
        # video_name = os.path.basename(video_path)
        # print("上传的视频名称：", video_name)
        # todo 这里要把上传视频和分析视频分开，分开后取消注释 return video_name
        # return video_name
        # todo 然后注释掉后面的内容
        data = {
            'video_name': "video_name",
            'export_tracking_index': True,
            'export_behavior_index': True,
            'export_tracking_video': True,
            'algorithm': {
                'OneStage': False,
                'detecting': 'YOLO5',
                'tracking': 'Sort',
            }
        }
        transfer_data = json.dumps(data).encode('utf8')
        # http://www.noobyard.com/article/p-ajlbccog-kq.html
        # 将producer函数加入到io_loop的callbacks列表中
        # 实例化IOLoop
        self.io_loop = tornado.ioloop.IOLoop.instance()
        self.io_loop.add_callback(
            partial(self.send_message, *('Upload_Vid_Channel', transfer_data))
        )
        # 启动IOLoop, 它会在内部执行callbacks列表中的函数
        self.io_loop.start()

        """
        Solver 1 (Error): https://stackoverflow.com/questions/46710299/why-does-pyqt-crashes-without-information-exit-code-0xc0000409
        # PyQT线程安全的，在这里启动多线程，发现连接没有开
        self._mutex.lock()
        self.writer.pub(
            'Upload_Vid_Channel',
            transfer_data,
            self.finish_pub
        )
        self._mutex.unlock()
        self.writer.start()
        ========================================================
        # Sovler 2:
        # https://github.com/nsqio/pynsq/issues/128
        response2 = requests.get(url=self.write_addr, params=transfer_data)
        print("send finish")
        self.finish_pub(response2)
        
        """


    def finish_pub(self, con, data):
        print(data)
        # 退出程序
        self.io_loop.stop()
        try:
            assert data.decode('utf8') == "OK", "发送请求失败，请检查nsq服务后重试"
            print("发送请求成功")
            # 这里调用对应的函数
        except Exception  as e:
            print(e)

    def analysis_video(self):
        # todo 这里要把上传视频和分析视频分开，分开后通过组件时间获取各个按钮的状态,统一在这里传送给后端
        video_name = self.upload_video()
        data = {
            'video_name': video_name,
            'export_tracking_index': True,
            'export_behavior_index': True,
            'export_tracking_video': True,
            'algorithm': {
                'OneStage': False,
                'detecting': 'YOLO5',
                'tracking': 'Sort',
            },
        }



    def export_metrics(self):
        button_name = self.sender().text()
        print("点击的按钮名称：", button_name)


    def save_video(self):
        # 视频保存的功能实现
        button_name = self.sender().text()
        print("点击的按钮名称：", button_name)
        # 同upload函数，发送一个请求，等待返回
        # handler = partial(self.recv_message)
        # nsq.Reader(
        #     message_handler = handler,
        #     nsqd_tcp_addresses = ['127.0.0.1:4150'],
        #     topic = 'results',
        #     channel = 'GroupA')


        # 在此处添加视频保存的代码逻辑


def nsq_start():
    pid = os.getpid()  # 获取当前进程的进程ID
    print(f'nsq: {pid}开始执行...')

    nsq.run()
    print(f'{pid}执行完成')

def QApp_start():
    pid = os.getpid()  # 获取当前进程的进程ID
    print(f'PyQt APP {pid}开始执行...')
    app = QApplication(sys.argv)
    fish = Track2D()
    fish.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    q_app = Process(target=QApp_start)
    q_app.start()
    # nsq_app = Process(target=nsq_start)
    # nsq_app.start()
    processes = [q_app]
    for p in processes:
        p.join()

    # https://www.jianshu.com/p/c8197304cd6b
    # https://stackoverflow.com/questions/64522179/after-nsq-run-my-python-script-is-not-executing-block-of-code-in-pynsq-packa
    # nsq.run() function starts an event loop in the background.
    nsq.run()

