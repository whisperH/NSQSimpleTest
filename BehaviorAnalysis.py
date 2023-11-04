import nsq
from tornado import gen
from time import sleep
from functools import partial
import json



def Tracking_Fish(video_name, algorithm):

    print(f"="*50)
    print(f"starting detecting fish from {video_name}")

    OneStage = algorithm['OneStage']
    if OneStage:
        det_track = algorithm['tracking']
        if det_track == 'CenterTrack':
            print(f"loading CenterTrack model...")
            sleep(1)
    else:
        detecting = algorithm['detecting']
        tracking = algorithm['tracking']
        if detecting == 'YOLO5':
            print(f"loading YOLOV5 model...")
            sleep(1)
        else:
            return None
        print(f"="*50)

        print(f"starting tracking fish by Sort")
        if tracking == 'Sort':
            print(f"loading Sort model...")
            sleep(1)
        else:
            return None
        print(f"="*50)

    print(f"starting connecting tracklet")
    sleep(1)
    track_file = f'path to {video_name}.csv'
    print(f"finish generating tracking file")
    return track_file

def Generate_Video(video_name, track_file):
    print(f"starting generating {video_name} from {track_file}")
    sleep(1)
    video_file = f'path to tracking results.mp4'
    print(f"finish generating tracking video")
    return video_file

def Generate_BehaviorIndex(video_name, track_file):
    print(f"starting generating behavior information from {video_name}")
    sleep(1)
    behavior_file = f'path to {video_name}.csv'
    print(f"finish generating behavior information")
    return behavior_file

def write_message(topic, data, writer):
    response = writer.pub(topic, data.encode('utf8'))
    if isinstance(response, nsq.Error):
        print ("Error with Message: {}:{}".format(data, response))
    else:
        print ("Published Message: ", data)

def BehaviorAnalysis(message, writer):
    message.enable_async()
    data = json.loads(message.body)
    video_name = data["video_name"]
    results = {}

    track_file = Tracking_Fish(video_name, data["algorithm"])
    if data["export_tracking_index"]:
        results['tracking_index'] = track_file
    if data["export_behavior_index"]:
        behavior_file = Generate_BehaviorIndex(video_name, track_file)
        results['behavior_index'] = behavior_file
    if data["export_tracking_video"]:
        video_file = Generate_Video(video_name, track_file)
        results['video_file'] = video_file

    topic = "results"

    output_message = json.dumps(results)
    write_message(topic, output_message, writer)
    message.finish()

if __name__ == "__main__":
    writer = nsq.Writer(['127.0.0.1:4150'])
    handler = partial(BehaviorAnalysis, writer=writer)
    YOLO5_SORT_reader  = nsq.Reader(
        message_handler = handler,
        nsqd_tcp_addresses = ['127.0.0.1:4150'],
        topic = 'Upload_Vid_Channel',
        channel = 'GroupA')
    nsq.run()
