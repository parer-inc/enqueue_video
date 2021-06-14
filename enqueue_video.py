"""This service allows to enqueu videos to parse"""
import os
import sys
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job

r = get_redis()


def enqueue_video(video, chan_id):
    """Enqueues video to parse"""
    result = False
    for i in range(4):
        q = Queue('get_videos', connection=r)
        job = q.enqueue('get_videos.get_videos', "WHERE", 'id', video)
        await_job(job)
        result = job.result
        if result is not False:
            break
        time.sleep(5)
    vid_type = "upd" if result != () else "new"
    if result is not False:
        q = Queue('parse_video', connection=r)
        if vid_type == "new":
            job = q.enqueue('parse_video.parse_video', video, chan_id, coms=True) # with comments
            await_job(job, 18000)
            res = job.result
            print(res)
            if res:
                q = Queue('write_videos', connection=r)
                job = q.enqueue('write_videos.write_videos', job.result)
            else:
                return False
        else:
            job = q.enqueue('parse_video.parse_video', video, chan_id) # no comments
            await_job(job, 18000)
            res = job.result
            print(res)
            if res:
                q = Queue('update_videos', connection=r)
                job = q.enqueue('update_videos.update_videos', job.result)
            else:
                return False
    else:
        return False
    return True


if __name__ == '__main__':
    q = Queue('enqueue_video', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='enqueue_video')
        worker.work()
