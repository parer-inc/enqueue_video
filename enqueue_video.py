"""This service allows to write new channels to db"""
import os
import sys
import time
from rq import Worker, Queue, Connection
from methods.connection import get_redis, await_job


def enqueue_video(video):
    """Enqueues video to parse"""
    result = False
    for i in range(4):
        q = Queue('get_videos', connection=r)
        job = q.enqueue('get_videos.get_videos', video)
        await_job(job)
        result = job.result
        if result:
            break
        time.sleep(5)
    vid_type = "upd" if result != () else "new"
    await_job(job, 1800)
    result = job.result
    if result:
        q = Queue('parse_video', connection=r)
        if vid_type == "new":
            job = q.enqueue('parse_video.parse_video', video) # with comments
            q = Queue('write_videos', connection=r)
            job = q.enqueue('write_videos.write_videos', result)
        else:
            job = q.enqueue('parse_video.parse_video', video) # no comments
            q = Queue('update_videos', connection=r)
            job = q.enqueue('update_videos.update_videos', result)
    else:
        return False


if __name__ == '__main__':
    time.sleep(5)
    r = get_redis()
    q = Queue('enqueue_video', connection=r)
    with Connection(r):
        worker = Worker([q], connection=r,  name='enqueue_video')
        worker.work()
