import argparse
import os, time, threading, subprocess, signal, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')
import util.logging as logging
import publish.cryptowatch.request_count

request_count = 0
def update_publish_request_count():
    global request_count
    read_cnt = 0
    while True:
        read_cnt += 1
        request_count = publish.cryptowatch.request_count.get_request_count(minutes=5)
        if read_cnt % 10 == 0:
            logging.info('publish_request_count: {request_count}'.format(request_count=request_count))
            print('publish_request_count: {request_count}'.format(request_count=request_count))
        time.sleep(6)


_restart_triggers = {'abnormal closure'}

def monitor_restart_trigger(p, append_if_p_killed):
    cnt_empty_lines = 0
    while True:
        if append_if_p_killed:
            logging.warn('(monitor_restart_trigger) the process is to be restarted. breaking out of the restart monitoring loop')
            break
        line = p.stdout.readline().decode().strip()
        if len(line) > 0:
            print('(th1 line): {line}'.format(line=line))
        if len(line) == 0:
            cnt_empty_lines += 1
        else:
            cnt_empty_lines = 0

        if cnt_empty_lines > 100:
            print('(monitor_restart_trigger) cnt_empty_lines: {cnt_empty_lines}, sleeping 10 seconds'.format(cnt_empty_lines=cnt_empty_lines))
            time.sleep(10)
        elif cnt_empty_lines > 0:
            print('(monitor_restart_trigger) cnt_empty_lines: {cnt_empty_lines}, sleeping 0.1 seconds'.format(cnt_empty_lines=cnt_empty_lines))
            time.sleep(0.1)

        is_restart_trigger_found = False
        found_triggers = []
        for trigger in _restart_triggers:
            if trigger in line:
                is_restart_trigger_found = True
                found_triggers.append(trigger)

        if is_restart_trigger_found:
            try:
                logging.warn('(monitor_restart_trigger) restarting as trigger: {trigger} was observed'.format(trigger=', '.join(found_triggers)))
                append_if_p_killed.append("dummy")
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)  # Send
            except Exception as ex:
                print(ex)
            time.sleep(10)
            break

def monitor_publish_request_count(p, t_pstart, append_if_p_killed):
    while True:
        if append_if_p_killed:
            logging.warn('(monitor_publish_request_count) the process is to be restarted. breaking out of the publish request monitoring loop')
            break

        logging.info('(monitor_publish_request_count) request_count: {c}'.format(c=request_count))
        t = datetime.datetime.now()
        dt = t - t_pstart
        if dt.seconds < 60 * 10 and request_count == 0:
            logging.info('(monitor_publish_request_count) request count is zero but still in the warm up period: {s} seconds'.format(s=dt.seconds))
        elif dt.seconds > 60 * 10 and request_count == 0:
            logging.warn('(monitor_publish_request_count) request count is zero')
            try:
                logging.warn('(monitor_publish_request_count) restarting as publish req count is zero after warnup')
                append_if_p_killed.append("dummy")
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)  # Send the signal to all the process groups
            except Exception as ex:
                print(ex)
            break
        time.sleep(10)

def run():
    global request_count

    while True:
        logging.info('starting run_polygon.py')
        p = subprocess.Popen(['go', 'run', 'run_ohlc.go', '4'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
        t_pstart = datetime.datetime.now()
        append_if_p_killed = []

        th1 = threading.Thread(target=monitor_restart_trigger, args=(p, append_if_p_killed,))
        th2 = threading.Thread(target=monitor_publish_request_count, args=(p, t_pstart, append_if_p_killed))

        th1.start()
        th2.start()

        while True:
            th1.join(10)
            logging.info('end of the join thread monitor_restart_trigger')
            if append_if_p_killed:
                logging.warn('observed that the process was killed in monitor_restart_trigger thread.')
                # TODO: finish
                # th2.cancel()
                break
            th2.join(10)
            logging.info('end of the join thread monitor_publish_request_count')
            if append_if_p_killed:
                logging.warn('observed that the process was killed in monitor_publish_request_count thread.')
                # th1.cancel()
                break
            logging.info('end of the join loop, process was not killed in the loop')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #parser.add_argument("-f", "--forcerun", action="store_true", help="forces run without waiting without observing the schedule.")
    args = parser.parse_args()

    threading.Thread(target=update_publish_request_count).start()
    run()
