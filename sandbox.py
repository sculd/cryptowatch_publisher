import argparse
import os, time, threading, subprocess, signal, datetime
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), 'credential.json')

if __name__ == '__main__':
    p = subprocess.Popen(['go', 'run', 'run_ohlc.go', '4'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
    while True:
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
