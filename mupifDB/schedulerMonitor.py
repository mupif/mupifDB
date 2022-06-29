#!/usr/bin/env python

import sys
import Pyro5


from builtins import str
import getopt
import sys
import mupif as mp

import time as timeTime
import curses
import re

global ns

running_col = 0
processed_col = 35
finished_col = 41
failed_col = 70


def usage():
    print("Usage: SchedulerStatus [-r refreshRateInSeconds]")


def processor(win, scheduler, schedulerURI):
    global ns

    win.erase()

    win.addstr(0, 0, "MuPIF Scheduler Status MONITOR")
    win.addstr(1, 0, "Scheduler URI:      "+str(schedulerURI))

    win.addstr(3, 0, "WEid")
    win.addstr(3, 30, "Wid")
    win.addstr(3, 50, "Status")
    win.addstr(3, 60, "Start/Finish Date")
    win.hline(4, 0, '-', 80)

    win.addstr(23, 0, "Press [q] to quit")
    win.refresh()

    win1 = curses.newwin (10, 80, 6, 0)

    win.nodelay(1)
    while True:
        stat = scheduler.getStatistics()
        win.addstr(0, 70, timeTime.strftime("%H:%M:%S", timeTime.gmtime()))
        win.addstr(22, 0, "TotalStat: Running:"+str(stat['runningTasks']))
        win.addstr(22, 25, "Processed:"+str(stat['processedTasks']))
        win.addstr(22, 45, "Finished:"+str(stat['finishedTasks']))
        win.addstr(22, 65, "Failed:"+str(stat['failedTasks']))
        win.refresh()

        win1.erase()
        c = win.getch()
        if c == ord('q'):
            break
        i = 0
        for rec in stat['lastJobs']:
            win1.addstr(i, 0, '{:30.29}'.format(rec[0]))
            win1.addstr(i, 30, '{:20.19}'.format(rec[1]))
            win1.addstr(i, 50, rec[2])
            if (rec[2] == 'Finished' or rec[2]=='Failed'):
                win1.addstr(i, 60, rec[4]) #end time
            else:
                win1.addstr(i, 60, rec[3]) #start time
            i = i+1
        win1.refresh()
        timeTime.sleep(1)
    return


#######################################################################################


def main():
    global ns
    global scheduler
    ns = mp.pyroutil.connectNameserver()
    ns_uri = str(ns._pyroUri)
   
    try:
        opts, args = getopt.getopt(sys.argv[1:], "r:")
        # print(opts, args)
    except getopt.GetoptError as err: 
        # print help information and exit: 
        print(str(err))  # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    
    for o, a in opts:
        #        if o in ("-p"):
        #            port = int(a)
        #        elif o in ("-h"):
        #            host = a
        if o in ("-r",):
            refreshRate = a
        else:
            assert False, "unhandled option"
    
    # print("huhu:"+host+str(port))

    # locate scheduler, request remote proxy
    schedulerURI = ns.lookup('mupif.scheduler')
    # get local port of jobmanager (from uri)
    scheduler = Pyro5.api.Proxy(schedulerURI)

    curses.wrapper(processor, scheduler, schedulerURI)

    ###########################################################


if __name__ == '__main__':
    main()
